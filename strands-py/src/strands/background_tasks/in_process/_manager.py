"""Agent-owned in-process background task manager."""

from __future__ import annotations

import asyncio
import builtins
import concurrent.futures
import copy
import inspect
import math
import threading
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal, Protocol

from strands._async import is_run_async_bridge
from strands._middleware import MiddlewareInterruptResult
from strands.background_tasks._errors import (
    BackgroundTaskNotFoundError,
    BackgroundTaskTimeoutError,
)
from strands.background_tasks._runtime import get_background_task_runtime
from strands.background_tasks._types import BackgroundTask, BackgroundTaskInterrupt, is_task_status_terminal
from strands.interrupt import Interrupt, InterruptException, _InterruptState
from strands.types.agent import LocalAgent
from strands.types.content import ContentBlock
from strands.types.interrupt import InterruptResponse, InterruptResponseContent
from strands.types.tools import AgentTool, ToolContext, ToolResult, ToolUse, _ToolMessageHandler

from ._engine import _MAX_PROGRESS_LENGTH, InProcessTaskEngine
from ._types import (
    CancelSignal,
    InProcessTaskExecutionContext,
    InProcessTaskExecutionOutcome,
    InProcessTaskRecord,
    InterruptStateData,
)


class _MiddlewareInterrupt(Protocol):
    def __call__(
        self,
        name: str,
        *,
        reason: Any = None,
        response: Any = None,
    ) -> MiddlewareInterruptResult:
        """Request middleware input."""
        ...


@dataclass(frozen=True)
class _LiveToolExecution:
    tool_use: ToolUse
    invocation_state: dict[str, Any]
    tool: AgentTool
    origin_loop: asyncio.AbstractEventLoop | None


@dataclass(frozen=True)
class _MessageRegistration:
    handler: _ToolMessageHandler
    loop: asyncio.AbstractEventLoop | None


class _BackgroundToolContext(ToolContext[LocalAgent]):
    def __init__(
        self,
        *,
        task_id: str,
        tool_use: ToolUse,
        agent: LocalAgent,
        invocation_state: dict[str, Any],
        cancel_signal: threading.Event,
        interrupt_state: _InterruptState,
        report_progress: Callable[[str], None],
        register_message: Callable[[_ToolMessageHandler], Callable[[], None]],
        report_activity: Callable[[str, Literal["tool_request", "tool_result"]], None],
    ) -> None:
        ToolContext.__init__(self, tool_use, agent, invocation_state)
        self._task_id = task_id
        self.cancel_signal = cancel_signal
        self._task_interrupt_state = interrupt_state
        self._progress_reporter = report_progress
        self._message_registrar = register_message
        self._activity_reporter = report_activity

    def _interrupt_id(self, name: str) -> str:
        return f"v1:tool_call:{self._task_id}:{uuid.uuid5(uuid.NAMESPACE_OID, name)}"

    def interrupt(self, name: str, reason: Any = None, response: Any = None) -> Any:
        """Request task-local tool input."""
        return _request_interrupt(
            self._task_interrupt_state,
            self._interrupt_id(name),
            name,
            reason,
            response,
        )


class InProcessTaskManager:
    """Execute approved tool calls on the process-lifetime background loop."""

    def __init__(
        self,
        agent: LocalAgent,
        execute_tool: Callable[[AgentTool, ToolContext[LocalAgent], _MiddlewareInterrupt], Awaitable[ToolResult]],
        *,
        max_concurrency: int = 4,
        timeout: float = math.inf,
        on_task_updated: Callable[[BackgroundTask], None] | None = None,
    ) -> None:
        """Initialize the task manager."""
        self._agent = agent
        self._execute_tool = execute_tool
        self._on_task_updated = on_task_updated
        self._runtime = get_background_task_runtime()
        self._has_tasks = threading.Event()
        self._engine = InProcessTaskEngine(
            max_concurrency=max_concurrency,
            timeout=timeout,
            execute=self._execute_tool_task,
            on_task_updated=self._handle_task_updated,
        )
        self._executions: dict[str, _LiveToolExecution] = {}
        self._task_id_by_submission: dict[tuple[str, str], str] = {}
        self._task_events: dict[str, asyncio.Event] = {}
        self._message_handlers: dict[str, _MessageRegistration] = {}
        self._message_handlers_lock = threading.Lock()

    async def submit(
        self,
        tool_use: ToolUse,
        invocation_state: dict[str, Any],
        pass_id: str,
        tool: AgentTool,
    ) -> BackgroundTask:
        """Submit one approved tool call."""
        origin_loop = None if is_run_async_bridge() else asyncio.get_running_loop()

        def submit() -> BackgroundTask:
            submission_key = (pass_id, tool_use["toolUseId"])
            existing_task_id = self._task_id_by_submission.get(submission_key)
            if existing_task_id is not None:
                existing = self._engine.get(existing_task_id)
                if existing is not None:
                    return _to_background_task(existing)

            invocation_state_id = str(uuid.uuid4())
            self._executions[invocation_state_id] = _LiveToolExecution(
                tool_use=copy.deepcopy(tool_use),
                invocation_state=invocation_state,
                tool=tool,
                origin_loop=origin_loop,
            )
            record = self._engine.submit(
                tool_use_id=tool_use["toolUseId"],
                tool_name=tool_use["name"],
                invocation_state_id=invocation_state_id,
            )
            self._task_id_by_submission[submission_key] = record["task_id"]
            self._has_tasks.set()
            return _to_background_task(record)

        return await self._runtime.run(submit)

    async def get(
        self, task_id: str, *, include_history: bool = False, after_sequence: int = 0
    ) -> BackgroundTask | None:
        """Get a task and optionally its retained activity from one runtime snapshot.

        Raises:
            ValueError: If the cursor is not a nonnegative integer, is out of range,
                or is supplied without requesting history.
        """
        if not include_history and after_sequence != 0:
            raise ValueError("after_sequence requires include_history=True")

        def get() -> BackgroundTask | None:
            record = self._engine.get(task_id)
            if record is None:
                return None
            task = _to_background_task(record)
            if include_history:
                task["activity"] = self._engine.get_activity(task_id, after_sequence=after_sequence)
            return task

        return await self._runtime.run(get)

    async def list(self) -> list[BackgroundTask]:
        """List tracked tasks."""
        return await self._runtime.run(lambda: [_to_background_task(record) for record in self._engine.list()])

    def has_tasks(self) -> bool:
        """Return whether any tasks remain tracked."""
        return self._has_tasks.is_set()

    async def cancel(self, task_id: str) -> BackgroundTask:
        """Cancel one task."""
        return await self._runtime.run(
            lambda: _to_background_task(self._engine.cancel(task_id, reason="Cancellation requested"))
        )

    async def message(self, task_id: str, message: str | builtins.list[ContentBlock]) -> None:
        """Deliver a message to a working task's registered handler.

        Raises:
            BackgroundTaskNotFoundError: If the task does not exist.
            RuntimeError: If the task is no longer working or has no message handler.
        """
        return await self._runtime.run(lambda: self._message(task_id, message))

    async def _message(self, task_id: str, message: str | builtins.list[ContentBlock]) -> None:
        record = self._engine.get(task_id)
        if record is None:
            raise BackgroundTaskNotFoundError(task_id)
        if record["status"] != "working":
            raise RuntimeError(f"Background task '{task_id}' cannot accept messages in status '{record['status']}'")
        with self._message_handlers_lock:
            registration = self._message_handlers.get(task_id)
        if registration is None:
            raise RuntimeError(f"Background task '{task_id}' does not accept messages")
        await self._invoke_message(task_id, registration, message)

    async def _invoke_message(
        self, task_id: str, registration: _MessageRegistration, message: str | builtins.list[ContentBlock]
    ) -> None:
        def call_handler() -> None | Awaitable[None]:
            with self._message_handlers_lock:
                if self._message_handlers.get(task_id) is not registration:
                    raise RuntimeError(f"Background task '{task_id}' message handler is no longer available")
            return registration.handler(copy.deepcopy(message))

        async def invoke() -> None:
            result = await asyncio.to_thread(call_handler) if registration.loop is None else call_handler()
            if inspect.isawaitable(result):
                await result

        if registration.loop is None or registration.loop is asyncio.get_running_loop():
            await invoke()
            return
        if registration.loop.is_closed():
            raise RuntimeError(f"Background task '{task_id}' message handler loop is closed")
        future = asyncio.run_coroutine_threadsafe(invoke(), registration.loop)
        await asyncio.wrap_future(future)

    async def wait(self, task_id: str) -> BackgroundTask:
        """Wait until one task requires input or becomes terminal."""
        return await self._runtime.run(lambda: self._wait(task_id))

    async def _wait(self, task_id: str) -> BackgroundTask:
        while True:
            record = self._engine.get(task_id)
            if record is None:
                raise BackgroundTaskNotFoundError(task_id)
            if record["status"] == "input_required" or is_task_status_terminal(record["status"]):
                return _to_background_task(record)
            changed = self._task_events.setdefault(task_id, asyncio.Event())
            await changed.wait()
            changed.clear()

    async def wait_for_idle(self, *, timeout: float | None = None) -> None:
        """Wait until no task is queued or physically executing."""
        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise TypeError(f"wait timeout must be a positive finite number, got {timeout!r}")
        if timeout is None:
            idle_wait: Awaitable[None] = self._runtime.run(self._engine.wait_for_idle)
            await idle_wait
            return
        cancel_signal = CancelSignal()

        def wait_for_idle() -> Awaitable[None]:
            return self._engine.wait_for_idle(cancel_signal=cancel_signal)

        idle_wait = self._runtime.run(wait_for_idle)
        operation = asyncio.ensure_future(idle_wait)
        try:
            await asyncio.wait_for(asyncio.shield(operation), timeout=float(timeout))
        except asyncio.TimeoutError as error:
            cancel_signal.abort(error)
            try:
                await operation
            except asyncio.TimeoutError:
                pass
            raise BackgroundTaskTimeoutError(float(timeout)) from error

    async def resume(self, task_id: str, responses: Sequence[InterruptResponse]) -> BackgroundTask:
        """Apply interrupt responses to one task."""

        def resume() -> BackgroundTask:
            def update(state_data: InterruptStateData) -> tuple[InterruptStateData, bool]:
                state = _InterruptState.from_dict(state_data)
                response_contents: list[InterruptResponseContent] = [
                    {
                        "interruptResponse": {
                            "interruptId": response["interruptId"],
                            "response": response["response"],
                        }
                    }
                    for response in responses
                ]
                state.resume(response_contents)
                return (
                    state.to_dict(),
                    all(interrupt.response is not None for interrupt in state.interrupts.values()),
                )

            return _to_background_task(self._engine.resume(task_id, update))

        return await self._runtime.run(resume)

    async def remove(self, task_ids: Sequence[str]) -> None:
        """Remove terminal tasks atomically."""

        def remove() -> None:
            unique_task_ids = list(dict.fromkeys(task_ids))
            for task_id in unique_task_ids:
                record = self._engine.get(task_id)
                if record is None:
                    raise BackgroundTaskNotFoundError(task_id)
                if not is_task_status_terminal(record["status"]):
                    raise RuntimeError(
                        f"Background task '{task_id}' cannot be removed before reaching a terminal status"
                    )
            for task_id in unique_task_ids:
                self._engine.remove(task_id)
                self._task_events.pop(task_id, None)
            removed = set(unique_task_ids)
            self._task_id_by_submission = {
                key: task_id for key, task_id in self._task_id_by_submission.items() if task_id not in removed
            }
            if not self._task_id_by_submission:
                self._has_tasks.clear()

        await self._runtime.run(remove)

    async def _execute_tool_task(self, context: InProcessTaskExecutionContext) -> InProcessTaskExecutionOutcome:
        execution = self._executions.get(context.invocation_state_id)
        if execution is None:
            raise RuntimeError("Background task live execution state is unavailable")
        interrupt_state = _InterruptState.from_dict(context.state) if context.state is not None else _InterruptState()
        runtime_loop = asyncio.get_running_loop()

        def report_activity(
            text: str, activity_type: Literal["progress", "tool_request", "tool_result"] = "progress"
        ) -> None:
            excerpt = text[-_MAX_PROGRESS_LENGTH:]
            report = partial(self._engine.report_progress, context.task_id, excerpt, activity_type=activity_type)
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            if current_loop is runtime_loop:
                report()
            else:
                runtime_loop.call_soon_threadsafe(report)

        def register_message(handler: _ToolMessageHandler) -> Callable[[], None]:
            if context.cancel_signal.is_set():
                return lambda: None
            try:
                handler_loop = asyncio.get_running_loop()
            except RuntimeError:
                handler_loop = None
            registration = _MessageRegistration(handler, handler_loop)
            with self._message_handlers_lock:
                self._message_handlers[context.task_id] = registration

            def unregister() -> None:
                with self._message_handlers_lock:
                    if self._message_handlers.get(context.task_id) is registration:
                        self._message_handlers.pop(context.task_id, None)

            return unregister

        tool_context = _BackgroundToolContext(
            task_id=context.task_id,
            tool_use=execution.tool_use,
            agent=self._agent,
            invocation_state=execution.invocation_state,
            cancel_signal=context.cancel_signal,
            interrupt_state=interrupt_state,
            report_progress=report_activity,
            register_message=register_message,
            report_activity=report_activity,
        )

        def middleware_interrupt(
            name: str,
            *,
            reason: Any = None,
            response: Any = None,
        ) -> MiddlewareInterruptResult:
            interrupt_id = f"v1:middleware_execute_tool:{context.task_id}:{uuid.uuid5(uuid.NAMESPACE_OID, name)}"
            return MiddlewareInterruptResult(
                response=_request_interrupt(interrupt_state, interrupt_id, name, reason, response)
            )

        async def execute_tool() -> ToolResult:
            return await self._execute_tool(execution.tool, tool_context, middleware_interrupt)

        try:
            if execution.origin_loop is None or execution.origin_loop.is_closed():
                result = await execute_tool()
            else:
                future = asyncio.run_coroutine_threadsafe(execute_tool(), execution.origin_loop)
                result = await _await_origin_result(future, context.cancel_signal)
        except InterruptException as error:
            if error.interrupt.id not in interrupt_state.interrupts:
                raise RuntimeError(f"Interrupt raised: {error.interrupt.name}") from error
            interrupt_state.activate()
            return {"status": "input_required", "state": interrupt_state.to_dict()}
        finally:
            with self._message_handlers_lock:
                self._message_handlers.pop(context.task_id, None)

        if result["status"] == "success":
            return {"status": "completed", "result": result}
        return {
            "status": "failed",
            "failure": {"type": "tool_error", "message": _tool_error_message(result)},
            "result": result,
        }

    def _handle_task_updated(self, record: InProcessTaskRecord) -> None:
        if record["status"] == "input_required" or is_task_status_terminal(record["status"]):
            changed = self._task_events.get(record["task_id"])
            if changed is not None:
                changed.set()
            with self._message_handlers_lock:
                self._message_handlers.pop(record["task_id"], None)
        if is_task_status_terminal(record["status"]):
            self._executions.pop(record["invocation_state_id"], None)
        if self._on_task_updated is not None:
            self._on_task_updated(_to_background_task(record))


async def _await_origin_result(
    future: concurrent.futures.Future[ToolResult],
    cancel_signal: CancelSignal,
) -> ToolResult:
    loop = asyncio.get_running_loop()
    wrapped = asyncio.wrap_future(future)
    aborted: asyncio.Future[None] = loop.create_future()

    def wake_aborted() -> None:
        if not aborted.done():
            aborted.set_result(None)

    def on_abort() -> None:
        # Abort may arrive from another thread, and possibly after the loop closed.
        try:
            loop.call_soon_threadsafe(wake_aborted)
        except RuntimeError:
            pass

    cancel_signal.add_abort_callback(on_abort)
    try:
        pending: set[asyncio.Future[Any]] = {wrapped, aborted}
        await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        if wrapped.done():
            return wrapped.result()
        raise asyncio.CancelledError
    except BaseException:
        future.cancel()
        raise
    finally:
        cancel_signal.remove_abort_callback(on_abort)
        if not aborted.done():
            aborted.cancel()


def _request_interrupt(
    state: _InterruptState,
    interrupt_id: str,
    name: str,
    reason: Any,
    response: Any,
) -> Any:
    interrupt = state.interrupts.setdefault(
        interrupt_id,
        Interrupt(id=interrupt_id, name=name, reason=reason, response=response),
    )
    if interrupt.response is not None:
        return interrupt.response
    raise InterruptException(interrupt)


def _tool_error_message(result: ToolResult) -> str:
    for content in result.get("content", []):
        text = content.get("text")
        if text:
            return text.removeprefix("Error: ")
    return "Tool returned an error without a message"


def _to_background_task(record: InProcessTaskRecord) -> BackgroundTask:
    task = BackgroundTask(
        task_id=record["task_id"],
        tool_use_id=record["tool_use_id"],
        tool_name=record["tool_name"],
        status=record["status"],
        created_at=record["created_at"],
        last_updated_at=record["last_updated_at"],
    )
    if "result" in record:
        task["result"] = {"content": copy.deepcopy(record["result"]["content"])}
    if "failure" in record:
        task["error"] = copy.deepcopy(record["failure"])
    if "state" in record:
        state = _InterruptState.from_dict(record["state"])
        interrupts = [
            _to_background_interrupt(interrupt) for interrupt in state.interrupts.values() if interrupt.response is None
        ]
        if interrupts:
            task["interrupts"] = interrupts
    if "progress" in record:
        task["progress"] = copy.deepcopy(record["progress"])
    return task


def _to_background_interrupt(interrupt: Interrupt) -> BackgroundTaskInterrupt:
    source: Literal["middleware", "tool"] = (
        "middleware" if interrupt.id.startswith("v1:middleware_execute_tool:") else "tool"
    )
    return {
        "id": interrupt.id,
        "name": interrupt.name,
        "reason": interrupt.reason,
        "source": source,
    }
