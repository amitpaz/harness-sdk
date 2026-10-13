# Interactive background agents

This local Python prototype is based on `python/v1.59.0` (`925067cb1`). It addresses
[issue #4702 option 1](https://github.com/strands-agents/harness-sdk/issues/4702):
wake the completion wait when the caller supplies input, let the parent respond
while background work continues, then resume waiting. No new dependencies.

## API

```python
agent.send(message: str | list[ContentBlock]) -> None

background_tasks={
    "agentic": ["research"],
    "wake_on_input": True,        # default False
    "wait_for_completion": True,  # existing default
}

tool_context.report_progress(text: str) -> None
unregister = tool_context.on_message(handler)
# handler(message: str | list[ContentBlock], **kwargs) -> None | Awaitable[None]
unregister()  # idempotent; removes only this registration
```

`strands_manage_background_task` accepts `list`, `get`, `cancel`, and the new
`message` mode with `task_id` and a string `message`. A successful message result
contains `{task_id, message_sent: True}`. It acknowledges handler execution, not
that a child model has processed the input. A task must be working and have a
handler; unknown, queued, interrupted, terminal, and unsupported tasks produce
clear errors. Input-required tasks still use the existing interrupt-response API.

The methods on a foreground `ToolContext` are inert, so one tool implementation
can run in either mode. A later message-handler registration replaces the earlier
one. Async handlers execute on their registering event loop; handlers registered
from a synchronous worker run in a worker thread. Handlers should finish promptly. Cancellation of a message call cancels a pending
async handler; it cannot stop a synchronous function that has already begun.

`get` includes optional `progress = {message, update_count, updated_at}`. Only the
last 512 characters of the latest update are retained, with an update counter and
UTC timestamp. Reporting does not cause a parent model call. Agent-as-tool adapters
register child `send` automatically and report tool activity/status and a short
rolling text excerpt from the child stream. Custom tools use the same methods.

## Activity history

Background tasks automatically retain the most recent 100 activity entries in memory, with messages capped at 512 characters. Ordinary `get`, `list`, and automatic result delivery remain compact. Request history explicitly:

```json
{"mode": "get", "task_id": "...", "include_history": true, "after_sequence": 17}
```

The task metadata then includes an `activity` object:

```json
{
  "events": [
    {"sequence": 18, "timestamp": "2026-10-13T10:02:15Z", "type": "progress", "message": "Reviewed 12 documents"}
  ],
  "first_available_sequence": 1,
  "last_sequence": 18,
  "truncated": false
}
```

Events are ordered by a per-task sequence number assigned on the engine's owning event loop. `after_sequence` defaults to zero and returns retained events strictly newer than that cursor. Use `last_sequence` as the next cursor for the same task. `truncated` is true if entries after the requested cursor have already been dropped. `first_available_sequence` identifies the oldest retained entry even when the filtered result is empty. Negative, non-integer, and future cursors are rejected. History options are valid only for `get`, and a nonzero cursor requires `include_history=True`.

The recorded types are:

- `lifecycle`: actual background-task status transitions, including queued, working, input_required, completed, failed, and cancelled.
- `progress`: explicit `report_progress` updates or the adapter's throttled text excerpts.
- `tool_request`: a child model's streamed tool request, not proof that execution started or was approved.
- `tool_result`: the response associated with a child tool call. Its success/error status describes that response; for nested background work it can be a dispatch acknowledgement, not completion of the nested task.

The child adapter records each request once by tool-call ID and matches results by that ID, including when tools run concurrently. Messages sent to sub-agents and raw tool arguments/results are not copied into the timeline; entries contain short progress or activity descriptions. Text excerpts remain subject to the existing throttling, so this is a recent-activity view rather than a complete transcript.

History is stored separately from normal task snapshots and is never included in persisted background task state. It is removed when the task is removed after automatic result delivery. Restored tasks have no activity history, and an explicit history request for them returns an availability error. A history response already added to conversation history is an ordinary tool result and may persist with that conversation. No new model calls, subscriptions, durable archive, or UI are introduced.

## Delivery and lifecycle

`send` accepts user content, rejecting empty block lists and tool-use/tool-result
protocol blocks before enqueue. It copies content blocks into a FIFO protected by a threading lock. It is safe
from any thread or coroutine and never starts an invocation. Simultaneous calls
are ordered by enqueue lock acquisition. It does not change the invocation lock:
a concurrent invocation still raises `ConcurrencyException`.

An SDK-last `BeforeModelCallEvent` hook contributes inbox input to the existing
continuation machinery after background-result hooks, before the model request
captures conversation history. Queued input is removed only after append succeeds. At each
boundary, already-ready background result pairs come first, then queued user
messages in FIFO order. Input sent during a tool call waits for the tool-result
boundary. Input sent after a boundary waits for the next one. Each queued input
stays a separate user message, even beside another user message, so each emits
`MessageAddedEvent` with the usual tracking ID, conversation-manager visibility,
and session persistence behavior.

An idle agent retains input for its next invocation. Its invocation prompt is
appended before pending inbox messages. A send during the final model response
can remain pending if there is no later model boundary. Hosts should keep the
active invocation alive while waiting for background work; `send` is not a queue
of future invocations or a promise of a response.

With `wake_on_input=True` and `wait_for_completion=True`, the background wait
also awaits an inbox notification. It requests a continuation using the existing
resume machinery, delivers any ready results and inbox messages, runs the normal
model/tool cycle, and returns to waiting while tasks remain. Multiple sends may
be handled in one cycle. With the option off, input still enters the next natural
model boundary but does not wake the background wait. Detached mode is unchanged.

Cancellation takes precedence over waking input. Pending inbox messages remain
queued if cancellation or a before-model interrupt prevents delivery. Background
`input_required` still surfaces the interrupt and requires valid responses;
sending text does not authorize or resolve it. Waiter cancellation removes its
notification registration. Tools continue to use their existing per-task cancel
signals.

## Persistence and concurrency

Undelivered inbox messages, message handlers, live progress, and activity history are in-memory
only. A restored session does not reconstruct them, and loading a snapshot clears
the current inbox. Delivered messages persist like other conversation messages.
Progress and activity history are excluded from persisted background task state; a progress excerpt
already delivered in a tool result is ordinary conversation history and may
therefore persist. As before, unfinished background work cannot resume after
restore and is recorded as failed.

The background engine owns task records on its process-lifetime loop. Progress
and message routing cross the thread/loop boundary through the existing runtime.
The inbox registers per-loop futures under the same lock used by enqueue to avoid
lost wakeups. It wakes them with `call_soon_threadsafe`; no caller loop owns the
inbox. Task handlers are unregistered when execution ends, and the adapter also
unregisters when its child stream closes. Fresh-context child calls clear pending
input; preserved-context children retain their inbox for their next invocation.

The inbox is intentionally unbounded, like normal conversation input. Hosts that
need admission limits should enforce them before calling `send`. Progress retains
one bounded excerpt rather than an accumulating log.

## TypeScript alignment and upstream questions

Existing names retain Python casing (`wait_for_completion` / `waitForCompletion`);
`message`, `get`, and `cancel` use shared single-word names. This prototype adds no
Python-only hook name: it reuses `MessageAddedEvent`.

[Closed draft #3834](https://github.com/strands-agents/harness-sdk/pull/3834)
was held for a conflicting durable-queue proposal
([maintainer comment](https://github.com/strands-agents/harness-sdk/pull/3834#issuecomment-5586200182))
and was superseded by [open proposal #4749](https://github.com/strands-agents/harness-sdk/pull/4749).
The latter proposes `queue` and `inject` concurrent invocation modes. Its `inject`
joins a running invocation and resolves with that invocation's result. Python
`send` here is a synchronous, fire-and-forget inbox operation, not a second
invocation. Neither proposal is treated as an already-released TS contract.

Questions for maintainers before offering an upstream PR:

- Should this inbox surface align with the evolving `inject` proposal, or remain
  a separate low-level `send` primitive?
- Should these new APIs enter through `strands.experimental` first under the
  feature lifecycle policy?
- Are the progress shape, fixed excerpt limit, and text-only model-facing message
  schema suitable for cross-SDK parity?

## Consumer usage

```python
from strands import Agent, ToolContext, tool

child = Agent(name="researcher")

@tool(context=True)
async def research(prompt: str, tool_context: ToolContext) -> str:
    """Research a topic while accepting additional guidance."""
    unregister = tool_context.on_message(lambda message, **kwargs: child.send(message))
    try:
        tool_context.report_progress("Research started")
        result = await child.invoke_async(prompt, cancel_signal=tool_context.cancel_signal)
        tool_context.report_progress("Research complete")
        return str(result)
    finally:
        unregister()

parent = Agent(
    tools=[research],
    background_tasks={"agentic": [research.tool_name], "wake_on_input": True},
)

async def run_turn():
    return await parent.invoke_async("Research the options")

def on_user_message(text: str):
    # Called by the host input handler while run_turn is active.
    parent.send(text)
```

The host keeps the same parent agent and active turn alive. The model may use
manage-tool `get` to inspect progress, `message` to guide the child, or `cancel`
to stop a task. Do not invoke the same child concurrently; construct separate
children if a custom tool supports parallel task calls. `child.as_tool()` provides
the adapter behavior automatically.

## Validation

All commands run from `strands-py/`. Tests use mocked/scripted models; no live
inference, cloud infrastructure, GitHub writes, or publishing was performed.

Baseline on the pinned tag:

- `hatch fmt --check`: Ruff and formatting passed (745 files); mypy passed (339 source files).
- `hatch test`: 7,470 passed, 31 skipped, 2 failed under automatic worker selection.
  Both failures were existing short subprocess-timeout tests and passed together
  with `hatch test -n 0` before implementation.

Final checks, including bounded activity history:

- `hatch fmt --check`: Ruff, mypy (341 source files), and formatting (752 files) pass.
- `hatch test --all -n 4`: full supported-version matrix; counts below.
- `hatch test -n 4 tests/strands/background_tasks tests/strands/agent/test_agent_as_tool.py tests/strands/types/test_tools.py`: 141 focused tests; the full matrix also exercises them on every supported version.
- Existing agent-loop and continuation tests pass without modifying them.
- `hatch build`: source and wheel packages build successfully.
- `hatch run complexity`: informational `complexity/high`, maximum 61 in the
  existing agent-as-tool stream function; new progress helper methods are at most 6.
- Local pre-commit formatting, lint/type checks, unit tests, and conventional
  commit checks pass. Local hook launchers only adapt the monorepo working directory.

The coverage pre-push run also reproduced the baseline shell-timeout failure while other suites ran concurrently; the unchanged checks were rerun with two workers after the matrix finished.

Provider-backed integration tests were not run: the repository reserves them for
CI with credentials. Existing dependency deprecation/runtime warnings and the
suite's platform/optional-dependency skips remain.

The source patch applies cleanly to the cloned `origin/main` (`58158f7e9`) using
`git apply --cached --check` against a temporary index; this is an application
check, not a claim that the feature has been tested on main.

| Python | Passed | Skipped |
| --- | ---: | ---: |
| 3.14 | 7,520 | 31 |
| 3.13 | 7,520 | 31 |
| 3.12 | 7,520 | 31 |
| 3.11 | 7,398 | 35 |
| 3.10 | 7,393 | 40 |
| Total | 37,351 | 168 |

The table records the final frozen-source run, with no failures. The branch has
not been rebased: it remains based on `python/v1.59.0`.
