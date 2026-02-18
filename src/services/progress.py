"""Progress tracking for LangGraph agent execution.

Provides a ProgressTracker that renders a live-updating Discord progress bar
as tools execute, then yields the final response for in-place message editing.
"""

import time
from dataclasses import dataclass, field

# Friendly display names for all tool functions
TOOL_DISPLAY_NAMES: dict[str, str] = {
    "tool_list_files": "Listing files",
    "tool_read_file": "Reading file",
    "tool_read_multiple_files": "Reading files",
    "tool_update_journal": "Updating journal",
    "tool_amend_knowledge": "Saving knowledge",
    "tool_add_task": "Adding task",
    "tool_log_harvest": "Logging harvest",
    "tool_overwrite_file": "Writing file",
    "tool_generate_calendar": "Generating calendar",
    "tool_get_date": "Checking date",
    "tool_find_related_files": "Finding related files",
    "tool_complete_task": "Completing task",
    "tool_search_file_contents": "Searching files",
    "tool_backup_file": "Backing up file",
    "tool_delete_file": "Deleting file",
    "tool_get_my_tasks": "Getting tasks",
    "tool_list_members": "Listing members",
    "tool_web_search": "Searching the web",
    "tool_remove_tasks": "Removing tasks",
}

BAR_WIDTH = 20
FILLED = "\u2588"  # █
EMPTY = "\u2591"   # ░


@dataclass
class ProgressTracker:
    """Tracks tool execution progress and renders a Discord-friendly progress bar."""

    tool_calls_completed: int = 0
    agent_iterations: int = 0
    current_tool_names: list[str] = field(default_factory=list)
    phase: str = "thinking"  # thinking | working | done | error
    start_time: float = field(default_factory=time.monotonic)
    final_response: str = ""
    error_message: str = ""

    @property
    def percent(self) -> int:
        """Asymptotic progress percentage, capped at 90% until done."""
        if self.phase == "done":
            return 100
        raw = 100 * (1 - 1 / (1 + 0.3 * self.tool_calls_completed + 0.1 * self.agent_iterations))
        return min(int(raw), 90)

    @property
    def elapsed(self) -> int:
        """Seconds since tracking started."""
        return int(time.monotonic() - self.start_time)

    def on_agent_output(self, tool_call_names: list[str]) -> None:
        """Called when the agent node produces tool calls."""
        self.agent_iterations += 1
        self.current_tool_names = tool_call_names
        self.phase = "working"

    def on_tools_complete(self, count: int) -> None:
        """Called when the tools node finishes executing tools."""
        self.tool_calls_completed += count

    def on_final_response(self, text: str) -> None:
        """Called when the agent produces a final text response (no tool calls)."""
        self.final_response = text
        self.phase = "done"

    def on_error(self, msg: str) -> None:
        """Called on timeout or unexpected error."""
        self.error_message = msg
        self.phase = "error"

    def render_bar(self) -> str:
        """Render a [████░░░░] style progress bar."""
        pct = self.percent
        filled = int(BAR_WIDTH * pct / 100)
        empty = BAR_WIDTH - filled
        return f"[{FILLED * filled}{EMPTY * empty}]"

    def render_status(self) -> str:
        """Render the full progress status line for Discord."""
        if self.phase == "error":
            return f"{self.render_bar()} Error: {self.error_message}"

        pct = self.percent
        bar = self.render_bar()

        # Friendly name for current tool(s)
        if self.current_tool_names:
            label = TOOL_DISPLAY_NAMES.get(
                self.current_tool_names[0],
                self.current_tool_names[0],
            )
            if len(self.current_tool_names) > 1:
                label += f" (+{len(self.current_tool_names) - 1} more)"
        elif self.phase == "thinking":
            label = "Thinking..."
        else:
            label = "Working..."

        # Build suffix with step count and elapsed time
        parts = []
        if self.tool_calls_completed > 0:
            steps = "step" if self.tool_calls_completed == 1 else "steps"
            parts.append(f"{self.tool_calls_completed} {steps} done")
        elapsed = self.elapsed
        if elapsed >= 2:
            parts.append(f"{elapsed}s")
        suffix = f" ({', '.join(parts)})" if parts else ""

        return f"{bar} {pct}% {label}{suffix}"
