
import os
import logging
from typing import List, TypedDict, Annotated
from langgraph.graph.message import add_messages
from langchain_core.messages import AIMessage, BaseMessage, RemoveMessage, SystemMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

# Import our custom tool functions
from src.services.tools import (
    update_journal,
    amend_topic_knowledge,
    add_task,
    log_harvest,
    overwrite_knowledge_file,
    generate_calendar_from_library,
    complete_task,
    read_multiple_files,
    backup_file,
    delete_knowledge_file,
    list_members,
    get_tasks_for_user,
    web_search,
    remove_tasks,
    reassign_tasks,
    search_knowledge,
)

logger = logging.getLogger(__name__)

# --- Checkpointer Setup ---
DB_PATH = os.path.join("data", "conversations.db")


# --- Tool Wrappers for LangChain ---

@tool
def tool_search_knowledge(query: str = ""):
    """Discover files in the knowledge library. Use this FIRST before reading files.
    - Empty query: lists all files.
    - Non-empty: searches filenames AND file content, returns deduplicated results with match-type annotations.
    Args: query (topic to search for, or empty string to list all files)"""
    return search_knowledge(query)

@tool
def tool_read_files(filenames: list[str]):
    """Reads one or more knowledge files. Pass a list with one or many filenames.
    Args: filenames (list of filenames, e.g. ['garlic.md'] or ['garlic.md', 'tomatoes.md'])"""
    results = read_multiple_files(filenames)
    parts = []
    for filename, content in results.items():
        parts.append(f"=== {filename} ===\n{content}")
    return "\n\n".join(parts)

@tool
def tool_update_journal(entry: str):
    """Logs a garden activity to the daily journal. Use for activities that do NOT match an existing task.
    If the activity matches an open task, use tool_complete_task instead (it auto-logs).
    Args: entry (text description of the activity)"""
    return update_journal(entry)

@tool
def tool_amend_knowledge(topic: str, content: str, source: str = ""):
    """Appends knowledge to a topic file (creates if new, appends if exists).
    Before amending an existing file, read it first with tool_read_files to check for contradictions.
    Args: topic (e.g. 'garlic'), content (text to append), source (provenance: URL, PDF filename, 'Discord message', or 'image')"""
    logger.info(f"tool_amend_knowledge called: topic={topic!r}, content_len={len(content)}, source={source!r}")
    result = amend_topic_knowledge(topic, content, source)
    logger.info(f"tool_amend_knowledge result: {result}")
    return result

@tool
def tool_add_task(task_description: str, due_date: str = "", assigned_to: str = "", skip_duplicate_check: bool = False, recurring: str = ""):
    """Adds a task to the tracker. Checks for similar existing tasks first — if duplicates found, ask the user before forcing with skip_duplicate_check=True.
    Args: task_description (text), due_date (YYYY-MM-DD, optional), assigned_to (name, optional),
    skip_duplicate_check (set True to force-add even if similar tasks exist),
    recurring (recurrence pattern, optional: daily, weekly, monthly, every N days, every N weeks — requires due_date)"""
    return add_task(task_description, due_date, assigned_to, skip_duplicate_check, recurring)

@tool
def tool_log_harvest(crop: str, amount: str, location: str, notes: str = ""):
    """Logs a harvest event. Args: crop, amount, location, notes (optional)"""
    return log_harvest(crop, amount, location, notes)

@tool
def tool_overwrite_file(filename: str, content: str):
    """Replaces entire file content. Use for tasks.md checkbox updates, calendar rewrites, and farm_layout.md updates.
    Always read the file first to preserve existing data. Never use this to remove tasks — use tool_remove_tasks instead.
    Args: filename (e.g. 'tasks.md'), content (full new file content)"""
    return overwrite_knowledge_file(filename, content)

@tool
def tool_generate_calendar():
    """Scans the entire knowledge library to generate/update 'planting_calendar.md'."""
    return generate_calendar_from_library()

@tool
def tool_complete_task(task_snippet: str):
    """Marks a task as complete (checks the box) AND automatically logs to the journal.
    Do NOT also call tool_update_journal — completion already logs it.
    Args: task_snippet (substring of task description to match)"""
    return complete_task(task_snippet)

@tool
def tool_delete_file(filename: str):
    """Deletes a knowledge file. Automatically backs up to data/backups/ first.
    Cannot delete system files (tasks.md, harvests.md, etc).
    Args: filename (e.g. 'garlic_care.md')"""
    backup_result = backup_file(filename)
    if backup_result.startswith("Error"):
        return backup_result
    delete_result = delete_knowledge_file(filename)
    return f"{backup_result}\n{delete_result}"

@tool
def tool_get_my_tasks(name: str):
    """Returns open tasks assigned to a specific person plus all unassigned tasks. Tasks assigned to other people are excluded.
    Args: name (person's name)"""
    tasks = get_tasks_for_user(name)
    if not tasks:
        return f"No open tasks for {name}."
    return f"Tasks for {name}:\n" + "\n".join(tasks)

@tool
def tool_list_members():
    """Lists all registered household/garden members and their Discord IDs."""
    members = list_members()
    if not members:
        return "No members registered. Use !register <name> in Discord to add members."
    lines = [f"- {name.title()} (ID: {did})" for name, did in members.items()]
    return "Registered members:\n" + "\n".join(lines)

@tool
def tool_web_search(query: str, max_results: int = 5):
    """Search the web for gardening information. Search the knowledge library first; only web-search
    when local files lack the answer. Save useful results via tool_amend_knowledge with the URL as source.
    Args: query (search terms), max_results (1-10, default 5)"""
    return web_search(query, max_results)

@tool
def tool_remove_tasks(snippet: str):
    """Permanently removes (deletes) all open tasks matching a substring.
    Unlike tool_complete_task which checks the box, this deletes the lines entirely.
    Use when the user wants tasks gone, not marked done.
    Args: snippet (case-insensitive text to match)"""
    return remove_tasks(snippet)

@tool
def tool_reassign_tasks(from_name: str, to_name: str):
    """Reassigns all open tasks from one person to another in a single bulk operation.
    Use instead of reading and rewriting tasks.md for each task individually.
    Set from_name to 'unassigned' to assign all unassigned tasks.
    Args: from_name (current assignee or 'unassigned'), to_name (new assignee)"""
    return reassign_tasks(from_name, to_name)

TOOLS = [
    tool_search_knowledge,
    tool_read_files,
    tool_update_journal,
    tool_amend_knowledge,
    tool_add_task,
    tool_log_harvest,
    tool_overwrite_file,
    tool_generate_calendar,
    tool_complete_task,
    tool_delete_file,
    tool_get_my_tasks,
    tool_list_members,
    tool_web_search,
    tool_remove_tasks,
    tool_reassign_tasks,
]

# Build a lookup to fix tool names when Gemini drops the "tool_" prefix.
_TOOL_NAME_SET = {t.name for t in TOOLS}
_TOOL_NAME_FIX = {
    t.name.removeprefix("tool_"): t.name
    for t in TOOLS
    if t.name.startswith("tool_")
}



# --- State Definition ---

class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]

# --- Node Logic ---

LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0"))
MAX_CONTEXT_TURNS = int(os.getenv("MAX_CONTEXT_TURNS", "4"))
SUMMARIZE_TOOL_LIMIT = int(os.getenv("SUMMARIZE_TOOL_LIMIT", "200"))
SUMMARIZE_HUMAN_LIMIT = int(os.getenv("SUMMARIZE_HUMAN_LIMIT", "300"))
SUMMARIZE_THRESHOLD = int(os.getenv("SUMMARIZE_THRESHOLD", "500"))


def get_model():
    return ChatGoogleGenerativeAI(
        model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        temperature=LLM_TEMPERATURE,
    ).bind_tools(TOOLS)

_SYSTEM_PROMPT_TEMPLATE = (
    "You are Beanbot, a gardening assistant with a markdown knowledge library.\n"
    "Current date/time: {current_date}\n"
    "You have no memory of file contents — always use tools to read them.\n"
    "\n"
    "## Rules\n"
    "- Call all necessary tools before responding with text.\n"
    "- Multi-item requests: process ALL items before writing any response text.\n"
    "- When you lack plant care info, use tool_web_search to find specifics. "
    "Create actionable tasks with real numbers, not 'check care' placeholders.\n"
    "\n"
    "## User Identity\n"
    "User's name appears as '[User: Name]' at the start of messages.\n"
    "Use their name for tool_get_my_tasks and task assignment.\n"
    "\n"
    "## Task Tools\n"
    "- Add: tool_add_task — if duplicates found, ask user before forcing.\n"
    "- Complete: tool_complete_task — checks box + auto-logs to journal. Do NOT also call tool_update_journal.\n"
    "- Remove/delete: tool_remove_tasks — permanently deletes lines. Never use tool_overwrite_file for this.\n"
    "- Reassign bulk: tool_reassign_tasks — moves tasks between people in one call.\n"
    "- Recurring: use recurring param on tool_add_task (requires due_date). "
    "Completed recurring tasks auto-reschedule — do not manually re-create.\n"
    "\n"
    "## File Reference\n"
    "- Layout/zones/beds: farm_layout.md\n"
    "- Tasks: tasks.md\n"
    "- Planting dates: planting_calendar.md\n"
    "- Harvests: harvests.md\n"
    "- Zone/frost: almanac.md\n"
    "- Today's weather: daily_YYYY-MM-DD.md\n"
    "- Activity log: garden_log.md\n"
    "- Categories: categories.md\n"
    "- Plant info: use tool_search_knowledge with plant name\n"
    "- Sources: check ## Sources section in topic files\n"
    "\n"
    "## Images\n"
    "You can see photos directly. For garden layout photos, update farm_layout.md.\n"
    "\n"
    "## Response Format\n"
    "Refer to info by topic name, not filename. Format as Discord markdown.\n"
    "Ensure all ** and ` formatting is properly closed.\n"
)


def _build_system_prompt() -> str:
    """Build the system prompt with the current date injected."""
    from datetime import datetime
    return _SYSTEM_PROMPT_TEMPLATE.format(
        current_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z"),
    )


def trim_messages_for_context(messages: List[BaseMessage], max_turns: int = MAX_CONTEXT_TURNS) -> List[BaseMessage]:
    """Keep only the last ~max_turns conversation turns to prevent context window blowup.

    Preserves any leading SystemMessages, then keeps the last max_turns HumanMessage
    boundaries (each turn = HumanMessage + all subsequent AI/Tool messages until the
    next HumanMessage).
    """
    # Find indices of all HumanMessages
    human_indices = [i for i, m in enumerate(messages) if isinstance(m, HumanMessage)]

    if len(human_indices) <= max_turns:
        return messages

    # Find the start of the window we want to keep
    cutoff_idx = human_indices[-max_turns]

    # Preserve any leading SystemMessages
    leading_system = []
    for m in messages:
        if isinstance(m, SystemMessage):
            leading_system.append(m)
        else:
            break

    return leading_system + messages[cutoff_idx:]


def _truncate_str(text: str, limit: int) -> str:
    """Truncate a string past the threshold, appending a length note."""
    if len(text) <= SUMMARIZE_THRESHOLD:
        return text
    return text[:limit] + f"... [truncated from {len(text)} chars]"


def _truncate_old_message(msg: BaseMessage) -> BaseMessage:
    """Truncate a single message from a previous turn. AIMessages are never modified."""
    if isinstance(msg, ToolMessage):
        content = msg.content
        if not isinstance(content, str) or len(content) <= SUMMARIZE_THRESHOLD:
            return msg
        return ToolMessage(
            content=_truncate_str(content, SUMMARIZE_TOOL_LIMIT),
            tool_call_id=msg.tool_call_id,
            name=getattr(msg, "name", None),
            id=msg.id,
            status=getattr(msg, "status", None),
        )
    if isinstance(msg, HumanMessage):
        content = msg.content
        if isinstance(content, list):
            image_count = 0
            new_parts = []
            for part in content:
                if isinstance(part, dict) and part.get("type") == "image_url":
                    image_count += 1
                elif isinstance(part, dict) and part.get("type") == "text":
                    new_parts.append({"type": "text", "text": _truncate_str(part.get("text", ""), SUMMARIZE_HUMAN_LIMIT)})
                else:
                    new_parts.append(part)
            if image_count:
                new_parts.append({"type": "text", "text": f"[{image_count} image(s) from earlier turn removed]"})
            return msg if new_parts == content else HumanMessage(content=new_parts, id=msg.id)
        if isinstance(content, str) and len(content) > SUMMARIZE_THRESHOLD:
            return HumanMessage(content=_truncate_str(content, SUMMARIZE_HUMAN_LIMIT), id=msg.id)
    return msg


def _summarize_old_turns(messages: list[BaseMessage]) -> list[BaseMessage]:
    """Truncate bulky ToolMessages and HumanMessages in previous turns.
    The current turn (last HumanMessage onward) is kept intact."""
    last_human_idx = next(
        (i for i in range(len(messages) - 1, -1, -1) if isinstance(messages[i], HumanMessage)),
        None,
    )
    if last_human_idx is None or last_human_idx == 0:
        return messages
    return [_truncate_old_message(m) if i < last_human_idx else m for i, m in enumerate(messages)]


def _fix_tool_call_names(response: AIMessage) -> AIMessage:
    """Fix tool call names when Gemini drops the 'tool_' prefix."""
    if not getattr(response, "tool_calls", None):
        return response
    fixed = False
    new_tool_calls = []
    for tc in response.tool_calls:
        name = tc["name"]
        if name not in _TOOL_NAME_SET and name in _TOOL_NAME_FIX:
            logger.warning("Fixing tool call name: %s -> %s", name, _TOOL_NAME_FIX[name])
            new_tool_calls.append({**tc, "name": _TOOL_NAME_FIX[name]})
            fixed = True
        else:
            new_tool_calls.append(tc)
    if not fixed:
        return response
    # Rebuild the AIMessage with corrected tool_calls and tool_call chunks
    new_chunks = []
    for chunk in response.additional_kwargs.get("tool_calls", []):
        fn_name = chunk.get("function", {}).get("name", "")
        if fn_name in _TOOL_NAME_FIX:
            chunk = {**chunk, "function": {**chunk["function"], "name": _TOOL_NAME_FIX[fn_name]}}
        new_chunks.append(chunk)
    new_kwargs = {**response.additional_kwargs}
    if new_chunks:
        new_kwargs["tool_calls"] = new_chunks
    return AIMessage(
        content=response.content,
        tool_calls=new_tool_calls,
        additional_kwargs=new_kwargs,
        response_metadata=response.response_metadata,
        id=response.id,
    )


def agent_node(state: AgentState):
    """
    Standard agent node:
    1. Trim accumulated history to last ~4 turns.
    2. Evict old messages from checkpointed state via RemoveMessage.
    3. Prepend static instructions.
    4. Invoke model.
    5. Fix tool call names if Gemini dropped the 'tool_' prefix.
    """
    model = get_model()
    all_messages = state['messages']
    messages = trim_messages_for_context(all_messages)

    # Evict trimmed messages from the checkpointed state so the DB stops growing.
    # Messages not in the trimmed window get RemoveMessage'd out of the state.
    kept_ids = {id(m) for m in messages}
    removals = [
        RemoveMessage(id=m.id)
        for m in all_messages
        if id(m) not in kept_ids and getattr(m, "id", None)
    ]
    if removals:
        logger.info(f"Evicting {len(removals)} old messages from checkpoint state")

    messages = _summarize_old_turns(messages)

    conversation = [SystemMessage(content=_build_system_prompt())] + messages

    response = model.invoke(conversation)
    response = _fix_tool_call_names(response)
    return {"messages": removals + [response]}

# --- Graph Construction ---

workflow = StateGraph(AgentState)

# Define Nodes
workflow.add_node("agent", agent_node)
workflow.add_node("tools", ToolNode(TOOLS))

# Define Edges
workflow.set_entry_point("agent")

def should_continue(state: AgentState):
    last_message = state['messages'][-1]
    # If the LLM made a tool call, go to 'tools'
    # Check if the attribute exists (it's on AIMessage)
    tool_calls = getattr(last_message, "tool_calls", None)
    if tool_calls:
        return "tools"
    # Otherwise, we are done
    return END

workflow.add_conditional_edges(
    "agent",
    should_continue,
    {
        "tools": "tools",
        END: END
    }
)

# After tools run, go back to agent to interpret the output
workflow.add_edge("tools", "agent")

# Compile without checkpointer initially; async init adds it later
app_graph = None

async def init_graph():
    """Async initializer — must be called once at bot startup."""
    global app_graph
    import aiosqlite
    conn = await aiosqlite.connect(DB_PATH)
    memory = AsyncSqliteSaver(conn=conn)
    await memory.setup()
    app_graph = workflow.compile(checkpointer=memory)


