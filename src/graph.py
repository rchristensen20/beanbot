
import os
import logging
from typing import List, TypedDict, Annotated
from langgraph.graph.message import add_messages
from langchain_core.messages import AIMessage, BaseMessage, SystemMessage, HumanMessage
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

# Import our custom tool functions
from src.services.tools import (
    list_knowledge_files,
    read_knowledge_file,
    update_journal,
    amend_topic_knowledge,
    add_task,
    log_harvest,
    overwrite_knowledge_file,
    generate_calendar_from_library,
    get_current_date,
    complete_task,
    find_related_files,
    search_file_contents,
    read_multiple_files,
    backup_file,
    delete_knowledge_file,
    list_members,
    get_tasks_for_user,
    web_search,
    remove_tasks,
)

logger = logging.getLogger(__name__)

# --- Checkpointer Setup ---
DB_PATH = os.path.join("data", "conversations.db")


# --- Tool Wrappers for LangChain ---

@tool
def tool_list_files():
    """Lists all available knowledge files in the data library."""
    return list_knowledge_files()

@tool
def tool_read_file(filename: str):
    """Reads a specific knowledge file. Args: filename (e.g. 'garlic.md')"""
    return read_knowledge_file(filename)

@tool
def tool_update_journal(entry: str):
    """Logs a general event to the daily garden log. Args: entry (text)"""
    return update_journal(entry)

@tool
def tool_amend_knowledge(topic: str, content: str, source: str = ""):
    """Appends knowledge to a topic file (creates if new, appends if exists). Before amending an existing file, always read it first with tool_read_file to check for contradictions. Args: topic (e.g. 'garlic'), content (text to append), source (provenance: URL, PDF filename, 'Discord message', or 'image')"""
    logger.info(f"tool_amend_knowledge called: topic={topic!r}, content_len={len(content)}, source={source!r}")
    result = amend_topic_knowledge(topic, content, source)
    logger.info(f"tool_amend_knowledge result: {result}")
    return result

@tool
def tool_add_task(task_description: str, due_date: str = "", assigned_to: str = "", skip_duplicate_check: bool = False):
    """Adds a task to the tracker. Checks for similar existing tasks first.
    Args: task_description (text), due_date (YYYY-MM-DD, optional), assigned_to (name, optional),
    skip_duplicate_check (set True to force-add even if similar tasks exist)"""
    return add_task(task_description, due_date, assigned_to, skip_duplicate_check)

@tool
def tool_log_harvest(crop: str, amount: str, location: str, notes: str = ""):
    """Logs a harvest. Args: crop, amount, location, notes (optional)"""
    return log_harvest(crop, amount, location, notes)

@tool
def tool_overwrite_file(filename: str, content: str):
    """Replaces entire file content. Use for tasks.md checkbox updates, calendar rewrites, and farm_layout.md updates. Always read the file first to preserve existing data. Args: filename (e.g. 'tasks.md'), content (full new file content)"""
    return overwrite_knowledge_file(filename, content)

@tool
def tool_generate_calendar():
    """Scans the entire library to generate/update 'planting_calendar.md'. Use when asked to update the calendar."""
    return generate_calendar_from_library()

@tool
def tool_get_date():
    """Returns the current date and time."""
    return get_current_date()

@tool
def tool_find_related_files(topic: str):
    """Finds all files related to a topic/plant. Use this FIRST to discover all relevant data before reading. Args: topic (e.g. 'tomato', 'pepper')"""
    return find_related_files(topic)

@tool
def tool_complete_task(task_snippet: str):
    """Marks a task as complete (checks the box) in tasks.md and automatically logs to the journal. Do not also call tool_update_journal. Args: task_snippet (substring of task description to match)"""
    return complete_task(task_snippet)

@tool
def tool_search_file_contents(query: str):
    """Searches inside all knowledge files for mentions of a topic. Returns list of matching filenames. Args: query (e.g. 'garlic', 'companion planting')"""
    results = search_file_contents(query)
    if not results:
        return f"No files contain mentions of '{query}'."
    return "Files mentioning '{}': {}".format(query, ", ".join(results))

@tool
def tool_read_multiple_files(filenames: list[str]):
    """Reads several knowledge files at once. More efficient than calling tool_read_file repeatedly. Args: filenames (list of filenames)"""
    results = read_multiple_files(filenames)
    parts = []
    for filename, content in results.items():
        parts.append(f"=== {filename} ===\n{content}")
    return "\n\n".join(parts)

@tool
def tool_backup_file(filename: str):
    """Creates a backup of a file before modifying/deleting it. Copies to data/backups/ with timestamp. Args: filename (e.g. 'garlic.md')"""
    return backup_file(filename)

@tool
def tool_delete_file(filename: str):
    """Deletes a knowledge file after merging. Cannot delete system files (tasks.md, harvests.md, etc). Args: filename (e.g. 'garlic_care.md')"""
    return delete_knowledge_file(filename)

@tool
def tool_get_my_tasks(name: str):
    """Returns open tasks assigned to a specific person plus all unassigned tasks. Tasks assigned to other people are excluded. Args: name (person's name)"""
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
def tool_remove_tasks(snippet: str):
    """Permanently removes (deletes) all open tasks matching a substring. Unlike tool_complete_task which checks the box, this deletes the lines entirely. Use when the user wants tasks gone, not marked done. Args: snippet (case-insensitive text to match)"""
    return remove_tasks(snippet)

@tool
def tool_web_search(query: str, max_results: int = 5):
    """Search the web using DuckDuckGo for gardening information not in the knowledge base.
    Use this when the knowledge library doesn't have enough info to answer a question.
    Returns titles, URLs, and snippets. Args: query (search terms), max_results (1-10, default 5)"""
    return web_search(query, max_results)

TOOLS = [
    tool_list_files,
    tool_read_file,
    tool_update_journal,
    tool_amend_knowledge,
    tool_add_task,
    tool_log_harvest,
    tool_overwrite_file,
    tool_generate_calendar,
    tool_get_date,
    tool_find_related_files,
    tool_complete_task,
    tool_search_file_contents,
    tool_read_multiple_files,
    tool_backup_file,
    tool_delete_file,
    tool_get_my_tasks,
    tool_list_members,
    tool_web_search,
    tool_remove_tasks,
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
MAX_CONTEXT_TURNS = int(os.getenv("MAX_CONTEXT_TURNS", "10"))


def get_model():
    return ChatGoogleGenerativeAI(
        model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        temperature=LLM_TEMPERATURE,
    ).bind_tools(TOOLS)

STATIC_SYSTEM_PROMPT = (
    "You are Beanbot, a gardening assistant with access to a markdown knowledge library.\n"
    "You must read files with tools to see their content. You have no memory of file contents.\n"
    "\n"
    "## Execution Model\n"
    "Always call tools first, then respond with text. Every response follows this order:\n"
    "1. Call all necessary tools (reading files, writing data, searching, completing tasks).\n"
    "2. Check each tool's return value. If a tool returns an error, report the failure honestly.\n"
    "3. Respond with a summary of what was accomplished. Describe only completed actions.\n\n"
    "For multi-item requests (e.g. 10 plants), call tools for every item before sending any text.\n"
    "If a request is too large to finish, complete as much as possible and list exactly what remains "
    "for the user to request next.\n"
    "When you lack specific plant care info, use tool_web_search to find concrete details. "
    "Create actionable tasks with real numbers (e.g. 'Water weekly, 1 inch during growing season'), "
    "not placeholder 'check care' or 'look up info' tasks. Finding information is your job.\n"
    "\n"
    "## User Identity\n"
    "The user's name appears as '[User: Name]' at the start of their message.\n"
    "- 'My tasks' -> call tool_get_my_tasks with their name.\n"
    "- Task assignment -> use the assigned_to param on tool_add_task. Leave empty when unspecified.\n"
    "\n"
    "## Tool Routing\n"
    "\n"
    "### Answering Questions\n"
    "1. Identify which files to read from the file lookup table below.\n"
    "2. Use tool_find_related_files and tool_search_file_contents to discover additional relevant files.\n"
    "3. Read files with tool_read_file or tool_read_multiple_files.\n"
    "4. If the library lacks sufficient info, call tool_web_search with a specific query.\n"
    "5. If web results contain generally useful info (care guides, planting dates, pest info), "
    "also save it via tool_amend_knowledge with the result URL as the source arg.\n"
    "6. Only web search when local files lack the answer. Skip for topics already well-covered.\n"
    "\n"
    "### User Reports Activity\n"
    "1. Read tasks.md to check if the activity matches an open task.\n"
    "2. If it matches: call tool_complete_task (auto-logs to journal; do not also call tool_update_journal).\n"
    "3. If no match: call tool_update_journal.\n"
    "\n"
    "### Task Management\n"
    "- Add tasks: tool_add_task. If it returns similar existing tasks, ask the user whether to add anyway, replace, or skip. "
    "Use skip_duplicate_check=True only after user confirmation.\n"
    "- Mark done: tool_complete_task (checks the box and logs to journal).\n"
    "- Delete/remove tasks: tool_remove_tasks (permanently deletes lines). "
    "Use this when user says 'remove' or 'delete', not tool_complete_task or tool_overwrite_file.\n"
    "- Assign tasks: use the assigned_to parameter on tool_add_task.\n"
    "\n"
    "### File Lookup Table\n"
    "- Inventory/layout/zone/location -> farm_layout.md (read first if zone is unknown)\n"
    "- 'What is planted in [area]?' -> farm_layout.md, then tool_find_related_files for any "
    "kit/collection names to get individual plant lists. List actual plants, not just kit names.\n"
    "- Plant details -> tool_find_related_files with plant name, then read relevant files\n"
    "- Tasks/reminders -> tasks.md\n"
    "- Planting schedule -> planting_calendar.md\n"
    "- Harvest history -> harvests.md\n"
    "- Zone/frost dates -> almanac.md\n"
    "- Today's weather/briefing -> daily_YYYY-MM-DD.md (use today's date)\n"
    "- Recent activity -> garden_log.md\n"
    "- Categories/plant groups -> categories.md (summarize contents; do not list files from memory)\n"
    "- 'Where did I learn this?' / sources for a topic -> read the topic file, check the '## Sources' section\n"
    "- If unsure of filename -> call tool_list_files\n"
    "\n"
    "### Cross-Referencing\n"
    "When giving planting advice or creating planting tasks:\n"
    "- Read almanac.md for zone and frost dates.\n"
    "- Read farm_layout.md for bed availability.\n"
    "- Search for relevant technique files (companion planting, succession planting, soil amendment) "
    "via tool_find_related_files.\n"
    "- Include specific bed locations in planting tasks when possible.\n"
    "\n"
    "### Images\n"
    "Users may send photos. You can see them directly.\n"
    "- Garden layout photos: read farm_layout.md first, then update via tool_overwrite_file "
    "merging new spatial info with existing content.\n"
    "- Plant/pest photos: identify and respond conversationally.\n"
    "- Area photos with captions: update the relevant section of farm_layout.md.\n"
    "\n"
    "## Response Format\n"
    "\n"
    "### Communication Style\n"
    "Refer to stored information by topic name, not filename "
    "(say 'the planting calendar' not 'planting_calendar.md'). Users cannot see raw files.\n"
    "\n"
    "### Discord Markdown\n"
    "Responses render as Discord markdown. Ensure all formatting is well-formed:\n"
    "- Every ** has a matching closing **. Every ` has a matching `.\n"
    "- Blank line before bullet lists. Consistent indentation.\n"
    "- Use ## and ### headers to organize long responses.\n"
    "- Verify all formatting is closed before ending your message.\n"
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
    1. Trim accumulated history to last ~10 turns.
    2. Prepend static instructions.
    3. Invoke model.
    4. Fix tool call names if Gemini dropped the 'tool_' prefix.
    """
    model = get_model()
    messages = trim_messages_for_context(state['messages'])

    conversation = [SystemMessage(content=STATIC_SYSTEM_PROMPT)] + messages

    response = model.invoke(conversation)
    response = _fix_tool_call_names(response)
    return {"messages": [response]}

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


