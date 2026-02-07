
import os
import logging
from typing import List, TypedDict, Annotated
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage
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
def tool_amend_knowledge(topic: str, content: str):
    """Updates a specific topic file with new notes/facts. Args: topic (e.g. 'garlic'), content (text)"""
    return amend_topic_knowledge(topic, content)

@tool
def tool_add_task(task_description: str, due_date: str = "", assigned_to: str = ""):
    """Adds a task to the tracker. Args: task_description (text), due_date (YYYY-MM-DD, optional), assigned_to (name, optional)"""
    return add_task(task_description, due_date, assigned_to)

@tool
def tool_log_harvest(crop: str, amount: str, location: str, notes: str = ""):
    """Logs a harvest. Args: crop, amount, location, notes (optional)"""
    return log_harvest(crop, amount, location, notes)

@tool
def tool_overwrite_file(filename: str, content: str):
    """Overwrites an entire file. USE WITH CAUTION. Args: filename, content"""
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
    """Marks a task as complete in 'tasks.md'. Args: task_snippet (text to match)"""
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
]



# --- State Definition ---

class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]

# --- Node Logic ---

def get_model():
    return ChatGoogleGenerativeAI(
        model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        temperature=0
    ).bind_tools(TOOLS)

STATIC_SYSTEM_PROMPT = (
    "You are Beanbot, a gardening assistant.\n"
    "You have access to a knowledge library, but you must read the files to see their content.\n"
    "TOOLS:\n"
    "- 'tool_read_file': Read content.\n"
    "- 'tool_add_task': Schedule a reminder.\n"
    "- 'tool_log_harvest': Record yields.\n"
    "- 'tool_complete_task': Mark a task as done. Args: substring of task description.\n"
    "- 'tool_update_journal': Log general activities that are NOT tracked tasks.\n"
    "- 'tool_overwrite_file': Edit full files.\n"
    "- 'tool_generate_calendar': Scans all files to rebuild 'planting_calendar.md'. Use if user asks to generate/update the calendar.\n"
    "- 'tool_get_date': Check current date/time. Use this if the date is not provided or ambiguous.\n"
    "- 'tool_find_related_files': Finds ALL files related to a topic/plant. Returns a list of matching filenames.\n"
    "- 'tool_search_file_contents': Search inside all files for mentions of a topic. Supplements tool_find_related_files which only matches filenames.\n"
    "- 'tool_read_multiple_files': Read several files at once. More efficient than calling tool_read_file repeatedly.\n"
    "- 'tool_backup_file': Create a backup before modifying/deleting during consolidation.\n"
    "- 'tool_delete_file': Delete a knowledge file after merging. Cannot delete system files.\n"
    "- 'tool_get_my_tasks': Returns open tasks assigned to a specific person plus all unassigned tasks. Args: name.\n"
    "- 'tool_list_members': Lists all registered household/garden members.\n"
    "- 'tool_add_task' supports an optional 'assigned_to' param. Use it when the user wants to assign a task to someone (e.g. 'remind George to weed').\n"
    "IDENTITY: The user's name is injected as '[User: Name]' at the start of their message. "
    "When they say 'my tasks', call 'tool_get_my_tasks' with their name. "
    "When they assign tasks to someone, use the 'assigned_to' param. When no assignee is specified, leave it empty.\n"
    "INSTRUCTION: To answer questions, you MUST first call 'tool_read_file' with the appropriate filename.\n"
    "INSTRUCTION: If the user says they did something, FIRST read 'tasks.md' to see if it was a tracked task.\n"
    "   - IF it matches a task: Call 'tool_complete_task'. This tool AUTOMATICALLY logs to the journal, so do NOT call 'tool_update_journal' as well.\n"
    "   - IF it is NOT a tracked task: Call 'tool_update_journal'.\n"
    "MAPPING:\n"
    "- For inventory/layout/location/zone -> read 'farm_layout.md'. IF ZONE IS UNKNOWN, READ THIS FIRST.\n"
    "- For plant details -> FIRST call 'tool_find_related_files' with the plant name to discover ALL related files (e.g. care guides, companion plants, pest info, seed starting, etc.), then read the relevant ones.\n"
    "- For tasks/reminders -> read 'tasks.md'\n"
    "- For planting schedules -> read 'planting_calendar.md'\n"
    "- For harvest history -> read 'harvests.md'\n"
    "- For weather/dates -> read 'almanac.md'\n"
    "- For today's weather/daily briefing/forecast -> read 'daily_YYYY-MM-DD.md' (use today's date). This includes current conditions, 48-hour forecast, and the briefing.\n"
    "- For what I did today/recently -> read 'garden_log.md'.\n"
    "- For categories/groups of plants/files -> read 'categories.md'. This file is auto-generated by !consolidate. Just summarize what's in it, do NOT try to list files from memory.\n"
    "If you are unsure of the filename, call 'tool_list_files'.\n"
    "Do not answer from memory. Do not say you have processed data unless you have called the tool.\n"
    "IMAGES:\n"
    "- Users may send photos. You can see them directly.\n"
    "- For garden layout photos: Extract spatial information (bed locations, what's planted where, "
    "infrastructure positions) and use 'tool_overwrite_file' on 'farm_layout.md' to update the layout. "
    "ALWAYS read 'farm_layout.md' first so you can merge new info with existing content.\n"
    "- For plant photos: Identify the plant/issue and respond conversationally.\n"
    "- For area photos with captions: Update the relevant section of 'farm_layout.md'.\n"
)


def trim_messages_for_context(messages: List[BaseMessage], max_turns: int = 10) -> List[BaseMessage]:
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


def agent_node(state: AgentState):
    """
    Standard agent node:
    1. Trim accumulated history to last ~10 turns.
    2. Prepend static instructions.
    3. Invoke model.
    """
    model = get_model()
    messages = trim_messages_for_context(state['messages'])

    conversation = [SystemMessage(content=STATIC_SYSTEM_PROMPT)] + messages

    response = model.invoke(conversation)
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


