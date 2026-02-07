
import os
import re
import json
import logging
from typing import List, TypedDict, Annotated
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage, AIMessage, ToolMessage
from langchain_core.tools import tool
from langchain_google_vertexai import ChatVertexAI
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
def tool_add_task(task_description: str, due_date: str = ""):
    """Adds a task to the tracker. Args: task_description (text), due_date (YYYY-MM-DD, optional)"""
    return add_task(task_description, due_date)

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
]



# --- State Definition ---

class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]

# --- Node Logic ---

def get_model():
    return ChatVertexAI(
        model_name=os.getenv("VERTEX_MODEL", "gemini-2.5-flash"),
        location=os.getenv("GCP_LOCATION", "us-central1"),
        temperature=0
    ).bind_tools(TOOLS)

STATIC_SYSTEM_PROMPT = (
    "You are Ryanbot, a gardening assistant.\n"
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


def _extract_json(text: str) -> dict | list:
    """Robustly extract JSON from LLM response text, handling markdown fences and preamble."""
    text = text.strip()
    # Try to find JSON inside markdown code fences first
    fence_match = re.search(r"```(?:json)?\s*\n?(.*?)```", text, re.DOTALL)
    if fence_match:
        text = fence_match.group(1).strip()
    # Otherwise try to find the first { ... } block
    elif not text.startswith("{"):
        brace_start = text.find("{")
        if brace_start != -1:
            text = text[brace_start:]
    # Trim trailing text after the last }
    if not text.endswith("}"):
        brace_end = text.rfind("}")
        if brace_end != -1:
            text = text[:brace_end + 1]
    return json.loads(text)


async def _categorize_batch(model, batch: list[dict]) -> dict[str, list[str]]:
    """Categorize a single batch of files. Returns {category: [filenames]}."""
    file_list = "\n".join(
        f"{e['filename']}|{e['title']}" for e in batch
    )

    prompt = (
        "You are a botanical and gardening expert. Categorize each file below into ONE category.\n\n"
        "CATEGORIES (use these, add others only if truly needed):\n"
        "Trees, Shrubs, Vegetables, Herbs, Flowers, Fruits, Grasses/Grains, Vines, "
        "Groundcovers, Wildflowers/Native Plants, Wildlife, Farm/Infrastructure, "
        "Techniques/Methods, Uncategorized\n\n"
        "RULES:\n"
        "- Use botanical knowledge (e.g. smooth_sumac=Shrubs, cherry_tomato=Vegetables, lavender=Herbs)\n"
        "- Every file gets exactly one category\n"
        "- Uncategorized is a last resort\n\n"
        "INPUT (filename|title):\n"
        f"{file_list}\n\n"
        "OUTPUT: One line per file, same order as input, format: filename|Category\n"
        "No headers, no explanation, no blank lines. ONLY the filename|Category lines."
    )

    response = await model.ainvoke([HumanMessage(content=prompt)])
    text = response.content if isinstance(response.content, str) else str(response.content)

    categories: dict[str, list[str]] = {}
    for line in text.strip().splitlines():
        line = line.strip()
        if "|" not in line:
            continue
        parts = line.split("|", 1)
        filename = parts[0].strip()
        category = parts[1].strip()
        if not filename or not category:
            continue
        categories.setdefault(category, []).append(filename)

    return categories


CATEGORIZE_BATCH_SIZE = 200


async def categorize_files(file_entries: list[dict], progress_callback=None) -> dict[str, list[str]]:
    """Categorize knowledge files in batches to avoid output token limits.

    Args:
        file_entries: list of {filename, title, size_bytes} dicts.
        progress_callback: optional async function(batch_num, total_batches) for progress updates.

    Returns:
        dict mapping category names to lists of filenames.
    """
    import asyncio

    model = ChatVertexAI(
        model_name=os.getenv("VERTEX_MODEL", "gemini-2.5-flash"),
        location=os.getenv("GCP_LOCATION", "us-central1"),
        temperature=0,
        max_output_tokens=16384,
    )

    # Split into batches
    batches = [
        file_entries[i:i + CATEGORIZE_BATCH_SIZE]
        for i in range(0, len(file_entries), CATEGORIZE_BATCH_SIZE)
    ]
    logger.info(f"Categorizing {len(file_entries)} files in {len(batches)} batches of ~{CATEGORIZE_BATCH_SIZE}")

    # Run all batches concurrently
    async def run_batch(idx, batch):
        logger.info(f"Batch {idx + 1}/{len(batches)}: {len(batch)} files")
        result = await _categorize_batch(model, batch)
        if progress_callback:
            await progress_callback(idx + 1, len(batches))
        return result

    batch_results = await asyncio.gather(
        *(run_batch(i, b) for i, b in enumerate(batches))
    )

    # Merge all batch results
    categories: dict[str, list[str]] = {}
    for batch_cats in batch_results:
        for cat, files in batch_cats.items():
            categories.setdefault(cat, []).extend(files)

    total_categorized = sum(len(files) for files in categories.values())
    logger.info(f"Categorization complete: {len(categories)} categories, {total_categorized}/{len(file_entries)} files categorized")

    if total_categorized == 0:
        raise ValueError("No files were categorized across any batch")

    return categories


async def suggest_merges(categories: dict[str, list[str]]) -> list[dict]:
    """Direct LLM call to identify merge candidates within categories.

    Args:
        categories: dict mapping category names to lists of filenames.

    Returns:
        list of {"target": "file.md", "files": ["file.md", "related.md", ...]} dicts.
    """
    # Build a compact representation
    cat_lines = []
    for cat, files in sorted(categories.items()):
        cat_lines.append(f"[{cat}]: {', '.join(sorted(files))}")
    cat_text = "\n".join(cat_lines)

    prompt = (
        "You are a gardening knowledge base expert. Given these categorized files, identify groups "
        "of files that are about the SAME plant/topic and should be merged into one file.\n\n"
        "For example: garlic.md, garlic_care.md, garlic_pests.md -> merge into garlic.md\n"
        "Or: tomato.md, cherry_tomato.md, tomato_varieties.md -> merge into tomato.md\n\n"
        f"FILES BY CATEGORY:\n{cat_text}\n\n"
        "Respond with ONLY valid JSON (no markdown fences). Return a list of merge groups:\n"
        '[{"target": "main_file.md", "files": ["main_file.md", "sub1.md", "sub2.md"]}, ...]\n'
        "Only include groups with 2+ files. If no merges needed, return []."
    )

    model = ChatVertexAI(
        model_name=os.getenv("VERTEX_MODEL", "gemini-2.5-flash"),
        location=os.getenv("GCP_LOCATION", "us-central1"),
        temperature=0,
    )

    logger.info("Sending merge suggestion request to LLM...")
    response = await model.ainvoke([HumanMessage(content=prompt)])
    text = response.content if isinstance(response.content, str) else str(response.content)

    try:
        # Handle both bare array and object wrapper
        text = text.strip()
        fence_match = re.search(r"```(?:json)?\s*\n?(.*?)```", text, re.DOTALL)
        if fence_match:
            text = fence_match.group(1).strip()
        result = json.loads(text)
        if isinstance(result, dict):
            result = result.get("merge_suggestions", [])
        logger.info(f"Merge suggestions: {len(result)} groups found")
        return result
    except (json.JSONDecodeError, Exception) as e:
        logger.warning(f"Failed to parse merge suggestions: {e}")
        return []
