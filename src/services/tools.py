import os
import glob
import json
import re
import shutil
from datetime import date, datetime, timedelta
import logging

logger = logging.getLogger(__name__)

DATA_DIR = "data"

SYSTEM_FILES = {"tasks.md", "harvests.md", "garden_log.md", "planting_calendar.md", "almanac.md", "farm_layout.md", "categories.md"}

MEMBERS_FILE = os.path.join(DATA_DIR, "members.json")


def _sanitize_topic(topic: str) -> str:
    """Sanitize a topic string into a safe, normalized filename stem."""
    return "".join(c for c in topic if c.isalnum() or c in (' ', '_', '-')).strip().lower().replace(' ', '_')


def _classify_source(source: str) -> str:
    """Classify a source string and return a formatted bullet with a quality hint tag.

    Examples:
        "https://extension.colostate.edu/garlic" -> "https://extension.colostate.edu/garlic (extension)"
        "https://www.nrcs.usda.gov/soils"       -> "https://www.nrcs.usda.gov/soils (government)"
        "https://www.rhs.org.uk/plants"          -> "https://www.rhs.org.uk/plants (organization)"
        "https://example.com/tips"               -> "https://example.com/tips (web)"
        "seed_catalog.pdf"                        -> "seed_catalog.pdf (PDF)"
        "Discord message"                         -> "Discord message (Discord)"
        "image"                                   -> "image (image)"
    """
    s = source.strip()
    lower = s.lower()

    if lower.startswith("http://") or lower.startswith("https://"):
        if ".edu" in lower:
            return f"{s} (extension)"
        if ".gov" in lower:
            return f"{s} (government)"
        if ".org" in lower:
            return f"{s} (organization)"
        return f"{s} (web)"
    if lower.endswith(".pdf"):
        return f"{s} (PDF)"
    if lower == "discord message":
        return f"{s} (Discord)"
    if lower == "image":
        return f"{s} (image)"
    # Fallback — return as-is
    return s


def _split_sources_section(content: str) -> tuple[str, list[str]]:
    """Split file content into (body, source_lines) at the '## Sources' header.

    Returns:
        (body text without the sources section, list of individual source lines).
        Source lines have leading '- ' stripped.
    """
    marker = "\n## Sources\n"
    idx = content.find(marker)
    if idx == -1:
        return content, []

    body = content[:idx]
    sources_block = content[idx + len(marker):]
    source_lines = []
    for line in sources_block.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            source_lines.append(stripped[2:])
        elif stripped:
            source_lines.append(stripped)
    return body, source_lines


def _list_md_paths(exclude_system: bool = True, exclude_daily: bool = True) -> list[str]:
    """List markdown file paths in the data directory, with optional filtering."""
    paths = glob.glob(os.path.join(DATA_DIR, "*.md"))
    result = []
    for p in paths:
        name = os.path.basename(p)
        if exclude_system and name in SYSTEM_FILES:
            continue
        if exclude_daily and name.startswith("daily_"):
            continue
        result.append(p)
    return result


def is_onboarding_complete() -> bool:
    """Check if onboarding is complete (almanac.md exists)."""
    return os.path.exists(os.path.join(DATA_DIR, "almanac.md"))


def _load_members() -> dict[str, int]:
    """Reads data/members.json, returns {lowercase_name: discord_id}."""
    if not os.path.exists(MEMBERS_FILE):
        return {}
    try:
        with open(MEMBERS_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load members: {e}")
        return {}


def _save_members(members: dict[str, int]) -> None:
    """Writes data/members.json."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(MEMBERS_FILE, "w") as f:
        json.dump(members, f, indent=2)


def register_member(name: str, discord_id: int) -> str:
    """Upserts a name -> discord_id mapping in the member registry."""
    members = _load_members()
    key = name.strip().lower()
    members[key] = discord_id
    _save_members(members)
    return f"Registered '{name}' (ID: {discord_id})."


def unregister_member(name: str) -> str:
    """Removes a name from the member registry."""
    members = _load_members()
    key = name.strip().lower()
    if key not in members:
        return f"'{name}' is not registered."
    del members[key]
    _save_members(members)
    return f"Unregistered '{name}'."


def get_member_discord_id(name: str) -> int | None:
    """Lookup discord_id by name."""
    return _load_members().get(name.strip().lower())


def get_member_name_by_discord_id(discord_id: int) -> str | None:
    """Reverse lookup: find a name by discord_id."""
    for name, did in _load_members().items():
        if did == discord_id:
            return name
    return None


def list_members() -> dict[str, int]:
    """Returns full member registry {name: discord_id}."""
    return _load_members()


def list_knowledge_files() -> str:
    """
    Lists all available markdown files in the data directory.
    Returns:
        str: A comma-separated list of filenames.
    """
    files = glob.glob(os.path.join(DATA_DIR, "*.md"))
    file_list = [os.path.basename(f) for f in files]
    return ", ".join(file_list)

def read_knowledge_file(filename: str) -> str:
    """
    Reads the content of a specific knowledge file.
    Args:
        filename: The name of the file to read (e.g., 'tomatoes.md').
    """
    # Security check: prevent directory traversal
    safe_name = os.path.basename(filename)
    path = os.path.join(DATA_DIR, safe_name)
    
    if not os.path.exists(path):
        return f"Error: File '{safe_name}' not found."
    
    try:
        with open(path, "r") as f:
            return f.read()
    except Exception as e:
        logger.error(f"Failed to read {path}: {e}")
        return f"Error reading file: {str(e)}"

def update_journal(entry: str) -> str:
    """
    Appends a new entry to 'garden_log.md' with a timestamp.
    Use this to log user activities or daily garden updates.
    
    Args:
        entry: The text content to log (e.g., "Planted 3 rows of garlic").
    """
    filename = "garden_log.md"
    path = os.path.join(DATA_DIR, filename)
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_entry = f"\n- [{timestamp}] {entry}"
    
    try:
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write(f"# Garden Log\n")
        
        with open(path, "a") as f:
            f.write(formatted_entry)
            
        return f"Successfully logged to {filename}."
    except Exception as e:
        logger.error(f"Failed to write to {path}: {e}")
        return f"Error writing to file: {str(e)}"

def amend_topic_knowledge(topic: str, content: str, source: str = "") -> str:
    """
    Appends knowledge or notes to a specific topic file. Creates the file if it doesn't exist.
    Use this when the user mentions specific facts about a plant or subject that should be remembered long-term.

    Args:
        topic: The topic name, which becomes the filename (e.g., 'garlic' -> 'garlic.md').
        content: The note or fact to append.
        source: Optional provenance string (URL, PDF filename, 'Discord message', 'image').
    """
    safe_topic = _sanitize_topic(topic)
    filename = f"{safe_topic}.md"
    path = os.path.join(DATA_DIR, filename)

    timestamp = datetime.now().strftime("%Y-%m-%d")
    new_block = f"\n\n### Update {timestamp}\n{content}"

    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                existing = f.read()
            body, existing_sources = _split_sources_section(existing)
        else:
            body = f"# {safe_topic.replace('_', ' ').title()}\n"
            existing_sources = []

        body += new_block

        # Add new source (deduplicated)
        if source.strip():
            classified = _classify_source(source)
            if classified not in existing_sources:
                existing_sources.append(classified)

        # Rebuild file: body + sources section
        if existing_sources:
            sources_block = "\n## Sources\n" + "\n".join(f"- {s}" for s in existing_sources) + "\n"
        else:
            sources_block = ""

        with open(path, "w") as f:
            f.write(body + sources_block)

        return f"Successfully updated knowledge for '{safe_topic}'."
    except Exception as e:
        logger.error(f"Failed to write to {path}: {e}")
        return f"Error updating topic file: {str(e)}"

def add_task(task_description: str, due_date: str = "", assigned_to: str = "", skip_duplicate_check: bool = False, recurring: str = "") -> str:
    """
    Adds a task to the task list.
    Args:
        task_description: The description of the task (e.g., "Fertilize the garlic").
        due_date: Optional due date in YYYY-MM-DD format.
        assigned_to: Optional name of the person this task is assigned to.
        skip_duplicate_check: If True, skip duplicate detection and add directly.
        recurring: Optional recurrence pattern (daily, weekly, monthly, every N days, every N weeks).
    """
    filename = "tasks.md"
    path = os.path.join(DATA_DIR, filename)

    # Validate recurrence
    if recurring.strip():
        if _parse_recurrence(recurring) is None:
            return f"Invalid recurrence pattern '{recurring}'. Valid patterns: {VALID_RECURRENCE_PATTERNS}"
        if not due_date:
            return f"Recurring tasks require a due_date. Please provide a due date in YYYY-MM-DD format."

    if not skip_duplicate_check:
        existing = get_open_tasks()
        if existing:
            similar = _find_similar_tasks(task_description, existing)
            if similar:
                bullet_list = "\n".join(f"- {t}" for t in similar)
                return (
                    f"Similar task(s) already exist:\n{bullet_list}\n"
                    "Call tool_add_task again with skip_duplicate_check=True to add anyway, "
                    "or use tool_complete_task to mark the old one done first."
                )

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    assigned_str = f" [Assigned: {assigned_to.strip()}]" if assigned_to.strip() else ""
    recurring_str = f" [Recurring: {recurring.strip()}]" if recurring.strip() else ""
    due_str = f" [Due: {due_date}]" if due_date else ""
    formatted_entry = f"\n- [ ] {task_description}{assigned_str}{recurring_str}{due_str} (Created: {timestamp})"

    try:
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write(f"# Task List\n")

        with open(path, "a") as f:
            f.write(formatted_entry)

        return f"Successfully added task: {task_description}"
    except Exception as e:
        logger.error(f"Failed to add task: {e}")
        return f"Error adding task: {str(e)}"

def log_harvest(crop: str, amount: str, location: str, notes: str = "") -> str:
    """
    Logs a harvest event.
    Args:
        crop: The name of the crop (e.g., "Tomatoes").
        amount: The yield amount (e.g., "5 lbs", "12 items").
        location: Where it was harvested (e.g., "Bed 2").
        notes: Optional observations.
    """
    filename = "harvests.md"
    path = os.path.join(DATA_DIR, filename)
    
    timestamp = datetime.now().strftime("%Y-%m-%d")
    
    # Table header if file is new
    header = "| Date | Crop | Amount | Location | Notes |\n|---|---|---|---|---|\n"
    entry = f"| {timestamp} | {crop} | {amount} | {location} | {notes} |\n"
    
    try:
        write_header = not os.path.exists(path)
        
        with open(path, "a") as f:
            if write_header:
                f.write("# Harvest Log\n\n" + header)
            f.write(entry)
            
        return f"Successfully logged harvest: {amount} of {crop} from {location}."
    except Exception as e:
        logger.error(f"Failed to log harvest: {e}")
        return f"Error logging harvest: {str(e)}"

def overwrite_knowledge_file(filename: str, content: str) -> str:
    """
    Overwrites (or creates) a knowledge file with new content. 
    CRITICAL: This replaces the ENTIRE file content.
    Use this to update the status of tasks (e.g. checking boxes) or rewriting the calendar.
    
    Args:
        filename: The filename (e.g. 'tasks.md', 'planting_calendar.md').
        content: The full new content of the file.
    """
    # Security check: prevent directory traversal
    safe_name = os.path.basename(filename)
    path = os.path.join(DATA_DIR, safe_name)
    
    try:
        with open(path, "w") as f:
            f.write(content)
        return f"Successfully overwrote {safe_name}."
    except Exception as e:
        logger.error(f"Failed to overwrite {path}: {e}")
        return f"Error overwriting file: {str(e)}"

def generate_calendar_from_library() -> str:
    """
    Scans all knowledge files in data/ for planting info and generates 'planting_calendar.md'.
    """
    calendar_entries = []

    count = 0
    for file_path in _list_md_paths():
        filename = os.path.basename(file_path)
            
        plant_name = filename.replace(".md", "").replace("_", " ").title()
        
        try:
            with open(file_path, "r") as f:
                content = f.read()

            # Strip ## Sources section so URLs don't get parsed as planting data
            content, _ = _split_sources_section(content)

            # Regex heuristics for planting info
            spring_match = re.search(r"\*\*Spring Planting Dates.*?\*\*(.*?)(?=\*\*Fall|\Z)", content, re.DOTALL)
            fall_match = re.search(r"\*\*Fall Planting Dates.*?\*\*(.*?)(?=\n###|\Z)", content, re.DOTALL)
            generic_sow = re.search(r"When to Sow \(Outdoors\): (.*)", content)
            
            entry = ""
            has_data = False
            
            if spring_match:
                # Clean up: replace newlines with spaces or keep structure
                text = spring_match.group(1).strip()
                if "N/A" not in text:
                    entry += f"  - **Spring:**\n    {text.replace('* ', '').replace('\n', ' ')}\n"
                    has_data = True
            if fall_match:
                text = fall_match.group(1).strip()
                if "N/A" not in text:
                    entry += f"  - **Fall:**\n    {text.replace('* ', '').replace('\n', ' ')}\n"
                    has_data = True
            
            if not has_data and generic_sow:
                text = generic_sow.group(1).strip()
                if "N/A" not in text:
                    entry += f"  - **Sow:** {text}\n"
                    has_data = True
                
            if has_data:
                calendar_entries.append(f"### {plant_name}\n{entry}")
                count += 1
                
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")

    output_content = "# Planting Calendar\n\nGenerated from knowledge library files.\n\n"
    if calendar_entries:
        output_content += "\n".join(sorted(calendar_entries))
    else:
        output_content += "No specific planting dates found in library files."

    return overwrite_knowledge_file("planting_calendar.md", output_content)

def find_related_files(topic: str) -> str:
    """
    Searches the data directory for all files related to a topic/plant.
    Returns filenames that contain the topic as a substring.
    Args:
        topic: The topic to search for (e.g., 'tomato', 'pepper', 'garlic').
    """
    safe_topic = _sanitize_topic(topic)
    matches = []
    for f in _list_md_paths(exclude_system=False, exclude_daily=True):
        basename = os.path.basename(f).lower()
        if safe_topic in basename:
            matches.append(os.path.basename(f))
    if not matches:
        return f"No files found related to '{topic}'."
    return "Related files: " + ", ".join(sorted(matches))


_STOP_WORDS = frozenset({
    "the", "a", "an", "in", "to", "for", "and", "of", "on", "at", "is", "it",
    "by", "or", "be", "as", "do", "if", "up", "my", "so", "no", "we", "all",
    "with", "this", "that", "from", "but", "not", "are", "was", "has", "had",
})

_TASK_METADATA_RE = re.compile(
    r'\[Assigned:\s*[^\]]*\]|\[Due:\s*[^\]]*\]|\[Recurring:\s*[^\]]*\]|\(Created:\s*[^\)]*\)|^- \[.\]\s*',
)

_RECURRENCE_RE = re.compile(r'\[Recurring:\s*([^\]]+)\]', re.IGNORECASE)

_EVERY_N_RE = re.compile(r'every\s+(\d+)\s+(day|week)s?', re.IGNORECASE)

VALID_RECURRENCE_PATTERNS = "daily, weekly, monthly, every N days, every N weeks"


def _parse_recurrence(pattern: str) -> timedelta | str | None:
    """Parse a recurrence pattern string into a timedelta or 'monthly' sentinel.

    Returns None if the pattern is invalid.
    """
    p = pattern.strip().lower()
    if p == "daily":
        return timedelta(days=1)
    if p == "weekly":
        return timedelta(weeks=1)
    if p == "monthly":
        return "monthly"
    m = _EVERY_N_RE.fullmatch(p)
    if m:
        n = int(m.group(1))
        if n < 1:
            return None
        unit = m.group(2).lower()
        if unit == "day":
            return timedelta(days=n)
        if unit == "week":
            return timedelta(weeks=n)
    return None


def _compute_next_due(current_due_str: str, pattern: str, today: date | None = None) -> str:
    """Compute the next due date for a recurring task.

    Uses max(current_due, today) as the base so overdue tasks don't reschedule into the past.
    Monthly handles month-end clamping (e.g. Jan 31 → Feb 28).

    Returns the next due date as a YYYY-MM-DD string.
    """
    if today is None:
        today = date.today()

    current_due = date.fromisoformat(current_due_str)
    base = max(current_due, today)

    delta = _parse_recurrence(pattern)
    if delta is None:
        return ""

    if delta == "monthly":
        # Advance by one month with day clamping
        import calendar
        month = base.month + 1
        year = base.year
        if month > 12:
            month = 1
            year += 1
        max_day = calendar.monthrange(year, month)[1]
        day = min(base.day, max_day)
        return date(year, month, day).isoformat()

    next_date = base + delta
    return next_date.isoformat()


def _find_similar_tasks(description: str, existing_tasks: list[str], threshold: float = 0.5) -> list[str]:
    """Find existing tasks similar to `description` using word-overlap (Jaccard similarity).

    Strips metadata (assigned, due, created, checkbox prefix) before comparing.
    Returns existing task lines where similarity >= threshold.
    """
    def _tokenize(text: str) -> set[str]:
        cleaned = _TASK_METADATA_RE.sub("", text).strip()
        return {w for w in cleaned.lower().split() if w not in _STOP_WORDS and len(w) > 1}

    new_tokens = _tokenize(description)
    if not new_tokens:
        return []

    matches = []
    for task in existing_tasks:
        task_tokens = _tokenize(task)
        if not task_tokens:
            continue
        intersection = new_tokens & task_tokens
        union = new_tokens | task_tokens
        similarity = len(intersection) / len(union)
        if similarity >= threshold:
            matches.append(task)
    return matches


def get_open_tasks() -> list[str]:
    """
    Reads tasks.md and returns a list of incomplete task lines.
    """
    filename = "tasks.md"
    path = os.path.join(DATA_DIR, filename)

    if not os.path.exists(path):
        return []

    try:
        with open(path, "r") as f:
            lines = f.readlines()
        return [line.strip() for line in lines if "- [ ]" in line]
    except Exception as e:
        logger.error(f"Failed to read tasks: {e}")
        return []


def get_tasks_for_user(name: str) -> list[str]:
    """
    Returns open tasks assigned to `name` plus all unassigned open tasks.
    Tasks assigned to other people are excluded.
    """
    assigned_re = re.compile(r'\[Assigned:\s*([^\]]+)\]', re.IGNORECASE)
    open_tasks = get_open_tasks()
    result = []
    name_lower = name.strip().lower()
    for task in open_tasks:
        match = assigned_re.search(task)
        if match:
            assignee = match.group(1).strip().lower()
            if assignee == name_lower:
                result.append(task)
            # else: assigned to someone else, skip
        else:
            # Unassigned — include for everyone
            result.append(task)
    return result


def filter_tasks_due_today_or_overdue(tasks: list[str], today_str: str) -> list[str]:
    """Filter tasks to only those due today or overdue (or with no due date).

    Args:
        tasks: List of raw task lines from tasks.md.
        today_str: Today's date as YYYY-MM-DD string (timezone-aware from caller).
    """
    due_re = re.compile(r'\[Due:\s*(\d{4}-\d{2}-\d{2})\]')
    result = []
    for task in tasks:
        match = due_re.search(task)
        if match:
            if match.group(1) <= today_str:
                result.append(task)
        else:
            # No due date — include (can't be deferred)
            result.append(task)
    return result


def get_current_date() -> str:
    """
    Returns the current date and time.
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")


def web_search(query: str, max_results: int = 5) -> str:
    """Search the web via DuckDuckGo and return formatted results."""
    max_results = max(1, min(10, max_results))
    try:
        from ddgs import DDGS
    except ImportError:
        return "Error: ddgs package is not installed."
    try:
        results = DDGS().text(query, max_results=max_results)
    except Exception as e:
        logger.error(f"Web search failed for query '{query}': {e}")
        return f"Web search failed: {str(e)}"
    if not results:
        return f"No web results found for '{query}'."
    parts = [f"Web search results for '{query}':\n"]
    for i, r in enumerate(results, 1):
        title = r.get("title", "No title")
        href = r.get("href", "")
        body = r.get("body", "No description")
        parts.append(f"{i}. **{title}**\n   URL: {href}\n   {body}\n")
    return "\n".join(parts)


def search_file_contents(query: str) -> list[str]:
    """
    Searches inside all .md files for case-insensitive mentions of a topic.
    Returns list of matching filenames (excluding system files and daily_* files).
    """
    matches = []
    query_lower = query.lower()
    for file_path in _list_md_paths():
        filename = os.path.basename(file_path)
        try:
            with open(file_path, "r") as f:
                content = f.read()
            if query_lower in content.lower():
                matches.append(filename)
        except Exception as e:
            logger.error(f"Error searching {filename}: {e}")
    return sorted(matches)


def search_knowledge(query: str = "") -> str:
    """Unified discovery: list files or search by filename and content.

    - Empty query → lists all files (delegates to list_knowledge_files()).
    - Non-empty → searches filenames AND content, returns deduplicated results
      with match-type annotations.
    """
    if not query.strip():
        return list_knowledge_files()

    filename_matches = set()
    safe_query = _sanitize_topic(query)
    for f in _list_md_paths(exclude_system=False, exclude_daily=True):
        basename = os.path.basename(f).lower()
        if safe_query in basename:
            filename_matches.add(os.path.basename(f))

    content_matches = set(search_file_contents(query))

    if not filename_matches and not content_matches:
        return f"No files found related to '{query}'."

    parts = []
    # Files matching by name only
    name_only = filename_matches - content_matches
    if name_only:
        parts.append("Filename matches: " + ", ".join(sorted(name_only)))
    # Files matching both name and content
    both = filename_matches & content_matches
    if both:
        parts.append("Filename + content matches: " + ", ".join(sorted(both)))
    # Files matching by content only
    content_only = content_matches - filename_matches
    if content_only:
        parts.append("Content matches: " + ", ".join(sorted(content_only)))

    return "\n".join(parts)


def read_multiple_files(filenames: list[str]) -> dict[str, str]:
    """
    Reads multiple files in one call.
    Returns {filename: content} dict.
    """
    results = {}
    for filename in filenames:
        safe_name = os.path.basename(filename)
        path = os.path.join(DATA_DIR, safe_name)
        if not os.path.exists(path):
            results[safe_name] = f"Error: File '{safe_name}' not found."
            continue
        try:
            with open(path, "r") as f:
                results[safe_name] = f.read()
        except Exception as e:
            logger.error(f"Failed to read {path}: {e}")
            results[safe_name] = f"Error reading file: {str(e)}"
    return results


def backup_file(filename: str) -> str:
    """
    Copies a file to data/backups/ with a timestamp suffix.
    """
    safe_name = os.path.basename(filename)
    src_path = os.path.join(DATA_DIR, safe_name)

    if not os.path.exists(src_path):
        return f"Error: File '{safe_name}' not found."

    backup_dir = os.path.join(DATA_DIR, "backups")
    os.makedirs(backup_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{safe_name}.{timestamp}.bak"
    dst_path = os.path.join(backup_dir, backup_name)

    try:
        shutil.copy2(src_path, dst_path)
        return f"Backed up '{safe_name}' to 'backups/{backup_name}'."
    except Exception as e:
        logger.error(f"Failed to backup {safe_name}: {e}")
        return f"Error backing up file: {str(e)}"


def delete_knowledge_file(filename: str) -> str:
    """
    Deletes a file from data/. Cannot delete system files.
    """
    safe_name = os.path.basename(filename)

    if safe_name in SYSTEM_FILES:
        return f"Error: '{safe_name}' is a protected system file and cannot be deleted."

    path = os.path.join(DATA_DIR, safe_name)
    if not os.path.exists(path):
        return f"Error: File '{safe_name}' not found."

    try:
        os.remove(path)
        return f"Deleted '{safe_name}'."
    except Exception as e:
        logger.error(f"Failed to delete {safe_name}: {e}")
        return f"Error deleting file: {str(e)}"


def get_library_files() -> list[dict]:
    """
    Scan all knowledge files (excluding system files, daily_* files).
    Returns list of {filename, title, size_bytes}.
    """
    entries = []

    for file_path in _list_md_paths():
        filename = os.path.basename(file_path)
        try:
            size = os.path.getsize(file_path)
            with open(file_path, "r") as f:
                first_line = f.readline().strip().lstrip("# ")
            entries.append({
                "filename": filename,
                "title": first_line or filename,
                "size_bytes": size,
            })
        except Exception as e:
            logger.error(f"Error indexing {filename}: {e}")

    return entries


def build_library_index() -> list[dict]:
    """
    Scans all knowledge files (excluding system files, daily_* files).
    Returns list of grouped dicts with {topic, files} using prefix-based grouping.
    """
    entries = get_library_files()

    # Group by shared filename prefix (stem before first underscore or full stem)
    from collections import defaultdict
    groups = defaultdict(list)
    for entry in entries:
        stem = entry["filename"].replace(".md", "")
        prefix = stem.split("_")[0] if "_" in stem else stem
        groups[prefix].append(entry)

    # Only return groups with 2+ files
    grouped = []
    for prefix, group_entries in sorted(groups.items()):
        if len(group_entries) >= 2:
            grouped.append({
                "topic": prefix,
                "files": sorted(group_entries, key=lambda e: e["filename"]),
            })

    return grouped

def get_clearable_knowledge_files() -> list[str]:
    """Returns filenames of all non-system, non-daily .md files that can be cleared."""
    return [os.path.basename(p) for p in _list_md_paths()]


def clear_all_knowledge_files() -> tuple[list[str], list[str]]:
    """
    Deletes all non-system knowledge files.
    Returns (deleted_names, errors).
    """
    deleted = []
    errors = []
    for path in _list_md_paths():
        name = os.path.basename(path)
        try:
            os.remove(path)
            deleted.append(name)
        except Exception as e:
            logger.error(f"Failed to delete {name}: {e}")
            errors.append(f"{name}: {e}")
    return deleted, errors


def clear_entire_garden() -> dict:
    """
    Factory reset — removes everything in data/.
    Returns {"deleted_files": [...], "deleted_dirs": [...], "errors": [...]}.
    """
    abs_data_dir = os.path.abspath(DATA_DIR)
    deleted_files = []
    deleted_dirs = []
    errors = []

    for entry in os.listdir(abs_data_dir):
        entry_path = os.path.join(abs_data_dir, entry)
        # Defense-in-depth: verify resolved path is inside data/
        resolved = os.path.realpath(entry_path)
        if not resolved.startswith(abs_data_dir):
            errors.append(f"{entry}: path escapes data directory, skipped")
            continue
        try:
            if os.path.isdir(resolved):
                shutil.rmtree(resolved)
                deleted_dirs.append(entry)
            else:
                os.remove(resolved)
                deleted_files.append(entry)
        except Exception as e:
            logger.error(f"Failed to remove {entry}: {e}")
            errors.append(f"{entry}: {e}")

    return {"deleted_files": deleted_files, "deleted_dirs": deleted_dirs, "errors": errors}


def reassign_tasks(from_name: str, to_name: str) -> str:
    """Reassign all open tasks from one person to another (or assign unassigned tasks) in bulk.

    Args:
        from_name: Current assignee name, or "unassigned" to match tasks with no [Assigned:] tag.
        to_name: New assignee name.
    """
    filename = "tasks.md"
    path = os.path.join(DATA_DIR, filename)

    if not os.path.exists(path):
        return "Task list file does not exist."

    try:
        with open(path, "r") as f:
            lines = f.readlines()

        new_lines = []
        reassigned = []
        assigned_re = re.compile(r'\[Assigned:\s*([^\]]+)\]', re.IGNORECASE)
        is_unassigned = from_name.strip().lower() in ("", "unassigned")

        for line in lines:
            if "- [ ]" not in line:
                new_lines.append(line)
                continue

            match = assigned_re.search(line)

            if is_unassigned:
                # Match open tasks with no [Assigned:] tag
                if match:
                    new_lines.append(line)
                    continue
                # Insert [Assigned: to_name] after the description, before other metadata
                # Find the first metadata tag position
                desc_end = len(line.rstrip("\n"))
                for tag_re in [r'\[Recurring:', r'\[Due:', r'\(Created:']:
                    tag_match = re.search(tag_re, line)
                    if tag_match and tag_match.start() < desc_end:
                        desc_end = tag_match.start()
                new_line = line[:desc_end].rstrip() + f" [Assigned: {to_name.strip()}]" + line[desc_end:]
                new_lines.append(new_line)
                # Extract description for summary
                desc = re.sub(r'^- \[[ x]\] ', '', line.strip())
                desc = re.sub(r'\s*\[.*?\]|\s*\(Created:.*?\)', '', desc).strip()
                reassigned.append(desc)
            elif match and match.group(1).strip().lower() == from_name.strip().lower():
                # Replace existing assignment
                new_line = line[:match.start()] + f"[Assigned: {to_name.strip()}]" + line[match.end():]
                new_lines.append(new_line)
                desc = re.sub(r'^- \[[ x]\] ', '', line.strip())
                desc = re.sub(r'\s*\[.*?\]|\s*\(Created:.*?\)', '', desc).strip()
                reassigned.append(desc)
            else:
                new_lines.append(line)

        if not reassigned:
            source = "unassigned tasks" if is_unassigned else f"tasks assigned to '{from_name}'"
            return f"No open {source} found."

        with open(path, "w") as f:
            f.writelines(new_lines)

        source = "unassigned" if is_unassigned else from_name.strip()
        summary = f"Reassigned {len(reassigned)} task(s) from {source} to {to_name.strip()}:\n"
        summary += "\n".join(f"- {desc}" for desc in reassigned)
        return summary
    except Exception as e:
        logger.error(f"Failed to reassign tasks: {e}")
        return f"Error reassigning tasks: {str(e)}"


def remove_tasks(snippet: str) -> str:
    """Remove (permanently delete) all open tasks whose description matches a substring.

    Unlike complete_task (which checks the box), this deletes the lines entirely.
    Only matches open tasks (``- [ ]``). Completed tasks are never touched.

    Args:
        snippet: Case-insensitive substring to match against task lines.
    """
    filename = "tasks.md"
    path = os.path.join(DATA_DIR, filename)

    if not os.path.exists(path):
        return "Task list file does not exist."

    try:
        with open(path, "r") as f:
            lines = f.readlines()

        kept: list[str] = []
        removed: list[str] = []
        snippet_lower = snippet.lower()

        for line in lines:
            if "- [ ]" in line and snippet_lower in line.lower():
                removed.append(line.strip())
            else:
                kept.append(line)

        if not removed:
            return f"No open tasks found matching '{snippet}'."

        with open(path, "w") as f:
            f.writelines(kept)

        return f"Removed {len(removed)} task(s) matching '{snippet}':\n" + "\n".join(f"- {t}" for t in removed)
    except Exception as e:
        logger.error(f"Failed to remove tasks: {e}")
        return f"Error removing tasks: {str(e)}"


def complete_task(task_snippet: str) -> str:
    """
    Marks a task as complete in 'tasks.md' by finding a matching line.
    Args:
        task_snippet: A substring of the task description (e.g., "Mulch garden").
    """
    filename = "tasks.md"
    path = os.path.join(DATA_DIR, filename)
    
    if not os.path.exists(path):
        return "Task list file does not exist."
    
    try:
        with open(path, "r") as f:
            lines = f.readlines()
            
        new_lines = []
        found = False
        
        for line in lines:
            # Check if line is a task, matches snippet, and is not already done
            if "- [ ]" in line and task_snippet.lower() in line.lower():
                new_line = line.replace("- [ ]", "- [x]", 1)
                new_lines.append(new_line)
                found = True
            else:
                new_lines.append(line)
        
        if found:
            with open(path, "w") as f:
                f.writelines(new_lines)

            # Auto-log to garden journal
            # Extract clean task description for the log
            # Line format: "- [ ] Description [Due: ...] (Created: ...)\n"
            clean_desc = task_snippet # Default fallback
            matched_line = ""
            # Try to get the actual line content
            for line in lines:
                 if "- [ ]" in line and task_snippet.lower() in line.lower():
                     matched_line = line
                     # Remove "- [ ] "
                     parts = line.split("- [ ] ", 1)
                     if len(parts) > 1:
                         clean_desc = parts[1].strip()
                     break

            update_journal(f"Completed task: {clean_desc}")

            result_msg = f"Successfully marked task matching '{task_snippet}' as complete and logged to journal."

            # Auto-reschedule recurring tasks
            if matched_line:
                recurrence_match = _RECURRENCE_RE.search(matched_line)
                if recurrence_match:
                    pattern = recurrence_match.group(1).strip()
                    due_match = re.search(r'\[Due:\s*(\d{4}-\d{2}-\d{2})\]', matched_line)
                    if due_match:
                        next_due = _compute_next_due(due_match.group(1), pattern)
                        if next_due:
                            # Extract assignee if present
                            assigned_match = re.search(r'\[Assigned:\s*([^\]]+)\]', matched_line)
                            assignee = assigned_match.group(1).strip() if assigned_match else ""
                            # Extract clean description (strip all metadata)
                            desc = re.sub(r'^- \[[ x]\] ', '', matched_line.strip())
                            desc = re.sub(r'\s*\[Assigned:\s*[^\]]*\]', '', desc)
                            desc = re.sub(r'\s*\[Recurring:\s*[^\]]*\]', '', desc)
                            desc = re.sub(r'\s*\[Due:\s*[^\]]*\]', '', desc)
                            desc = re.sub(r'\s*\(Created:\s*[^)]*\)', '', desc).strip()
                            add_task(desc, due_date=next_due, assigned_to=assignee,
                                     skip_duplicate_check=True, recurring=pattern)
                            result_msg += f" Next occurrence scheduled for {next_due}."
                    else:
                        result_msg += " Warning: recurring task has no [Due:] date — cannot reschedule."

            return result_msg
        else:
            return f"No pending task found matching '{task_snippet}'."
            
    except Exception as e:
        logger.error(f"Failed to complete task: {e}")
        return f"Error completing task: {str(e)}"


