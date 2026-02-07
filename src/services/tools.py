import os
import glob
import shutil
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

DATA_DIR = "data"

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

def amend_topic_knowledge(topic: str, content: str) -> str:
    """
    Appends knowledge or notes to a specific topic file. Creates the file if it doesn't exist.
    Use this when the user mentions specific facts about a plant or subject that should be remembered long-term.
    
    Args:
        topic: The topic name, which becomes the filename (e.g., 'garlic' -> 'garlic.md').
        content: The note or fact to append.
    """
    # Sanitize topic to be a valid filename
    safe_topic = "".join(c for c in topic if c.isalnum() or c in (' ', '_', '-')).strip().lower().replace(' ', '_')
    filename = f"{safe_topic}.md"
    path = os.path.join(DATA_DIR, filename)
    
    timestamp = datetime.now().strftime("%Y-%m-%d")
    formatted_entry = f"\n\n### Update {timestamp}\n{content}"
    
    try:
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write(f"# {safe_topic.replace('_', ' ').title()}\n")
        
        with open(path, "a") as f:
            f.write(formatted_entry)
            
        return f"Successfully updated knowledge for '{safe_topic}'."
    except Exception as e:
        logger.error(f"Failed to write to {path}: {e}")
        return f"Error updating topic file: {str(e)}"

def add_task(task_description: str, due_date: str = "") -> str:
    """
    Adds a task to the task list.
    Args:
        task_description: The description of the task (e.g., "Fertilize the garlic").
        due_date: Optional due date in YYYY-MM-DD format.
    """
    filename = "tasks.md"
    path = os.path.join(DATA_DIR, filename)
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    due_str = f" [Due: {due_date}]" if due_date else ""
    formatted_entry = f"\n- [ ] {task_description}{due_str} (Created: {timestamp})"

    
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
    import re
    files = glob.glob(os.path.join(DATA_DIR, "*.md"))
    calendar_entries = []
    
    # Files to skip
    skip_files = ["almanac.md", "farm_layout.md", "garden_log.md", "tasks.md", "harvests.md", "planting_calendar.md"]

    count = 0
    for file_path in files:
        filename = os.path.basename(file_path)
        if filename in skip_files or filename.startswith("daily_"):
            continue
            
        plant_name = filename.replace(".md", "").replace("_", " ").title()
        
        try:
            with open(file_path, "r") as f:
                content = f.read()
                
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
    safe_topic = "".join(c for c in topic if c.isalnum() or c in (' ', '_', '-')).strip().lower().replace(' ', '_')
    files = glob.glob(os.path.join(DATA_DIR, "*.md"))
    matches = []
    for f in files:
        basename = os.path.basename(f).lower()
        if safe_topic in basename:
            matches.append(os.path.basename(f))
    if not matches:
        return f"No files found related to '{topic}'."
    return "Related files: " + ", ".join(sorted(matches))


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


def get_current_date() -> str:
    """
    Returns the current date and time.
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")


SYSTEM_FILES = {"tasks.md", "harvests.md", "garden_log.md", "planting_calendar.md", "almanac.md", "farm_layout.md", "categories.md"}


def search_file_contents(query: str) -> list[str]:
    """
    Searches inside all .md files for case-insensitive mentions of a topic.
    Returns list of matching filenames (excluding system files and daily_* files).
    """
    files = glob.glob(os.path.join(DATA_DIR, "*.md"))
    matches = []
    query_lower = query.lower()
    for file_path in files:
        filename = os.path.basename(file_path)
        if filename in SYSTEM_FILES or filename.startswith("daily_"):
            continue
        try:
            with open(file_path, "r") as f:
                content = f.read()
            if query_lower in content.lower():
                matches.append(filename)
        except Exception as e:
            logger.error(f"Error searching {filename}: {e}")
    return sorted(matches)


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
    files = glob.glob(os.path.join(DATA_DIR, "*.md"))
    entries = []

    for file_path in files:
        filename = os.path.basename(file_path)
        if filename in SYSTEM_FILES or filename.startswith("daily_"):
            continue
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
            # Try to get the actual line content
            for line in lines:
                 if "- [ ]" in line and task_snippet.lower() in line.lower():
                     # Remove "- [ ] "
                     parts = line.split("- [ ] ", 1)
                     if len(parts) > 1:
                         clean_desc = parts[1].strip()
                     break
            
            update_journal(f"Completed task: {clean_desc}")
            
            return f"Successfully marked task matching '{task_snippet}' as complete and logged to journal."
        else:
            return f"No pending task found matching '{task_snippet}'."
            
    except Exception as e:
        logger.error(f"Failed to complete task: {e}")
        return f"Error completing task: {str(e)}"


