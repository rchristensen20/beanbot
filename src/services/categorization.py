import os
import re
import json
import logging
import asyncio

from langchain_core.messages import HumanMessage
from langchain_google_genai import ChatGoogleGenerativeAI

logger = logging.getLogger(__name__)

CATEGORIZE_BATCH_SIZE = int(os.getenv("CATEGORIZE_BATCH_SIZE", "200"))
CATEGORIZE_MAX_TOKENS = int(os.getenv("CATEGORIZE_MAX_TOKENS", "16384"))


def _extract_json(text: str) -> dict | list:
    """Robustly extract JSON (object or array) from LLM response text."""
    text = text.strip()
    fence_match = re.search(r"```(?:json)?\s*\n?(.*?)```", text, re.DOTALL)
    if fence_match:
        text = fence_match.group(1).strip()
    # Find earliest [ or {
    arr_start = text.find("[")
    obj_start = text.find("{")
    if arr_start == -1 and obj_start == -1:
        raise json.JSONDecodeError("No JSON found", text, 0)
    if arr_start != -1 and (obj_start == -1 or arr_start < obj_start):
        text = text[arr_start:]
        end = text.rfind("]")
        if end != -1:
            text = text[:end + 1]
    else:
        text = text[obj_start:]
        end = text.rfind("}")
        if end != -1:
            text = text[:end + 1]
    return json.loads(text)


async def _categorize_batch(model, batch: list[dict]) -> dict[str, dict[str, list[str]]]:
    """Categorize a single batch of files. Returns {category: {species: [filenames]}}."""
    file_list = "\n".join(
        f"{e['filename']}|{e['title']}" for e in batch
    )

    prompt = (
        "You are a botanical and gardening expert. Categorize each file below into ONE category AND one species/topic.\n\n"
        "CATEGORIES (use these, add others only if truly needed):\n"
        "Trees, Shrubs, Vegetables, Herbs, Flowers, Fruits, Grasses/Grains, Vines, "
        "Groundcovers, Wildflowers/Native Plants, Wildlife, Farm/Infrastructure, "
        "Techniques/Methods, Uncategorized\n\n"
        "RULES:\n"
        "- Use botanical knowledge (e.g. smooth_sumac=Shrubs, cherry_tomato=Vegetables, lavender=Herbs)\n"
        "- Every file gets exactly one category and one species/topic\n"
        "- Species should be the common plant name that groups related files together\n"
        "  e.g. garlic.md + garlic_care.md + garlic_pests.md all get Species=Garlic\n"
        "  e.g. cherry_tomato.md + tomato_varieties.md + tomato.md all get Species=Tomato\n"
        "- Use singular form, title case (Garlic not garlics, Tomato not tomatoes)\n"
        "- For non-plant files, use a short descriptive topic (e.g. Composting, Irrigation, Raised Beds)\n"
        "- Uncategorized is a last resort\n\n"
        "INPUT (filename|title):\n"
        f"{file_list}\n\n"
        "OUTPUT: One line per file, same order as input, format: filename|Category|Species\n"
        "No headers, no explanation, no blank lines. ONLY the filename|Category|Species lines."
    )

    response = await model.ainvoke([HumanMessage(content=prompt)])
    text = response.content if isinstance(response.content, str) else str(response.content)

    categories: dict[str, dict[str, list[str]]] = {}
    for line in text.strip().splitlines():
        line = line.strip()
        if "|" not in line:
            continue
        parts = line.split("|")
        if len(parts) >= 3:
            filename = parts[0].strip()
            category = parts[1].strip()
            species = parts[2].strip()
        elif len(parts) == 2:
            filename = parts[0].strip()
            category = parts[1].strip()
            # Fallback: derive species from filename stem
            species = filename.replace(".md", "").replace("_", " ").title()
        else:
            continue
        if not filename or not category or not species:
            continue
        categories.setdefault(category, {}).setdefault(species, []).append(filename)

    return categories


async def categorize_files(file_entries: list[dict], progress_callback=None) -> dict[str, dict[str, list[str]]]:
    """Categorize knowledge files in batches to avoid output token limits.

    Args:
        file_entries: list of {filename, title, size_bytes} dicts.
        progress_callback: optional async function(batch_num, total_batches) for progress updates.

    Returns:
        dict mapping category names to {species: [filenames]} dicts.
    """
    model = ChatGoogleGenerativeAI(
        model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        temperature=0,
        max_output_tokens=CATEGORIZE_MAX_TOKENS,
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

    # Merge all batch results (nested: {cat: {species: [files]}})
    categories: dict[str, dict[str, list[str]]] = {}
    for batch_cats in batch_results:
        for cat, species_dict in batch_cats.items():
            cat_entry = categories.setdefault(cat, {})
            for species, files in species_dict.items():
                cat_entry.setdefault(species, []).extend(files)

    total_categorized = sum(
        len(files) for species_dict in categories.values() for files in species_dict.values()
    )
    total_species = sum(len(species_dict) for species_dict in categories.values())
    logger.info(f"Categorization complete: {len(categories)} categories, {total_species} species, {total_categorized}/{len(file_entries)} files categorized")

    if total_categorized == 0:
        raise ValueError("No files were categorized across any batch")

    return categories


def derive_merge_suggestions(categories: dict[str, dict[str, list[str]]]) -> list[dict]:
    """Derive merge candidates deterministically: any species with 2+ files is a candidate.

    Args:
        categories: nested dict {category: {species: [filenames]}}.

    Returns:
        list of {"target": "file.md", "files": [...], "species": str, "category": str} dicts,
        sorted by file count descending.
    """
    suggestions = []
    for cat, species_dict in sorted(categories.items()):
        for species, files in sorted(species_dict.items()):
            if len(files) >= 2:
                target_stem = species.lower().replace(" ", "_")
                target = f"{target_stem}.md"
                if target not in files:
                    target = sorted(files)[0]
                suggestions.append({
                    "target": target,
                    "files": sorted(files),
                    "species": species,
                    "category": cat,
                })
    suggestions.sort(key=lambda s: -len(s["files"]))
    return suggestions


async def analyze_duplicate_tasks(open_tasks: list[str]) -> list[dict]:
    """Analyze open tasks for duplicates and near-duplicates using an LLM.

    Args:
        open_tasks: list of raw task lines (e.g. "- [ ] Water tomatoes [Due: ...]").

    Returns:
        list of dicts: {indices: [int], reason: str, suggested_merge: str, type: "duplicate"|"similar"}
        Indices are 0-based positions in the input list.
    """
    if len(open_tasks) < 2:
        return []

    # Strip checkbox prefix for cleaner LLM input
    numbered_lines = []
    for i, task in enumerate(open_tasks):
        clean = re.sub(r'^- \[[ x]\] ', '', task.strip())
        numbered_lines.append(f"{i}: {clean}")
    task_block = "\n".join(numbered_lines)

    prompt = (
        "You are analyzing a task list for duplicates and near-duplicates.\n\n"
        "TASKS (index: description):\n"
        f"{task_block}\n\n"
        "RULES:\n"
        "- Group tasks that are genuinely duplicate or overlapping (same work described differently)\n"
        "- Do NOT group tasks just because they involve the same plant or area\n"
        "- Each task can appear in at most ONE group\n"
        "- Tasks assigned to different people doing the SAME work ARE duplicates\n"
        "- Tasks with DIFFERENT [Recurring: ...] patterns are NOT duplicates\n"
        "- A recurring task and a non-recurring task with the same description are NOT duplicates\n"
        "- 'duplicate' = essentially identical tasks; 'similar' = overlapping scope that could be merged\n"
        "- For suggested_merge: write a single clean task line combining the best details from the group. "
        "Preserve any [Assigned: ...] and [Due: ...] tags from the EARLIEST due date. "
        "If tasks have different assignees, note both.\n\n"
        "Respond with ONLY valid JSON (no markdown fences). Return a list:\n"
        '[{"indices": [0, 3], "reason": "Both describe watering tomatoes", '
        '"suggested_merge": "Water tomatoes in bed 2 [Due: 2025-06-15]", "type": "duplicate"}, ...]\n'
        "If no duplicates or similar tasks exist, return []."
    )

    model = ChatGoogleGenerativeAI(
        model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        temperature=0,
        max_output_tokens=4096,
    )

    logger.info(f"Analyzing {len(open_tasks)} tasks for duplicates...")
    response = await model.ainvoke([HumanMessage(content=prompt)])
    text = response.content if isinstance(response.content, str) else str(response.content)

    try:
        result = _extract_json(text)
        if isinstance(result, dict):
            result = result.get("groups", result.get("duplicates", []))
        if not isinstance(result, list):
            logger.warning(f"Unexpected result type from LLM: {type(result)}")
            return []
    except (json.JSONDecodeError, Exception) as e:
        logger.warning(f"Failed to parse duplicate analysis: {e}")
        return []

    # Validate: indices in-bounds, no task in multiple groups
    max_idx = len(open_tasks) - 1
    seen_indices: set[int] = set()
    validated = []
    for group in result:
        if not isinstance(group, dict):
            continue
        indices = group.get("indices", [])
        if not isinstance(indices, list) or len(indices) < 2:
            continue
        # Check bounds and uniqueness
        valid = True
        for idx in indices:
            if not isinstance(idx, int) or idx < 0 or idx > max_idx:
                valid = False
                break
            if idx in seen_indices:
                valid = False
                break
        if not valid:
            continue
        seen_indices.update(indices)
        validated.append({
            "indices": indices,
            "reason": group.get("reason", ""),
            "suggested_merge": group.get("suggested_merge", ""),
            "type": group.get("type", "similar"),
        })

    logger.info(f"Found {len(validated)} duplicate/similar task groups")
    return validated


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

    model = ChatGoogleGenerativeAI(
        model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        temperature=0,
    )

    logger.info("Sending merge suggestion request to LLM...")
    response = await model.ainvoke([HumanMessage(content=prompt)])
    text = response.content if isinstance(response.content, str) else str(response.content)

    try:
        result = _extract_json(text)
        if isinstance(result, dict):
            result = result.get("merge_suggestions", [])
        logger.info(f"Merge suggestions: {len(result)} groups found")
        return result
    except (json.JSONDecodeError, Exception) as e:
        logger.warning(f"Failed to parse merge suggestions: {e}")
        return []
