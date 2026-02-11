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


async def categorize_files(file_entries: list[dict], progress_callback=None) -> dict[str, list[str]]:
    """Categorize knowledge files in batches to avoid output token limits.

    Args:
        file_entries: list of {filename, title, size_bytes} dicts.
        progress_callback: optional async function(batch_num, total_batches) for progress updates.

    Returns:
        dict mapping category names to lists of filenames.
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
