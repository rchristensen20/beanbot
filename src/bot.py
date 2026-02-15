import asyncio
import discord
from discord.ext import commands, tasks
import os
import re
import logging
import base64
import httpx
from datetime import date, datetime, time, timedelta
from urllib.parse import urlparse, urljoin
from zoneinfo import ZoneInfo
from langchain_core.messages import HumanMessage
from bs4 import BeautifulSoup

BOT_TZ = ZoneInfo(os.getenv("BOT_TIMEZONE", "America/Denver"))

# Import our new graph
from src.graph import init_graph
import src.graph as graph_module
# Import basic file reader for the daily report
from src.services.tools import read_knowledge_file, get_open_tasks, find_related_files, search_file_contents, build_library_index, get_library_files, is_onboarding_complete, register_member, get_member_name_by_discord_id, list_members, get_tasks_for_user, get_clearable_knowledge_files, clear_all_knowledge_files, clear_entire_garden, delete_knowledge_file, complete_task, filter_tasks_due_today_or_overdue, backup_file, overwrite_knowledge_file, _sanitize_topic, SYSTEM_FILES, DATA_DIR
from src.services.categorization import categorize_files, derive_merge_suggestions, analyze_duplicate_tasks
from src.services.weather import fetch_current_weather, fetch_forecast

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("beanbot")

DISCORD_MESSAGE_LIMIT = 2000
INGESTION_CHUNK_SIZE = int(os.getenv("INGESTION_CHUNK_SIZE", "50000"))
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "60"))
URL_FETCH_TIMEOUT = float(os.getenv("URL_FETCH_TIMEOUT", "30"))
MAX_INGEST_URLS = int(os.getenv("MAX_INGEST_URLS", "5"))
MAX_RECAP_DAYS = int(os.getenv("MAX_RECAP_DAYS", "90"))
MAX_CRAWL_LINKS = int(os.getenv("MAX_CRAWL_LINKS", "50"))
CRAWL_CONCURRENCY = int(os.getenv("CRAWL_CONCURRENCY", "5"))

# Schedule times (HH:MM in 24h format)
_briefing_h, _briefing_m = os.getenv("BRIEFING_TIME", "08:00").split(":")
BRIEFING_TIME = time(hour=int(_briefing_h), minute=int(_briefing_m), tzinfo=BOT_TZ)

_debrief_h, _debrief_m = os.getenv("DEBRIEF_TIME", "20:00").split(":")
DEBRIEF_TIME = time(hour=int(_debrief_h), minute=int(_debrief_m), tzinfo=BOT_TZ)

_recap_h, _recap_m = os.getenv("WEEKLY_RECAP_TIME", "20:00").split(":")
WEEKLY_RECAP_TIME = time(hour=int(_recap_h), minute=int(_recap_m), tzinfo=BOT_TZ)
WEEKLY_RECAP_DAY = int(os.getenv("WEEKLY_RECAP_DAY", "6"))  # 0=Mon, 6=Sun

_prune_h, _prune_m = os.getenv("DB_PRUNE_TIME", "03:00").split(":")
DB_PRUNE_TIME = time(hour=int(_prune_h), minute=int(_prune_m), tzinfo=BOT_TZ)

WEATHER_ALERT_INTERVAL_HOURS = int(os.getenv("WEATHER_ALERT_INTERVAL_HOURS", "6"))

# Retention
DB_PRUNE_RETENTION_DAYS = int(os.getenv("DB_PRUNE_RETENTION_DAYS", "7"))
DB_PRUNE_MAX_CHECKPOINTS = int(os.getenv("DB_PRUNE_MAX_CHECKPOINTS", "20"))


def _read_version() -> str:
    """Read the project version from pyproject.toml."""
    pyproject_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pyproject.toml")
    try:
        with open(pyproject_path) as f:
            for line in f:
                if line.strip().startswith("version"):
                    match = re.search(r'"([^"]+)"', line)
                    if match:
                        return match.group(1)
    except Exception:
        pass
    return "unknown"


BOT_VERSION = _read_version()


def _extract_task_description(task_line: str) -> str:
    """Strip metadata from a raw task line for clean display in the debrief Select menu.

    Input:  '- [ ] Water the tomatoes [Assigned: Ryan] [Due: 2025-06-15] (Created: 2025-06-14 08:30:00)'
    Output: 'Water the tomatoes (Due: Jun 15)'
    """
    text = task_line.strip()
    # Remove checkbox prefix
    text = re.sub(r'^- \[[ x]\] ', '', text)
    # Extract due date before stripping
    due_match = re.search(r'\[Due:\s*(\d{4}-\d{2}-\d{2})\]', text)
    due_suffix = ""
    if due_match:
        try:
            dt = datetime.strptime(due_match.group(1), "%Y-%m-%d")
            due_suffix = f" (Due: {dt.strftime('%b %-d')})"
        except ValueError:
            due_suffix = f" (Due: {due_match.group(1)})"
    # Strip metadata tags
    text = re.sub(r'\s*\[Assigned:\s*[^\]]*\]', '', text)
    text = re.sub(r'\s*\[Due:\s*[^\]]*\]', '', text)
    text = re.sub(r'\s*\(Created:\s*[^)]*\)', '', text)
    return (text.strip() + due_suffix).strip()


def _extract_task_description_numbered(num: int, desc: str) -> str:
    """Format a task description as a numbered line."""
    return f"{num}. {desc}"


CHANNEL_CONTEXT = {
    "journal": "[CONTEXT: User is posting in the JOURNAL channel. Prioritize logging updates and amending knowledge.]\n\n",
    "questions": "[CONTEXT: User is posting in the QUESTIONS channel. You MUST use tools to retrieve info before answering.]\n\n",
    "knowledge_ingest": (
        "[CONTEXT: User is posting content to INGEST into the knowledge library.\n"
        "Your job:\n"
        "1. Identify all gardening/permaculture topics mentioned (plant names, techniques, etc.)\n"
        "2. For EACH topic, use the BROADEST/SIMPLEST topic name — prefer 'garlic' over 'garlic_growing_tips', "
        "'tomato' over 'cherry_tomato_care'. This keeps related info in one file instead of fragmenting.\n"
        "3. Call tool_amend_knowledge with the topic name, relevant facts, AND the 'source' arg.\n"
        "   - Parse the source from the '--- Content from <source> ---' header if present.\n"
        "   - For plain text with no header, use source='Discord message'.\n"
        "   - For images, use source='image'.\n"
        "4. Before amending a topic that already has a file, READ it first with tool_read_file.\n"
        "   If the new info contradicts existing facts, include a conflict note in the content:\n"
        "   > **Conflict:** Previous entry says X, but this source says Y. Verify for your zone.\n"
        "5. Be thorough - extract cultivar info, planting dates, care tips, companion plants, etc.\n"
        "6. When extracting planting dates, use headers like '**Spring Planting Dates:**' and "
        "'**Fall Planting Dates:**' so the planting calendar generator can parse them.\n"
        "7. Reply with a short confirmation (under 500 chars): list the TOPICS updated by name "
        "(e.g. 'Garlic, Tomato, Companion Planting') and a one-line summary. "
        "Do NOT reference filenames or repeat the ingested content.\n"
        "8. On the very last line of your reply, write exactly: TOPICS: topic1, topic2, topic3 "
        "(matching the topic names you passed to tool_amend_knowledge). This line is parsed by the system.]\n\n"
    ),
    "onboarding": (
        "[CONTEXT: This is a NEW USER ONBOARDING session via DM. Guide them through setup step by step.\n"
        "Be warm, conversational, and ask ONE topic at a time. Wait for their response before moving on.\n\n"
        "PHASE 1 — Location & Zone:\n"
        "- Ask for their city/state or general area\n"
        "- Determine their USDA hardiness zone and estimated frost dates from the location\n"
        "- Create 'almanac.md' via tool_overwrite_file with: zone, last/first frost dates, growing season length, and climate notes\n"
        "- Do NOT create almanac.md until the user confirms their location\n\n"
        "PHASE 2 — Garden Layout:\n"
        "- Ask about their garden setup (raised beds, in-ground rows, containers, greenhouse, etc.)\n"
        "- Create 'farm_layout.md' via tool_overwrite_file from their description\n"
        "- Let them know they can draw/sketch their garden layout, label it, and upload the photo — the bot will extract spatial info and update farm_layout.md automatically\n\n"
        "PHASE 3 — Knowledge Building (user-driven, NOT LLM-generated):\n"
        "- Do NOT generate plant information from your own knowledge\n"
        "- Explain the knowledge-ingest channel and what they can upload:\n"
        "  * URLs/articles — paste links to growing guides, extension service pages, seed company info\n"
        "  * PDFs — upload seed catalogs, planting guides, soil reports\n"
        "  * Photos — upload garden photos for plant/pest identification, or layout sketches\n"
        "  * Text — paste notes, tips, or information directly\n"
        "- Explain that the more they feed it, the smarter the daily briefings and advice become\n\n"
        "PHASE 4 — Orient to channels & commands:\n"
        "- Explain each channel: journal (daily updates), questions (Q&A), reminders (briefings/alerts), knowledge-ingest (feed info)\n"
        "- Mention key commands: !briefing, !debrief, !recap, !consolidate\n"
        "- Let them know they can always ask questions or add more info anytime\n"
        "- Welcome them warmly and let them know setup is complete!]\n\n"
    ),
}

class DebriefModal(discord.ui.Modal, title="Daily Garden Debrief"):
    activities = discord.ui.TextInput(
        label="Activities",
        placeholder="What you did today (planting, weeding, watering, etc.)",
        style=discord.TextStyle.paragraph,
        required=False,
    )
    harvests = discord.ui.TextInput(
        label="Harvests",
        placeholder="What you harvested (crop, amount, location)",
        style=discord.TextStyle.paragraph,
        required=False,
    )
    pest_disease = discord.ui.TextInput(
        label="Pest/Disease",
        placeholder="Any pest sightings, disease, or problems",
        style=discord.TextStyle.paragraph,
        required=False,
    )
    observations = discord.ui.TextInput(
        label="Observations",
        placeholder="General notes (weather, soil, growth, etc.)",
        style=discord.TextStyle.paragraph,
        required=False,
    )
    task_updates = discord.ui.TextInput(
        label="Task Updates",
        placeholder="Status of open tasks (completed, deferred, notes)",
        style=discord.TextStyle.paragraph,
        required=False,
    )

    def __init__(self, bot: "BeanBot"):
        super().__init__()
        self.bot = bot

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)

        today = date.today().isoformat()
        sections = []
        if self.activities.value:
            sections.append(f"**Activities:** {self.activities.value}")
        if self.harvests.value:
            sections.append(f"**Harvests:** {self.harvests.value}")
        if self.pest_disease.value:
            sections.append(f"**Pest/Disease:** {self.pest_disease.value}")
        if self.observations.value:
            sections.append(f"**Observations:** {self.observations.value}")
        if self.task_updates.value:
            sections.append(f"**Task Updates:** {self.task_updates.value}")

        if not sections:
            await interaction.followup.send("No data entered — debrief skipped.")
            return

        debrief_text = "\n".join(sections)
        prompt = (
            f"[CONTEXT: This is an EVENING DEBRIEF submission for {today}. "
            f"Process the following structured debrief data. Use your tools to:\n"
            f"1. Log activities to garden_log.md via tool_update_journal\n"
            f"2. Log any harvests to harvests.md via tool_log_harvest\n"
            f"3. Mark completed tasks via tool_complete_task\n"
            f"4. Amend knowledge if pest/disease/observation info is noteworthy\n"
            f"After processing, reply with a short confirmation summary of what was logged.]\n\n"
            f"{debrief_text}"
        )

        response = await self.bot.process_llm_request(
            prompt, "journal", thread_id=f"debrief_{today}"
        )
        await interaction.followup.send(f"**Debrief logged!**\n{response}")


class DebriefView(discord.ui.View):
    def __init__(self, bot: "BeanBot"):
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Log Today's Debrief",
        style=discord.ButtonStyle.primary,
        custom_id="debrief_button",
    )
    async def debrief_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(DebriefModal(self.bot))


class DebriefTaskView(discord.ui.View):
    """Non-persistent debrief view with a task-completion Select menu + debrief button."""

    def __init__(self, bot: "BeanBot", tasks: list[str]):
        super().__init__(timeout=None)
        self.bot = bot
        self.tasks = tasks  # raw task lines from tasks.md

        # Build the Select menu (Discord caps at 25 options)
        options = []
        for i, task_line in enumerate(tasks[:25]):
            label = _extract_task_description(task_line)[:100]  # Discord 100-char limit
            options.append(discord.SelectOption(label=label, value=str(i)))

        select = discord.ui.Select(
            placeholder="Select completed tasks...",
            min_values=0,
            max_values=len(options),
            options=options,
        )
        select.callback = self._select_callback
        self._select = select
        self.add_item(select)

        # "Mark Complete" button
        mark_btn = discord.ui.Button(
            label="Mark Complete",
            style=discord.ButtonStyle.secondary,
        )
        mark_btn.callback = self._mark_complete
        self.add_item(mark_btn)

        # "Log Today's Debrief" button — same custom_id as the persistent DebriefView
        debrief_btn = discord.ui.Button(
            label="Log Today's Debrief",
            style=discord.ButtonStyle.primary,
            custom_id="debrief_button",
        )
        debrief_btn.callback = self._open_debrief_modal
        self.add_item(debrief_btn)

    async def _select_callback(self, interaction: discord.Interaction):
        # Just acknowledge — selections are stored on the Select widget
        await interaction.response.defer()

    async def _mark_complete(self, interaction: discord.Interaction):
        selected = self._select.values
        if not selected:
            await interaction.response.send_message(
                "No tasks selected. Pick tasks from the dropdown first.", ephemeral=True
            )
            return

        await interaction.response.defer(thinking=True)

        completed_names = []
        for idx_str in selected:
            idx = int(idx_str)
            task_line = self.tasks[idx]
            # Extract a snippet for complete_task (description without checkbox prefix)
            snippet = re.sub(r'^- \[[ x]\] ', '', task_line.strip())
            # Use just the first part (before metadata) as the snippet for matching
            snippet_clean = re.sub(r'\s*\[Assigned:.*', '', snippet)
            snippet_clean = re.sub(r'\s*\[Due:.*', '', snippet_clean)
            snippet_clean = re.sub(r'\s*\(Created:.*', '', snippet_clean).strip()
            result = complete_task(snippet_clean)
            completed_names.append(_extract_task_description(task_line))

        # Rebuild the view with remaining open tasks (filtered to today/overdue)
        remaining_tasks = get_open_tasks()
        today = datetime.now(BOT_TZ).date().isoformat()
        remaining_tasks = filter_tasks_due_today_or_overdue(remaining_tasks, today)
        summary = "\n".join(f"- ~~{name}~~" for name in completed_names)

        if remaining_tasks:
            task_display = "\n".join(
                f"- {_extract_task_description(t)}" for t in remaining_tasks
            )
            new_msg = (
                f"**Marked {len(completed_names)} task(s) complete:**\n{summary}\n\n"
                f"**Remaining tasks:**\n{task_display}\n\n"
                f"Click below to log what you did today."
            )
            new_view = DebriefTaskView(self.bot, remaining_tasks)
        else:
            new_msg = (
                f"**Marked {len(completed_names)} task(s) complete:**\n{summary}\n\n"
                f"All tasks done! Click below to log what you did today."
            )
            new_view = DebriefView(self.bot)

        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            content=new_msg,
            view=new_view,
        )

    async def _open_debrief_modal(self, interaction: discord.Interaction):
        await interaction.response.send_modal(DebriefModal(self.bot))


class ConfirmView(discord.ui.View):
    """Reusable non-persistent confirmation view with author-gated buttons."""

    def __init__(self, confirm_label: str, on_confirm, author_id: int, timeout: float = 120):
        super().__init__(timeout=timeout)
        self.on_confirm_callback = on_confirm
        self.author_id = author_id
        self.message: discord.Message | None = None

        confirm_btn = discord.ui.Button(label=confirm_label, style=discord.ButtonStyle.danger)
        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)

        confirm_btn.callback = self._confirm
        cancel_btn.callback = self._cancel

        self.add_item(confirm_btn)
        self.add_item(cancel_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Only the person who initiated this command can confirm.", ephemeral=True
            )
            return False
        return True

    async def _confirm(self, interaction: discord.Interaction):
        await self.on_confirm_callback(interaction)
        self.stop()

    async def _cancel(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="Cancelled. No files were deleted.", view=None)
        self.stop()

    async def on_timeout(self):
        if self.message:
            try:
                await self.message.edit(content="Confirmation timed out. No files were deleted.", view=None)
            except Exception:
                pass


class CrawlLinksView(discord.ui.View):
    """Non-persistent, author-gated view offering to crawl discovered same-domain links."""

    def __init__(self, bot: "BeanBot", links: list[str], original_urls: list[str], author_id: int, channel_id: str):
        super().__init__(timeout=300)  # 5-minute timeout
        self.bot = bot
        self.links = links
        self.original_urls = original_urls
        self.author_id = author_id
        self.channel_id = channel_id
        self.message: discord.Message | None = None

        crawl_btn = discord.ui.Button(
            label=f"Crawl All ({len(links)} links)",
            style=discord.ButtonStyle.primary,
        )
        skip_btn = discord.ui.Button(
            label="Skip",
            style=discord.ButtonStyle.secondary,
        )

        crawl_btn.callback = self._crawl_all
        skip_btn.callback = self._skip

        self.add_item(crawl_btn)
        self.add_item(skip_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Only the person who posted the URL can use these buttons.", ephemeral=True
            )
            return False
        return True

    async def _crawl_all(self, interaction: discord.Interaction):
        total = len(self.links)
        await interaction.response.edit_message(
            content=f"Crawling {total} links...", view=None
        )

        # Fetch all URLs concurrently with a semaphore
        semaphore = asyncio.Semaphore(CRAWL_CONCURRENCY)
        results: dict[str, str | None] = {}

        async def fetch_one(url: str):
            async with semaphore:
                try:
                    content = await self.bot._fetch_url_content(url)
                    return url, content
                except Exception as e:
                    logger.error(f"Crawl fetch failed for {url}: {e}")
                    return url, None

        tasks_list = [fetch_one(url) for url in self.links]
        completed = 0
        for coro in asyncio.as_completed(tasks_list):
            url, content = await coro
            results[url] = content
            completed += 1
            if completed % 5 == 0 or completed == total:
                try:
                    await interaction.message.edit(
                        content=f"Fetching {completed}/{total} URLs..."
                    )
                except Exception:
                    pass

        # Separate successes and failures
        successes = []
        failed_count = 0
        for url in self.links:
            content = results.get(url)
            if content and not content.startswith("[Error") and not content.startswith("[Unsupported"):
                successes.append(f"--- Content from {url} ---\n{content}")
            else:
                failed_count += 1

        if not successes:
            await interaction.message.edit(
                content=f"Could not fetch any of the {total} linked pages."
            )
            return

        combined = "\n\n".join(successes)
        chunks = BeanBot._chunk_text(combined)

        total_chunks = len(chunks)
        responses = []
        for i, chunk in enumerate(chunks, 1):
            response = await self.bot.process_llm_request(
                chunk, "knowledge_ingest", thread_id=self.channel_id
            )
            responses.append(response)
            try:
                await interaction.message.edit(
                    content=f"Processed {i}/{total_chunks} chunks..."
                )
            except Exception:
                pass

        fetched = total - failed_count
        fail_note = f" ({failed_count} failed)" if failed_count else ""
        await interaction.message.edit(
            content=f"Done! Crawled {fetched} linked pages, ingested {total_chunks} chunk(s){fail_note}."
        )

        # Send LLM responses
        summary = "\n".join(responses)
        msg_chunks = BeanBot._chunk_text(summary, max_size=DISCORD_MESSAGE_LIMIT)
        for chunk in msg_chunks:
            await interaction.message.channel.send(chunk)

    async def _skip(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="Skipped crawling linked pages.", view=None
        )
        self.stop()

    async def on_timeout(self):
        if self.message:
            try:
                await self.message.edit(
                    content="Link crawl offer expired (5 min timeout).", view=None
                )
            except Exception:
                pass


class CalendarTaskView(discord.ui.View):
    """Non-persistent, author-gated view offering to update planting calendar and create tasks after ingestion."""

    def __init__(self, bot: "BeanBot", ingested_files: list[str], author_id: int):
        super().__init__(timeout=300)  # 5-minute timeout
        self.bot = bot
        self.ingested_files = ingested_files
        self.author_id = author_id
        self.message: discord.Message | None = None

        # Build Select menu from ingested filenames (Discord caps at 25 options)
        options = []
        for i, filename in enumerate(ingested_files[:25]):
            label = filename.removesuffix(".md").replace("_", " ").title()[:100]
            options.append(discord.SelectOption(label=label, value=str(i), default=True))

        select = discord.ui.Select(
            placeholder="Select topics to include...",
            min_values=1,
            max_values=len(options),
            options=options,
        )
        select.callback = self._select_callback
        self._select = select
        self.add_item(select)

        update_btn = discord.ui.Button(
            label="Update Calendar & Create Tasks",
            style=discord.ButtonStyle.success,
        )
        skip_btn = discord.ui.Button(
            label="Skip",
            style=discord.ButtonStyle.secondary,
        )

        update_btn.callback = self._update_calendar_tasks
        skip_btn.callback = self._skip

        self.add_item(update_btn)
        self.add_item(skip_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Only the person who posted the content can use these buttons.", ephemeral=True
            )
            return False
        return True

    async def _select_callback(self, interaction: discord.Interaction):
        # Just acknowledge — selections are stored on the Select widget
        await interaction.response.defer()

    async def _update_calendar_tasks(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="Updating planting calendar and creating tasks...", view=None
        )

        # .values is only populated after user interacts with the Select;
        # if they just click the button, default to all files (visually pre-selected).
        if self._select.values:
            selected_indices = [int(v) for v in self._select.values]
            selected_files = [self.ingested_files[i] for i in selected_indices]
        else:
            selected_files = self.ingested_files[:25]
        files_str = ", ".join(selected_files)
        today = date.today().isoformat()
        prompt = (
            "[CONTEXT: POST-INGESTION CALENDAR & TASK UPDATE.]\n\n"
            f"The following knowledge topics were just ingested: {files_str}\n\n"
            "Your job:\n"
            "1. Read these files using tool_read_multiple_files to review the ingested content.\n"
            "2. Read 'almanac.md' for the user's zone, frost dates, and growing season.\n"
            "3. Read 'farm_layout.md' for available beds/zones and current plantings.\n"
            "4. Read 'tasks.md' to check for existing tasks and avoid duplicates.\n"
            "5. Search for relevant technique/method files (e.g. seed starting, companion planting, "
            "soil amendment) using tool_find_related_files and note useful cross-references.\n"
            "6. Call tool_generate_calendar to rebuild the planting calendar with the new data.\n"
            "7. For each plant with actionable planting dates in the current or upcoming season:\n"
            "   - Create tasks via tool_add_task with specific due dates.\n"
            "   - Reference bed locations from farm_layout.md when possible.\n"
            "   - Include relevant technique tips in the task description.\n"
            "   - Do NOT create tasks for dates that have already passed (use tool_get_date).\n"
            "8. Reply with a summary: which plants were added to the calendar, what tasks were "
            "created, and any relevant technique cross-references. Do NOT reference filenames — "
            "refer to things by topic name (e.g. 'the planting calendar' not 'planting_calendar.md')."
        )

        ephemeral_thread_id = f"calendar_task_{today}"
        response = await self.bot.process_llm_request(
            prompt, "questions", thread_id=ephemeral_thread_id
        )

        chunks = BeanBot._chunk_text(response, max_size=DISCORD_MESSAGE_LIMIT)
        for chunk in chunks:
            await interaction.message.channel.send(chunk)

    async def _skip(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="Skipped calendar and task update.", view=None
        )
        self.stop()

    async def on_timeout(self):
        if self.message:
            try:
                await self.message.edit(
                    content="Calendar/task offer expired (5 min timeout).", view=None
                )
            except Exception:
                pass


class TaskConsolidateView(discord.ui.View):
    """Non-persistent, author-gated view for stepping through duplicate task groups."""

    def __init__(self, bot: "BeanBot", groups: list[dict], all_tasks: list[str], completed_tasks: list[str], author_id: int):
        super().__init__(timeout=300)  # 5-minute timeout
        self.bot = bot
        self.groups = groups
        self.all_tasks = all_tasks  # open tasks
        self.completed_tasks = completed_tasks
        self.author_id = author_id
        self.current_group = 0
        self.decisions: dict[int, str] = {}  # group_index -> "merge"|"keep"|"remove_dups"
        self.message: discord.Message | None = None

        merge_btn = discord.ui.Button(label="Merge", style=discord.ButtonStyle.success)
        keep_btn = discord.ui.Button(label="Keep All", style=discord.ButtonStyle.secondary)
        remove_btn = discord.ui.Button(label="Remove Duplicates", style=discord.ButtonStyle.danger)

        merge_btn.callback = self._merge
        keep_btn.callback = self._keep
        remove_btn.callback = self._remove_dups

        self.add_item(merge_btn)
        self.add_item(keep_btn)
        self.add_item(remove_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Only the person who ran this command can use these buttons.", ephemeral=True
            )
            return False
        return True

    def _build_embed(self) -> discord.Embed:
        """Build an embed for the current group."""
        group = self.groups[self.current_group]
        group_type = group.get("type", "similar").title()
        embed = discord.Embed(
            title=f"Group {self.current_group + 1}/{len(self.groups)} — {group_type}",
            color=discord.Color.orange() if group["type"] == "similar" else discord.Color.red(),
        )
        # Show the tasks in this group
        task_lines = []
        for idx in group["indices"]:
            task_lines.append(f"`{idx+1}.` {_extract_task_description(self.all_tasks[idx])}")
        embed.add_field(name="Tasks", value="\n".join(task_lines), inline=False)
        embed.add_field(name="Why grouped", value=group.get("reason", "N/A"), inline=False)
        if group.get("suggested_merge"):
            clean_merge = re.sub(r'^- \[[ x]\] ', '', group["suggested_merge"])
            embed.add_field(name="Suggested merge", value=clean_merge, inline=False)
        embed.set_footer(text="Merge = combine into one task | Keep All = no change | Remove Duplicates = keep first, delete rest")
        return embed

    async def _advance(self, interaction: discord.Interaction, decision: str):
        """Record decision and advance to next group or apply all."""
        self.decisions[self.current_group] = decision
        self.current_group += 1

        if self.current_group < len(self.groups):
            embed = self._build_embed()
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.edit_message(
                content="Applying changes...", embed=None, view=None
            )
            summary = await self._apply_decisions()
            await interaction.message.edit(content=summary)

    async def _merge(self, interaction: discord.Interaction):
        await self._advance(interaction, "merge")

    async def _keep(self, interaction: discord.Interaction):
        await self._advance(interaction, "keep")

    async def _remove_dups(self, interaction: discord.Interaction):
        await self._advance(interaction, "remove_dups")

    async def _apply_decisions(self) -> str:
        """Apply all decisions: backup, rewrite tasks.md, return summary."""
        backup_file("tasks.md")

        indices_to_remove: set[int] = set()
        new_merged_lines: list[str] = []
        merge_count = 0
        remove_count = 0

        for group_idx, decision in self.decisions.items():
            group = self.groups[group_idx]
            group_indices = group["indices"]

            if decision == "keep":
                continue

            elif decision == "merge":
                # Remove all tasks in the group
                indices_to_remove.update(group_indices)
                # Parse the suggested merge into task line(s)
                suggested = group.get("suggested_merge", "").strip()
                if not suggested:
                    # Fallback: keep the first task
                    indices_to_remove.discard(group_indices[0])
                    continue
                # Collect assignees from original tasks
                assignees: set[str] = set()
                earliest_due = None
                for idx in group_indices:
                    task = self.all_tasks[idx]
                    assigned_match = re.search(r'\[Assigned:\s*([^\]]+)\]', task)
                    if assigned_match:
                        assignees.add(assigned_match.group(1).strip())
                    due_match = re.search(r'\[Due:\s*(\d{4}-\d{2}-\d{2})\]', task)
                    if due_match:
                        d = due_match.group(1)
                        if earliest_due is None or d < earliest_due:
                            earliest_due = d

                # Strip any existing metadata from suggested merge
                clean_suggested = re.sub(r'\s*\[Assigned:\s*[^\]]*\]', '', suggested)
                clean_suggested = re.sub(r'\s*\[Due:\s*[^\]]*\]', '', clean_suggested)
                clean_suggested = re.sub(r'\s*\(Created:\s*[^)]*\)', '', clean_suggested)
                clean_suggested = re.sub(r'^- \[[ x]\] ', '', clean_suggested).strip()

                due_str = f" [Due: {earliest_due}]" if earliest_due else ""
                now_str = datetime.now(BOT_TZ).strftime("%Y-%m-%d %H:%M:%S")

                if len(assignees) > 1:
                    # Multiple assignees: create one line per assignee
                    for assignee in sorted(assignees):
                        new_merged_lines.append(
                            f"- [ ] {clean_suggested} [Assigned: {assignee}]{due_str} (Created: {now_str})"
                        )
                elif len(assignees) == 1:
                    assignee = next(iter(assignees))
                    new_merged_lines.append(
                        f"- [ ] {clean_suggested} [Assigned: {assignee}]{due_str} (Created: {now_str})"
                    )
                else:
                    new_merged_lines.append(
                        f"- [ ] {clean_suggested}{due_str} (Created: {now_str})"
                    )
                merge_count += 1

            elif decision == "remove_dups":
                # Keep first (lowest index), remove rest
                sorted_indices = sorted(group_indices)
                indices_to_remove.update(sorted_indices[1:])
                remove_count += len(sorted_indices) - 1

        # Reconstruct tasks.md
        surviving_open = []
        for i, task in enumerate(self.all_tasks):
            if i not in indices_to_remove:
                surviving_open.append(task)

        lines = ["# Task List\n"]
        for task in surviving_open:
            lines.append(task)
        for task in new_merged_lines:
            lines.append(task)
        for task in self.completed_tasks:
            lines.append(task)

        content = "\n".join(lines) + "\n"
        overwrite_knowledge_file("tasks.md", content)

        parts = []
        if merge_count:
            parts.append(f"{merge_count} group(s) merged")
        if remove_count:
            parts.append(f"{remove_count} duplicate(s) removed")
        kept = len(self.decisions) - merge_count - (1 if remove_count else 0)
        # Count groups where decision was "keep"
        kept_count = sum(1 for d in self.decisions.values() if d == "keep")
        if kept_count:
            parts.append(f"{kept_count} group(s) kept as-is")

        summary = f"**Task consolidation complete.** " + ", ".join(parts) + ". Backup saved to `data/backups/`."
        return summary

    async def on_timeout(self):
        if self.message:
            try:
                await self.message.edit(
                    content="Task consolidation timed out (5 min). No changes were made.",
                    embed=None,
                    view=None,
                )
            except Exception:
                pass


class BeanBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)
        
        # Channel IDs
        self.reminders_channel_id = int(os.getenv("REMINDERS_CHANNEL_ID", "0"))
        self.journal_channel_id = int(os.getenv("JOURNAL_CHANNEL_ID", "0"))
        self.questions_channel_id = int(os.getenv("QUESTIONS_CHANNEL_ID", "0"))
        self.knowledge_ingest_channel_id = int(os.getenv("KNOWLEDGE_INGEST_CHANNEL_ID", "0"))
        
        self.weather_api_key = os.getenv("OPENWEATHER_API_KEY")
        self.weather_lat = os.getenv("WEATHER_LAT")
        self.weather_lon = os.getenv("WEATHER_LON")
        self.weather_units = os.getenv("WEATHER_UNITS", "metric")
        
        # Register commands
        @self.command(name="briefing")
        async def briefing_cmd(ctx):
             await self.briefing(ctx)

        @self.command(name="debrief")
        async def debrief_cmd(ctx):
            await self.debrief(ctx)

        @self.command(name="consolidate")
        async def consolidate_cmd(ctx, *, topic: str = ""):
            await self.consolidate(ctx, topic.strip())

        @self.command(name="recap")
        async def recap_cmd(ctx, days: int = 7):
            await self.recap(ctx, days)

        @self.command(name="setup")
        async def setup_cmd(ctx):
            await self.setup_onboarding(ctx)

        @self.command(name="register")
        async def register_cmd(ctx, name: str = "", member: discord.Member = None):
            await self.register(ctx, name, member)

        @self.command(name="members")
        async def members_cmd(ctx):
            await self.show_members(ctx)

        @self.command(name="commands")
        async def commands_cmd(ctx):
            await self.show_commands(ctx)

        @self.command(name="version")
        async def version_cmd(ctx):
            await ctx.send(f"Beanbot v{BOT_VERSION}")

        @self.command(name="tasks")
        async def tasks_cmd(ctx):
            await self.show_tasks(ctx)

        @self.command(name="clear")
        async def clear_cmd(ctx, *, topic: str = ""):
            await self.clear(ctx, topic.strip())

    async def setup_hook(self):
        await init_graph()
        logger.info("Conversation memory initialized.")
        self.add_view(DebriefView(self))
        self.daily_report.start()
        self.daily_debrief.start()
        self.weather_alerts.start()
        self.weekly_recap.start()
        self.db_prune.start()
        logger.info("BeanBot setup complete. All schedulers started.")

    async def on_ready(self):
        logger.info(f"Logged in as {self.user} (ID: {self.user.id})")

    async def register(self, ctx, name: str, member: discord.Member = None):
        """Register a user as a named garden member."""
        if not name:
            await ctx.send("Usage: `!register <name>` or `!register <name> @user`")
            return
        target = member or ctx.author
        result = register_member(name, target.id)
        await ctx.send(f"Registered **{name.title()}** as <@{target.id}>.")

    async def show_members(self, ctx):
        """List all registered garden members."""
        members = list_members()
        if not members:
            await ctx.send("No members registered. Use `!register <name>` to add yourself.")
            return
        lines = [f"- **{name.title()}**: <@{did}>" for name, did in members.items()]
        await ctx.send("**Registered Members:**\n" + "\n".join(lines))

    async def show_tasks(self, ctx):
        """Show all open tasks grouped by assignee."""
        open_tasks = get_open_tasks()
        if not open_tasks:
            await ctx.send("No open tasks.")
            return

        assigned_re = re.compile(r'\[Assigned:\s*([^\]]+)\]', re.IGNORECASE)
        groups: dict[str, list[str]] = {}  # assignee -> task descriptions

        for task in open_tasks:
            match = assigned_re.search(task)
            key = match.group(1).strip().title() if match else "Unassigned"
            groups.setdefault(key, []).append(_extract_task_description(task))

        # Build output: unassigned first, then alphabetical by name
        sections = []
        if "Unassigned" in groups:
            lines = [f"  {_extract_task_description_numbered(i, t)}" for i, t in enumerate(groups["Unassigned"], 1)]
            sections.append(f"**Unassigned** ({len(groups['Unassigned'])}):\n" + "\n".join(lines))

        for name in sorted(k for k in groups if k != "Unassigned"):
            tasks = groups[name]
            lines = [f"  {_extract_task_description_numbered(i, t)}" for i, t in enumerate(tasks, 1)]
            sections.append(f"**{name}** ({len(tasks)}):\n" + "\n".join(lines))

        total = len(open_tasks)
        header = f"**Open Tasks ({total})**\n"
        body = header + "\n\n".join(sections)
        await self._send_long_reply(ctx.message, body)

    async def show_commands(self, ctx):
        """Show all available commands with brief usage."""
        text = (
            "**Beanbot Commands**\n\n"
            "`!briefing` — Trigger the morning briefing (weather, tasks, planting advice)\n"
            "`!debrief` — Start the evening debrief (shows your tasks, opens logging form)\n"
            "`!recap [days]` — Summarize the last N days of garden activity (default 7)\n"
            "`!consolidate` — Categorize all knowledge files and find merge candidates\n"
            "`!consolidate <topic>` — Merge all files about a topic into one clean file\n"
            "`!consolidate tasks` — Deduplicate and clean up the task list interactively\n"
            "`!tasks` — Show all open tasks grouped by assignee\n"
            "`!register <name>` — Register yourself as a garden member\n"
            "`!register <name> @user` — Register someone else as a garden member\n"
            "`!members` — List all registered garden members\n"
            "`!setup` — Run first-time onboarding (location, garden layout, orientation)\n"
            "`!clear <topic>` — Delete a single knowledge file (2-step confirmation)\n"
            "`!clear knowledge` — Delete all knowledge files, keep system files (3-step confirmation)\n"
            "`!clear garden` — Factory reset: delete everything in data/ (3-step confirmation)\n"
            "`!version` — Show the current Beanbot version\n"
            "`!commands` — Show this help message"
        )
        await ctx.send(text)

    async def clear(self, ctx, topic: str):
        """Route !clear to the appropriate sub-method."""
        allowed = [self.reminders_channel_id, self.journal_channel_id, self.questions_channel_id]
        if ctx.channel.id not in allowed:
            await ctx.send("This command can only be used in the reminders, journal, or questions channel.")
            return

        if not topic:
            await ctx.send(
                "**Usage:**\n"
                "`!clear <topic>` — Delete a single knowledge file\n"
                "`!clear knowledge` — Delete all knowledge files (keeps system files)\n"
                "`!clear garden` — Factory reset (deletes everything in data/)"
            )
            return

        if topic.lower() == "garden":
            await self._clear_garden(ctx)
        elif topic.lower() == "knowledge":
            await self._clear_knowledge(ctx)
        else:
            await self._clear_topic(ctx, topic)

    async def _clear_topic(self, ctx, topic: str):
        """Delete a single knowledge file with 2-step confirmation."""
        safe = _sanitize_topic(topic)
        filename = f"{safe}.md"

        if filename in SYSTEM_FILES:
            await ctx.send(f"`{filename}` is a protected system file and cannot be deleted.")
            return

        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            await ctx.send(f"File `{filename}` not found.")
            return

        size = os.path.getsize(filepath)
        size_str = self._format_size(size)

        # Step 1
        async def step2(interaction: discord.Interaction):
            view2 = ConfirmView(f"Delete {filename}", final_confirm, ctx.author.id)
            await interaction.response.edit_message(
                content=f"**Are you sure?** This will permanently delete `{filename}` ({size_str}). This cannot be undone.",
                view=view2,
            )
            view2.message = interaction.message

        # Step 2 (final)
        async def final_confirm(interaction: discord.Interaction):
            result = delete_knowledge_file(filename)
            await interaction.response.edit_message(content=f"Deleted `{filename}`.", view=None)

        view1 = ConfirmView("Continue", step2, ctx.author.id)
        msg = await ctx.send(
            f"**Clear file:** `{filename}` ({size_str})\n\nThis will permanently delete this knowledge file.",
            view=view1,
        )
        view1.message = msg

    async def _clear_knowledge(self, ctx):
        """Delete all non-system knowledge files with 3-step confirmation."""
        files = get_clearable_knowledge_files()
        if not files:
            await ctx.send("No knowledge files to delete.")
            return

        count = len(files)
        file_list = ", ".join(f"`{f}`" for f in sorted(files)[:20])
        if count > 20:
            file_list += f" ... and {count - 20} more"

        # Step 1
        async def step2(interaction: discord.Interaction):
            view2 = ConfirmView("Continue", step3, ctx.author.id)
            await interaction.response.edit_message(
                content=(
                    f"**Second confirmation.** You are about to delete **{count}** knowledge files.\n\n"
                    f"System files (tasks, harvests, almanac, etc.) will NOT be touched.\n"
                    f"This cannot be undone."
                ),
                view=view2,
            )
            view2.message = interaction.message

        # Step 2
        async def step3(interaction: discord.Interaction):
            view3 = ConfirmView(f"Delete all {count} files", final_confirm, ctx.author.id)
            await interaction.response.edit_message(
                content=f"**Final confirmation.** Click below to permanently delete all {count} knowledge files.",
                view=view3,
            )
            view3.message = interaction.message

        # Step 3 (final)
        async def final_confirm(interaction: discord.Interaction):
            deleted, errors = clear_all_knowledge_files()
            summary = f"Deleted **{len(deleted)}** knowledge file(s)."
            if errors:
                summary += f"\n\nErrors ({len(errors)}):\n" + "\n".join(f"- {e}" for e in errors)
            await interaction.response.edit_message(content=summary, view=None)

        view1 = ConfirmView("I understand, continue", step2, ctx.author.id)
        msg = await ctx.send(
            f"**Clear knowledge library** — this will delete **{count}** file(s):\n{file_list}\n\n"
            f"System files will be preserved.",
            view=view1,
        )
        view1.message = msg

    async def _clear_garden(self, ctx):
        """Factory reset with 3-step confirmation."""
        # Inventory what will be deleted
        knowledge_files = get_clearable_knowledge_files()
        system_files = [f for f in os.listdir(DATA_DIR) if f in SYSTEM_FILES and os.path.isfile(os.path.join(DATA_DIR, f))]
        other_files = [
            f for f in os.listdir(DATA_DIR)
            if os.path.isfile(os.path.join(DATA_DIR, f)) and f not in SYSTEM_FILES and f not in knowledge_files
        ]
        dirs = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]

        # Step 1
        async def step2(interaction: discord.Interaction):
            view2 = ConfirmView("Continue", step3, ctx.author.id)
            await interaction.response.edit_message(
                content=(
                    "**This will delete EVERYTHING in data/:**\n"
                    "- All knowledge files\n"
                    "- System files (tasks, harvests, almanac, garden log, etc.)\n"
                    "- Conversation memory (conversations.db)\n"
                    "- Member registry (members.json)\n"
                    "- All backups\n"
                    "- Weather alert flags\n\n"
                    "**Nothing will be recoverable.**"
                ),
                view=view2,
            )
            view2.message = interaction.message

        # Step 2
        async def step3(interaction: discord.Interaction):
            view3 = ConfirmView("Reset everything", final_confirm, ctx.author.id)
            await interaction.response.edit_message(
                content="**POINT OF NO RETURN.** Click below to factory-reset your entire garden.",
                view=view3,
            )
            view3.message = interaction.message

        # Step 3 (final)
        async def final_confirm(interaction: discord.Interaction):
            result = clear_entire_garden()
            n_files = len(result["deleted_files"])
            n_dirs = len(result["deleted_dirs"])
            summary = f"Factory reset complete. Deleted **{n_files}** file(s) and **{n_dirs}** directory/directories."
            if result["errors"]:
                summary += f"\n\nErrors ({len(result['errors'])}):\n" + "\n".join(f"- {e}" for e in result["errors"])
            await interaction.response.edit_message(content=summary, view=None)

        inventory_lines = []
        if knowledge_files:
            inventory_lines.append(f"- **{len(knowledge_files)}** knowledge files")
        if system_files:
            inventory_lines.append(f"- **{len(system_files)}** system files ({', '.join(system_files)})")
        if other_files:
            inventory_lines.append(f"- **{len(other_files)}** other files ({', '.join(other_files[:5])}{'...' if len(other_files) > 5 else ''})")
        if dirs:
            inventory_lines.append(f"- **{len(dirs)}** directories ({', '.join(dirs)})")

        inventory = "\n".join(inventory_lines) if inventory_lines else "- (data/ is empty)"

        view1 = ConfirmView("I understand", step2, ctx.author.id)
        msg = await ctx.send(
            f"**Factory reset** — this will delete everything in `data/`:\n{inventory}\n\n"
            f"This is irreversible. All knowledge, tasks, logs, conversations, and backups will be gone.",
            view=view1,
        )
        view1.message = msg

    def _inject_mentions(self, text: str) -> str:
        """Replace registered member names with Discord @mentions in text."""
        members = list_members()
        for name, did in members.items():
            # Case-insensitive word boundary match on the display name
            pattern = re.compile(r'\b' + re.escape(name) + r'\b', re.IGNORECASE)
            text = pattern.sub(f"<@{did}>", text)
        return text

    def _extract_urls(self, text: str) -> list[str]:
        """Extract URLs from message text."""
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        return re.findall(url_pattern, text)

    @staticmethod
    def _extract_ingested_topics(text: str) -> tuple[list[str], str]:
        """Extract topic names from TOPICS: footer(s), return (filenames, cleaned_text).

        Handles multiple TOPICS: lines (from multi-chunk ingestion).

        Returns:
            (deduplicated list of derived filenames, response text with TOPICS: lines stripped)
        """
        lines = text.rstrip().split("\n")
        all_topics = []
        cleaned_lines = []
        for line in lines:
            if line.strip().startswith("TOPICS:"):
                raw = line.strip()[len("TOPICS:"):].strip()
                all_topics.extend(t.strip() for t in raw.split(",") if t.strip())
            else:
                cleaned_lines.append(line)
        cleaned = "\n".join(cleaned_lines).rstrip()
        if not all_topics:
            return [], cleaned
        # Same logic as _sanitize_topic() in tools.py
        filenames = []
        for t in all_topics:
            stem = "".join(c for c in t if c.isalnum() or c in (' ', '_', '-')).strip().lower().replace(' ', '_')
            if stem:
                filenames.append(f"{stem}.md")
        return list(dict.fromkeys(filenames)), cleaned

    @staticmethod
    def _extract_same_domain_links(soup: BeautifulSoup, base_url: str) -> list[str]:
        """Extract deduplicated same-domain links from a parsed HTML page."""
        base_parsed = urlparse(base_url)
        base_clean = base_parsed._replace(fragment='').geturl()
        seen = set()
        links = []

        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if not href or href.startswith(('#', 'mailto:', 'javascript:', 'tel:')):
                continue

            resolved = urljoin(base_url, href)
            parsed = urlparse(resolved)

            # Same domain only
            if parsed.netloc != base_parsed.netloc:
                continue

            # Strip fragment, normalize
            clean = parsed._replace(fragment='').geturl()
            if clean == base_clean or clean in seen:
                continue

            seen.add(clean)
            links.append(clean)

        return links

    async def _fetch_url_content(self, url: str, extract_links: bool = False) -> str | tuple[str, list[str]]:
        """Fetch and extract text from URL. When extract_links=True, returns (text, links)."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(timeout=URL_FETCH_TIMEOUT, headers=headers) as client:
            try:
                resp = await client.get(url, follow_redirects=True)
                resp.raise_for_status()
                content_type = resp.headers.get('content-type', '')

                if 'text/html' in content_type:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    # Extract links BEFORE decomposing nav/footer
                    links = self._extract_same_domain_links(soup, str(resp.url)) if extract_links else []
                    # Remove scripts, styles, nav, footer
                    for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
                        tag.decompose()
                    text = soup.get_text(separator='\n', strip=True)
                    return (text, links) if extract_links else text
                elif 'text/' in content_type or url.endswith(('.md', '.txt')):
                    return (resp.text, []) if extract_links else resp.text
                else:
                    msg = f"[Unsupported content type: {content_type}]"
                    return (msg, []) if extract_links else msg
            except Exception as e:
                logger.error(f"Failed to fetch {url}: {e}")
                msg = f"[Error fetching URL: {e}]"
                return (msg, []) if extract_links else msg

    @staticmethod
    def _extract_pdf_text(pdf_bytes: bytes, filename: str) -> str:
        """Extract text content from PDF bytes."""
        import pymupdf
        try:
            doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
            pages = [page.get_text() for page in doc]
            doc.close()
            return "\n\n".join(pages)
        except Exception as e:
            logger.error(f"Failed to extract text from PDF {filename}: {e}")
            return ""

    @staticmethod
    def _chunk_text(text: str, max_size: int = INGESTION_CHUNK_SIZE) -> list[str]:
        """Split text into chunks at natural boundaries."""
        if len(text) <= max_size:
            return [text]

        chunks = []
        while text:
            if len(text) <= max_size:
                chunks.append(text)
                break

            # Try to split at double newline
            split_at = text.rfind('\n\n', 0, max_size)
            if split_at < max_size // 2:
                # Try single newline
                split_at = text.rfind('\n', 0, max_size)
            if split_at < max_size // 2:
                # Try space
                split_at = text.rfind(' ', 0, max_size)
            if split_at < max_size // 2:
                # Hard cut
                split_at = max_size

            chunks.append(text[:split_at])
            text = text[split_at:].lstrip('\n')

        return chunks

    async def _send_long_reply(self, message: discord.Message, response: str):
        """Send a reply, splitting into multiple messages if over Discord's 2000 char limit."""
        chunks = self._chunk_text(response, max_size=DISCORD_MESSAGE_LIMIT)
        await message.reply(chunks[0])
        for chunk in chunks[1:]:
            await message.channel.send(chunk)

    async def on_message(self, message):
        if message.author == self.user:
            return

        # Check if message is in one of our designated channels
        is_journal = message.channel.id == self.journal_channel_id
        is_question = message.channel.id == self.questions_channel_id
        is_ingest = message.channel.id == self.knowledge_ingest_channel_id
        is_dm = isinstance(message.channel, discord.DMChannel)
        is_mention = self.user in message.mentions

        # Handle knowledge ingestion channel
        if is_ingest:
            ctx = await self.get_context(message)
            if ctx.valid:
                await self.process_commands(message)
                return
            ingested_files = []
            async with message.channel.typing():
                # Extract image attachments separately
                image_data = await self._extract_images(message)
                non_image_attachments = [
                    att for att in message.attachments
                    if not (att.content_type and att.content_type.startswith("image/"))
                ]

                # Extract PDF attachments
                pdf_texts = []
                other_attachments = []
                for att in non_image_attachments:
                    if (att.content_type and "pdf" in att.content_type) or att.filename.lower().endswith(".pdf"):
                        pdf_bytes = await att.read()
                        text = self._extract_pdf_text(pdf_bytes, att.filename)
                        if text:
                            pdf_texts.append(f"--- Content from {att.filename} ---\n{text}")
                    else:
                        other_attachments.append(att)

                # Collect URLs from message text
                urls = self._extract_urls(message.content)
                # Add non-image, non-PDF attachment URLs
                urls.extend([att.url for att in other_attachments])

                all_discovered_links = []
                fetched_urls = set()

                if urls:
                    # Fetch URL content and discover same-domain links
                    contents = []
                    for url in urls[:MAX_INGEST_URLS]:
                        content, links = await self._fetch_url_content(url, extract_links=True)
                        contents.append(f"--- Content from {url} ---\n{content}")
                        all_discovered_links.extend(links)
                        fetched_urls.add(url)
                    combined = "\n\n".join(pdf_texts + contents)
                elif message.content.strip() or image_data or pdf_texts:
                    # Plain text message, images, and/or PDF content
                    parts = pdf_texts + [message.content or ""]
                    combined = "\n\n".join(parts).strip()
                else:
                    await message.reply("Send a URL, upload a file, or type knowledge to ingest.")
                    return

                chunks = self._chunk_text(combined) if combined.strip() else [combined]

                if len(chunks) == 1:
                    response = await self.process_llm_request(chunks[0], "knowledge_ingest", thread_id=str(message.channel.id), images=image_data)
                    ingested_files, response = self._extract_ingested_topics(response)
                    await self._send_long_reply(message, response)
                else:
                    total = len(chunks)
                    progress_msg = await message.reply(f"Ingesting large content in {total} parts...")
                    responses = []
                    for i, chunk in enumerate(chunks, 1):
                        # Only pass images with the first chunk
                        chunk_images = image_data if i == 1 else []
                        response = await self.process_llm_request(chunk, "knowledge_ingest", thread_id=str(message.channel.id), images=chunk_images)
                        responses.append(response)
                        await progress_msg.edit(content=f"Processed {i}/{total} parts...")
                    await progress_msg.edit(content=f"Done! Ingested {total} parts.")
                    summary = "\n".join(responses)
                    ingested_files, summary = self._extract_ingested_topics(summary)
                    await self._send_long_reply(message, summary)

            # Offer to update calendar & create tasks from ingested content
            if ingested_files:
                calendar_view = CalendarTaskView(
                    bot=self,
                    ingested_files=ingested_files,
                    author_id=message.author.id,
                )
                cal_msg = await message.channel.send(
                    "Would you like me to update the planting calendar and create tasks "
                    "from the ingested content?",
                    view=calendar_view,
                )
                calendar_view.message = cal_msg

            # Offer to crawl discovered links (outside typing block)
            if all_discovered_links:
                # Deduplicate and exclude already-fetched URLs
                seen = set()
                unique_links = []
                for link in all_discovered_links:
                    if link not in seen and link not in fetched_urls:
                        seen.add(link)
                        unique_links.append(link)

                unique_links = unique_links[:MAX_CRAWL_LINKS]

                if unique_links:
                    # Build the link preview (show first 5, summarize rest)
                    preview_lines = [f"  - {link}" for link in unique_links[:5]]
                    preview = "\n".join(preview_lines)
                    if len(unique_links) > 5:
                        preview += f"\n  ... and {len(unique_links) - 5} more"

                    crawl_view = CrawlLinksView(
                        bot=self,
                        links=unique_links,
                        original_urls=list(fetched_urls),
                        author_id=message.author.id,
                        channel_id=str(message.channel.id),
                    )
                    crawl_msg = await message.channel.send(
                        f"Found **{len(unique_links)}** same-domain links on that page:\n{preview}\n\n"
                        f"Would you like me to crawl and ingest them too?",
                        view=crawl_view,
                    )
                    crawl_view.message = crawl_msg

            return

        if is_journal or is_question or is_dm or is_mention:
            # Process as conversational LLM request ONLY if it is not a command
            ctx = await self.get_context(message)
            if not ctx.valid:
                async with message.channel.typing():
                    content = message.content.replace(f"<@{self.user.id}>", "").strip()

                    # Inject user identity if registered
                    user_name = get_member_name_by_discord_id(message.author.id)
                    if user_name:
                        content = f"[User: {user_name.title()}] {content}"

                    # Extract image attachments
                    image_data = await self._extract_images(message)

                    # Determine "context" based on channel
                    channel_type = "dm"
                    if is_journal: channel_type = "journal"
                    elif is_question: channel_type = "questions"
                    elif is_dm and not is_onboarding_complete():
                        channel_type = "onboarding"

                    response = await self.process_llm_request(content, channel_type, thread_id=str(message.channel.id), images=image_data)
                    await self._send_long_reply(message, response)
        
        await self.process_commands(message)

    async def briefing(self, ctx):
        """Manually trigger the daily briefing."""
        if ctx.channel.id not in [self.reminders_channel_id, self.journal_channel_id]:
            await ctx.send("This command can only be used in the reminders or journal channel.")
            return

        await ctx.send("Triggering daily briefing manually...")
        await self.run_daily_report_logic(ctx.channel, manual=True)


    def _extract_text(self, content) -> str:
        """Extract plain text from message content (handles Gemini's list format)."""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            # Gemini returns [{'type': 'text', 'text': '...', ...}]
            texts = [block.get('text', '') for block in content if isinstance(block, dict) and block.get('type') == 'text']
            return ''.join(texts)
        return str(content)

    async def _extract_images(self, message: discord.Message) -> list[dict]:
        """Extract image attachments from a Discord message as bytes with metadata."""
        image_data = []
        for att in message.attachments:
            if att.content_type and att.content_type.startswith("image/"):
                try:
                    img_bytes = await att.read()
                    image_data.append({
                        "bytes": img_bytes,
                        "mime_type": att.content_type,
                        "filename": att.filename,
                    })
                except Exception as e:
                    logger.error(f"Failed to read image attachment {att.filename}: {e}")
        return image_data

    async def process_llm_request(self, user_input: str, channel_type: str, thread_id: str = "", images: list[dict] | None = None) -> str:
        """
        Passes the user input into the LangGraph workflow with channel-specific context.
        Uses thread_id for conversation memory via the SqliteSaver checkpointer.
        """
        # Fold channel-specific guidance into the HumanMessage (not a separate
        # SystemMessage) to prevent old guidance from accumulating in checkpoint history.
        guidance = CHANNEL_CONTEXT.get(channel_type, "")

        if images:
            content_parts = [{"type": "text", "text": guidance + user_input}]
            for img in images:
                b64 = base64.b64encode(img["bytes"]).decode("utf-8")
                content_parts.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:{img['mime_type']};base64,{b64}"},
                })
            inputs = {"messages": [HumanMessage(content=content_parts)]}
        else:
            inputs = {"messages": [HumanMessage(content=guidance + user_input)]}

        config = {"configurable": {"thread_id": thread_id}} if thread_id else {}

        try:
            result = await asyncio.wait_for(
                graph_module.app_graph.ainvoke(inputs, config=config),
                timeout=LLM_TIMEOUT,
            )
            final_message = result["messages"][-1]
            return self._extract_text(final_message.content)
        except asyncio.TimeoutError:
            logger.error(f"LangGraph execution timed out after {LLM_TIMEOUT} seconds")
            return "Sorry, that took too long — I wasn't able to get a response in time. Please try again in a moment."
        except Exception as e:
            logger.error(f"Error in LangGraph execution: {e}", exc_info=True)
            err_str = str(e).lower()
            if "resource" in err_str and "exhausted" in err_str or "429" in str(e) or "quota" in err_str:
                return "I've hit my API usage limit — please wait a few minutes before sending another message. I'll be back shortly!"
            return "I encountered an error processing your request."

    @tasks.loop(time=BRIEFING_TIME)
    async def daily_report(self):
        if self.reminders_channel_id == 0:
            return

        channel = self.get_channel(self.reminders_channel_id)
        if not channel:
            logger.warning(f"Reminders channel {self.reminders_channel_id} not found.")
            return

        await self.run_daily_report_logic(channel)

    async def run_daily_report_logic(self, channel, manual: bool = False):
        logger.info("Generating daily report...")

        # Pre-read knowledge files directly
        weather_data = await fetch_current_weather(self.weather_api_key, self.weather_lat, self.weather_lon, self.weather_units)
        forecast = await fetch_forecast(self.weather_api_key, self.weather_lat, self.weather_lon, self.weather_units)

        today = datetime.now(BOT_TZ).date().isoformat()

        # Build forecast guidance for the LLM
        forecast_guidance = ""
        if forecast.get("rain_alert"):
            forecast_guidance += "- Rain is expected in the next 48 hours. Mention that watering may not be needed.\n"
        if forecast.get("frost_risk"):
            forecast_guidance += "- Frost is expected! Suggest covering sensitive plants and bringing in any tender seedlings.\n"

        # Build member context for task grouping
        members = list_members()
        member_context = ""
        if members:
            member_names = ", ".join(name.title() for name in members.keys())
            member_context = (
                f"\nREGISTERED MEMBERS: {member_names}\n"
                "When listing tasks, group them by assignee. Show each person's assigned tasks under their name, "
                "and list unassigned tasks in a separate 'Unassigned' section. Use the member names exactly as shown.\n"
            )

        prompt = (
            f"Generate the daily morning briefing.\n"
            f"Today's Date: {today}\n"
            f"Current Weather: {weather_data}\n"
            f"Forecast: {forecast['summary']}\n\n"
            "INSTRUCTION: Read the 'garden_log.md', 'tasks.md', 'planting_calendar.md', and 'almanac.md' files using your tools.\n"
            "Analyze the information to identify:\n"
            "1. Urgent tasks due today/soon (from tasks.md)\n"
            "2. Planting actions based on weather/season (from planting_calendar.md)\n"
            "3. Recent log context (from garden_log.md)\n"
            "4. Weather-based advice based on the forecast\n"
        )
        if member_context:
            prompt += member_context
        if forecast_guidance:
            prompt += f"\nWEATHER NOTES:\n{forecast_guidance}\n"
        prompt += "If nothing is urgent, respond with exactly 'NO_ACTION'."

        inputs = {"messages": [HumanMessage(content=prompt)]}
        ephemeral_thread_id = f"daily_report_{today}"
        config = {"configurable": {"thread_id": ephemeral_thread_id}}
        try:
            result = await asyncio.wait_for(
                graph_module.app_graph.ainvoke(inputs, config=config),
                timeout=LLM_TIMEOUT,
            )
            response = self._extract_text(result["messages"][-1].content)

            # Write weather + forecast + briefing to daily file for tool access
            daily_path = os.path.join("data", f"daily_{today}.md")
            daily_content = (
                f"# Daily Report — {today}\n\n"
                f"## Weather\n{weather_data}\n\n"
                f"## Forecast\n{forecast['summary']}\n\n"
                f"## Briefing\n{response}\n"
            )
            try:
                with open(daily_path, "w") as f:
                    f.write(daily_content)
                logger.info(f"Wrote daily file: {daily_path}")
            except Exception as e:
                logger.error(f"Failed to write daily file: {e}")

            if "NO_ACTION" not in response:
                if members:
                    response = self._inject_mentions(response)
                full_msg = f"**Morning Briefing:**\n{response}"
                chunks = self._chunk_text(full_msg, max_size=DISCORD_MESSAGE_LIMIT)
                for chunk in chunks:
                    await channel.send(chunk)
            elif manual:
                await channel.send("Nothing urgent today — all clear!")
        except asyncio.TimeoutError:
            logger.error(f"Daily report timed out after {LLM_TIMEOUT} seconds")
            if channel:
                await channel.send("The daily briefing timed out — I'll try again tomorrow. You can also run `!briefing` to retry manually.")
        except Exception as e:
            logger.error(f"Daily report failed: {e}")
            if channel:
                err_str = str(e).lower()
                if "resource" in err_str and "exhausted" in err_str or "429" in str(e) or "quota" in err_str:
                    await channel.send("I've hit my API usage limit — the daily briefing will resume once the quota resets. Try `!briefing` later.")
                else:
                    await channel.send(f"Error generating briefing: {e}")

    @daily_report.before_loop
    async def before_daily_report(self):
        await self.wait_until_ready()

    async def debrief(self, ctx):
        """Manually trigger the daily debrief."""
        if ctx.channel.id != self.journal_channel_id:
            await ctx.send("This command can only be used in the journal channel.")
            return

        await self.run_daily_debrief_logic(ctx.channel, user_id=ctx.author.id)

    async def setup_onboarding(self, ctx):
        """Start the onboarding flow via DM."""
        if is_onboarding_complete():
            await ctx.send("Setup is already complete! Your almanac and garden layout are configured. "
                           "You can update them anytime by chatting with me in the questions channel or DMs.")
            return

        if not isinstance(ctx.channel, discord.DMChannel):
            # Called from a server channel — redirect to DMs
            await ctx.send("Check your DMs — I'll walk you through setup there!")
            dm_channel = await ctx.author.create_dm()
        else:
            dm_channel = ctx.channel

        prompt = "Hi! I'm Beanbot, your personal gardening assistant. Let's get you set up! First — what city or area are you located in? This helps me determine your hardiness zone and frost dates."
        response = await self.process_llm_request(prompt, "onboarding", thread_id=str(dm_channel.id))
        await dm_channel.send(response)

    async def consolidate(self, ctx, topic: str):
        """Consolidate knowledge files — single topic or full report."""
        allowed = [self.questions_channel_id, self.reminders_channel_id, self.journal_channel_id]
        if ctx.channel.id not in allowed:
            await ctx.send("This command can only be used in the questions, reminders, or journal channel.")
            return

        if topic and topic.lower() == "tasks":
            await self._consolidate_tasks(ctx)
        elif topic:
            await self._consolidate_single(ctx, topic)
        else:
            await self._consolidate_full(ctx)

    async def _consolidate_single(self, ctx, topic: str):
        """Consolidate all files related to a single topic."""
        async with ctx.channel.typing():
            # Pre-discover related files by filename and content
            filename_result = find_related_files(topic)
            content_matches = search_file_contents(topic)

            # Parse filenames from find_related_files result
            related_files = []
            if not filename_result.startswith("No files found"):
                # Format: "Related files: a.md, b.md"
                parts = filename_result.split(": ", 1)
                if len(parts) == 2:
                    related_files = [f.strip() for f in parts[1].split(",")]

            # Combine and deduplicate
            all_files = list(dict.fromkeys(related_files + content_matches))

            if not all_files:
                await ctx.send(f"No files found related to **{topic}**.")
                return

            file_list = "\n".join(f"- `{f}`" for f in all_files)
            full_msg = f"**Consolidating '{topic}'** — found {len(all_files)} file(s):\n{file_list}\n\nWorking..."
            if len(full_msg) <= DISCORD_MESSAGE_LIMIT:
                await ctx.send(full_msg)
            else:
                chunks = self._chunk_text(f"**Consolidating '{topic}'** — found {len(all_files)} file(s):\n{file_list}", max_size=DISCORD_MESSAGE_LIMIT)
                for chunk in chunks:
                    await ctx.send(chunk)
                await ctx.send("Working...")

            filenames_str = ", ".join(all_files)
            prompt = (
                f"[CONTEXT: KNOWLEDGE CONSOLIDATION for topic '{topic}'.]\n\n"
                f"The following files are related to '{topic}': {filenames_str}\n\n"
                f"Your job:\n"
                f"1. Read ALL of these files using tool_read_multiple_files.\n"
                f"2. Back up EVERY file using tool_backup_file (one call per file).\n"
                f"3. Analyze the content. Identify which files are primarily ABOUT '{topic}' vs files that merely mention it.\n"
                f"4. For files primarily about '{topic}': merge all unique facts into a single clean '{topic}.md' using tool_overwrite_file.\n"
                f"   - Organize by category (Overview, Planting, Care, Pests, Harvesting, Companion Plants, Notes, etc.)\n"
                f"   - Remove duplicate information and '### Update YYYY-MM-DD' section headers\n"
                f"   - Preserve ALL unique facts — only remove true duplicates\n"
                f"   - Preserve any '> **Conflict:**' notes — these flag contradictions for manual review\n"
                f"   - Collect all '## Sources' entries from merged files, deduplicate them, and include a single '## Sources' section at the bottom of the consolidated file\n"
                f"   - Use clean markdown formatting\n"
                f"5. Delete sub-files that were fully merged into '{topic}.md' using tool_delete_file. Do NOT delete '{topic}.md' itself.\n"
                f"6. Leave files that merely MENTION '{topic}' alone — do not modify or delete them.\n"
                f"7. Reply with a summary of what was consolidated, merged, and deleted."
            )

            today = date.today().isoformat()
            response = await self.process_llm_request(
                prompt, "questions", thread_id=f"consolidate_{topic}_{today}"
            )
            await self._send_long_reply(ctx.message, response)

    async def _consolidate_tasks(self, ctx):
        """Analyze open tasks for duplicates and present interactive consolidation UI."""
        open_tasks = get_open_tasks()
        if len(open_tasks) < 2:
            await ctx.send("Not enough open tasks to consolidate (need at least 2).")
            return

        # Read completed tasks from tasks.md
        tasks_path = os.path.join(DATA_DIR, "tasks.md")
        completed_tasks = []
        try:
            with open(tasks_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if "- [x]" in line:
                        completed_tasks.append(line)
        except Exception:
            pass

        progress_msg = await ctx.send(f"Analyzing {len(open_tasks)} open tasks for duplicates...")

        try:
            groups = await analyze_duplicate_tasks(open_tasks)
        except Exception as e:
            logger.error(f"Task duplicate analysis failed: {e}", exc_info=True)
            await progress_msg.edit(content=f"Analysis failed: {e}")
            return

        if not groups:
            await progress_msg.edit(content="No duplicate or similar tasks found — all clear!")
            return

        view = TaskConsolidateView(
            bot=self,
            groups=groups,
            all_tasks=open_tasks,
            completed_tasks=completed_tasks,
            author_id=ctx.author.id,
        )
        embed = view._build_embed()
        await progress_msg.edit(
            content=f"Found **{len(groups)}** group(s) of duplicate/similar tasks. Review each one:",
            embed=embed,
            view=view,
        )
        view.message = progress_msg

    def _format_size(self, size_bytes: int) -> str:
        """Format byte size as human-readable string."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        return f"{size_bytes / 1024:.1f} KB"

    def _build_categories_md(self, categories: dict[str, dict[str, list[str]]], size_map: dict) -> str:
        """Build the categories.md content with 2-tier category/species structure."""
        today = date.today().isoformat()
        lines = [f"# Knowledge Library Categories\n", f"*Last updated: {today}*\n"]

        sorted_cats = sorted(
            categories.items(),
            key=lambda x: (x[0] == "Uncategorized", x[0])
        )

        total_files = sum(
            len(files) for species_dict in categories.values() for files in species_dict.values()
        )
        total_species = sum(len(species_dict) for species_dict in categories.values())
        lines.append(f"**{total_files} files** across **{len(categories)} categories**, **{total_species} species/topics**\n")

        for cat_name, species_dict in sorted_cats:
            cat_file_count = sum(len(files) for files in species_dict.values())
            section_lines = [f"## {cat_name} ({cat_file_count} files, {len(species_dict)} species)\n"]

            # Sort species by file count descending, then alphabetically
            sorted_species = sorted(
                species_dict.items(),
                key=lambda x: (-len(x[1]), x[0])
            )

            for species, files in sorted_species:
                sorted_files = sorted(files)
                count_label = f"{len(files)} file{'s' if len(files) != 1 else ''}"
                section_lines.append(f"### {species} ({count_label})")
                for f in sorted_files:
                    section_lines.append(f"  - {f} ({self._format_size(size_map.get(f, 0))})")
                if len(files) >= 2:
                    hint = species.lower().replace(" ", "_")
                    section_lines.append(f"  > `!consolidate {hint}`")
                section_lines.append("")

            lines.append("\n".join(section_lines))

        return "\n\n".join(lines) + "\n"

    async def _consolidate_full(self, ctx):
        """Categorize all knowledge files, save to categories.md, and send a summary."""
        async with ctx.channel.typing():
            file_entries = get_library_files()

            if not file_entries:
                await ctx.send("No knowledge files found in the library.")
                return

            progress_msg = await ctx.send(f"Categorizing {len(file_entries)} knowledge files...")

            async def update_progress(batch_num, total_batches):
                try:
                    await progress_msg.edit(content=f"Categorizing {len(file_entries)} files... (batch {batch_num}/{total_batches})")
                except Exception:
                    pass

            try:
                categories = await categorize_files(file_entries, progress_callback=update_progress)
                size_map = {e["filename"]: e["size_bytes"] for e in file_entries}

                # Derive merge suggestions deterministically (no extra LLM call)
                merge_suggestions = derive_merge_suggestions(categories)

                # Save categories.md
                categories_md = self._build_categories_md(categories, size_map)
                categories_path = os.path.join("data", "categories.md")
                with open(categories_path, "w") as f:
                    f.write(categories_md)
                logger.info(f"Saved categorization to {categories_path}")

                # Build a short Discord summary
                total_files = sum(
                    len(files) for species_dict in categories.values() for files in species_dict.values()
                )
                total_species = sum(len(species_dict) for species_dict in categories.values())
                sorted_cats = sorted(
                    categories.items(),
                    key=lambda x: (-sum(len(f) for f in x[1].values()), x[0])
                )
                cat_summary = "\n".join(
                    f"  **{name}**: {sum(len(f) for f in species_dict.values())} files ({len(species_dict)} species)"
                    for name, species_dict in sorted_cats
                )

                summary = f"Categorized **{total_files}** files into **{len(categories)}** categories, **{total_species}** species:\n{cat_summary}\n"

                if merge_suggestions:
                    top_merges = merge_suggestions[:15]
                    merge_lines = []
                    for m in top_merges:
                        merge_lines.append(
                            f"  {m['species']}: {len(m['files'])} files \u2192 `!consolidate {m['species'].lower().replace(' ', '_')}`"
                        )
                    summary += f"\n**Top merge candidates** ({len(merge_suggestions)} species with 2+ files):\n" + "\n".join(merge_lines)
                    if len(merge_suggestions) > 15:
                        summary += f"\n  *(+{len(merge_suggestions) - 15} more in categories.md)*"

                summary += "\n\nSee `categories.md` for the full species breakdown."
                await self._send_long_reply(ctx.message, summary)

            except Exception as e:
                logger.warning(f"LLM categorization failed: {e}", exc_info=True)
                await ctx.send(f"Categorization failed: {type(e).__name__}: {e}")

    async def run_daily_debrief_logic(self, channel, user_id: int = None):
        """Send the debrief prompt with open tasks, a task-completion Select, and the debrief button."""
        logger.info("Sending daily debrief prompt...")

        today = datetime.now(BOT_TZ).date().isoformat()

        if user_id:
            # Manual !debrief — show the calling user's tasks + unassigned
            user_name = get_member_name_by_discord_id(user_id)
            if user_name:
                open_tasks = get_tasks_for_user(user_name)
                label = f"Your tasks ({user_name.title()})"
            else:
                open_tasks = get_open_tasks()
                label = "Open tasks"
        else:
            # Scheduled 8 PM debrief — show all open tasks
            open_tasks = get_open_tasks()
            label = "Open tasks"

        # Filter to only today's and overdue tasks (+ tasks with no due date)
        open_tasks = filter_tasks_due_today_or_overdue(open_tasks, today)

        if open_tasks:
            task_display = "\n".join(
                f"- {_extract_task_description(t)}" for t in open_tasks
            )
            msg = f"**Evening Debrief — {today}**\n\n{label}:\n{task_display}\n\nSelect completed tasks below, then click **Mark Complete**. When you're done, click **Log Today's Debrief**."
            view = DebriefTaskView(self, open_tasks)
        else:
            msg = f"**Evening Debrief — {today}**\n\nNo open tasks. Click below to log what you did today."
            view = DebriefView(self)

        if len(msg) <= DISCORD_MESSAGE_LIMIT:
            await channel.send(msg, view=view)
        else:
            chunks = self._chunk_text(msg, max_size=DISCORD_MESSAGE_LIMIT)
            for chunk in chunks[:-1]:
                await channel.send(chunk)
            await channel.send(chunks[-1], view=view)

    @tasks.loop(time=DEBRIEF_TIME)
    async def daily_debrief(self):
        if self.journal_channel_id == 0:
            return

        channel = self.get_channel(self.journal_channel_id)
        if not channel:
            logger.warning(f"Journal channel {self.journal_channel_id} not found.")
            return

        await self.run_daily_debrief_logic(channel)

    @daily_debrief.before_loop
    async def before_daily_debrief(self):
        await self.wait_until_ready()

    # --- Weather Alerts (every 6 hours) ---

    @tasks.loop(hours=WEATHER_ALERT_INTERVAL_HOURS)
    async def weather_alerts(self):
        if self.reminders_channel_id == 0:
            return

        channel = self.get_channel(self.reminders_channel_id)
        if not channel:
            return

        forecast = await fetch_forecast(self.weather_api_key, self.weather_lat, self.weather_lon, self.weather_units)
        if not forecast.get("frost_risk") and not forecast.get("rain_alert"):
            return

        # Deduplicate: max one alert per day via flag file
        flag_path = os.path.join("data", ".alert_flag")
        today = datetime.now(BOT_TZ).date().isoformat()
        try:
            if os.path.exists(flag_path):
                with open(flag_path) as f:
                    if f.read().strip() == today:
                        return  # Already alerted today
        except Exception:
            pass

        # Build alert message
        temp_label = "°F" if self.weather_units == "imperial" else "°C"
        precip_label = "in" if self.weather_units == "imperial" else "mm"
        parts = ["**⚠ Weather Alert**"]
        if forecast.get("frost_risk"):
            parts.append(
                f"🥶 **Frost risk** — temps dropping to {forecast.get('min_temp', '?'):.0f}{temp_label} in the next 48 hours. "
                "Consider covering sensitive plants and bringing in tender seedlings."
            )
        if forecast.get("rain_alert"):
            parts.append(
                f"🌧 **Rain expected** — up to {forecast.get('max_rain_prob', 0):.0f}% chance, "
                f"{forecast.get('max_rain_mm', 0):.1f}{precip_label} total. You may be able to skip watering."
            )

        await channel.send("\n".join(parts))

        # Write flag
        try:
            os.makedirs(os.path.dirname(flag_path), exist_ok=True)
            with open(flag_path, "w") as f:
                f.write(today)
        except Exception as e:
            logger.error(f"Failed to write alert flag: {e}")

    @weather_alerts.before_loop
    async def before_weather_alerts(self):
        await self.wait_until_ready()

    # --- Recap ---

    async def recap(self, ctx, days: int = 7):
        """Manually trigger a garden recap for the last N days."""
        allowed = [self.reminders_channel_id, self.journal_channel_id, self.questions_channel_id]
        if ctx.channel.id not in allowed:
            await ctx.send("This command can only be used in the reminders, journal, or questions channel.")
            return

        days = max(1, min(days, MAX_RECAP_DAYS))
        await ctx.send(f"Generating {days}-day garden recap...")
        await self.run_recap_logic(ctx.channel, days, reply_message=ctx.message)

    async def run_recap_logic(self, channel, days: int = 7, reply_message=None):
        """Generate a recap of recent garden activity."""
        today = date.today().isoformat()
        start_date = (date.today() - timedelta(days=days)).isoformat()

        prompt = (
            f"[CONTEXT: Generate a GARDEN RECAP covering {start_date} to {today} ({days} days).]\n\n"
            "Read 'garden_log.md', 'harvests.md', and 'tasks.md' using your tools.\n"
            "Summarize:\n"
            "1. **Activities**: Key things done in this period (from garden_log.md)\n"
            "2. **Harvests**: What was harvested and approximate totals (from harvests.md)\n"
            "3. **Tasks**: Tasks completed vs still open (from tasks.md)\n"
            "4. **Highlights**: Any notable events, milestones, or concerns\n\n"
            f"Focus on entries from the last {days} days. Keep the recap concise but informative."
        )

        ephemeral_thread_id = f"recap_{today}"
        response = await self.process_llm_request(prompt, "questions", thread_id=ephemeral_thread_id)

        if reply_message:
            await self._send_long_reply(reply_message, response)
        else:
            # For scheduled recaps, send directly to channel
            chunks = self._chunk_text(response, max_size=DISCORD_MESSAGE_LIMIT)
            await channel.send(f"**Weekly Garden Recap ({days} days)**\n{chunks[0]}")
            for chunk in chunks[1:]:
                await channel.send(chunk)

    @tasks.loop(time=WEEKLY_RECAP_TIME)
    async def weekly_recap(self):
        """Post a weekly recap on the configured day."""
        now = datetime.now(BOT_TZ)
        if now.weekday() != WEEKLY_RECAP_DAY:
            return

        if self.reminders_channel_id == 0:
            return

        channel = self.get_channel(self.reminders_channel_id)
        if not channel:
            logger.warning(f"Reminders channel {self.reminders_channel_id} not found for weekly recap.")
            return

        logger.info("Generating weekly recap...")
        await self.run_recap_logic(channel, days=7)

    @weekly_recap.before_loop
    async def before_weekly_recap(self):
        await self.wait_until_ready()

    # --- Database Checkpoint Pruning (nightly at 3 AM) ---

    @tasks.loop(time=DB_PRUNE_TIME)
    async def db_prune(self):
        """Nightly pruning of old conversation checkpoints."""
        await self._run_db_prune()

    @db_prune.before_loop
    async def before_db_prune(self):
        await self.wait_until_ready()

    async def _run_db_prune(self):
        """Delete old ephemeral checkpoints and trim persistent threads."""
        import aiosqlite
        from src.graph import DB_PATH

        cutoff = (datetime.now(BOT_TZ).date() - timedelta(days=DB_PRUNE_RETENTION_DAYS)).isoformat()

        try:
            async with aiosqlite.connect(DB_PATH) as conn:
                # 1) Delete ephemeral threads older than 7 days
                #    These have date suffixes like daily_report_2025-06-15
                ephemeral_prefixes = [
                    "daily_report_",
                    "debrief_",
                    "recap_",
                    "consolidate_",
                    "calendar_task_",
                ]
                total_deleted = 0

                for prefix in ephemeral_prefixes:
                    # ISO date string comparison works for chronological ordering
                    cursor = await conn.execute(
                        "DELETE FROM writes WHERE thread_id LIKE ? AND thread_id < ?",
                        (f"{prefix}%", f"{prefix}{cutoff}"),
                    )
                    total_deleted += cursor.rowcount
                    cursor = await conn.execute(
                        "DELETE FROM checkpoints WHERE thread_id LIKE ? AND thread_id < ?",
                        (f"{prefix}%", f"{prefix}{cutoff}"),
                    )
                    total_deleted += cursor.rowcount

                # 2) For persistent threads (numeric channel IDs), keep only last 20 checkpoints
                cursor = await conn.execute(
                    "SELECT DISTINCT thread_id FROM checkpoints WHERE thread_id GLOB '[0-9]*'"
                )
                persistent_threads = [row[0] for row in await cursor.fetchall()]

                for tid in persistent_threads:
                    # Get the 20th newest checkpoint_id as cutoff
                    cursor = await conn.execute(
                        "SELECT checkpoint_id FROM checkpoints "
                        "WHERE thread_id = ? ORDER BY checkpoint_id DESC LIMIT 1 OFFSET ?",
                        (tid, DB_PRUNE_MAX_CHECKPOINTS - 1),
                    )
                    row = await cursor.fetchone()
                    if row:
                        cutoff_id = row[0]
                        cursor = await conn.execute(
                            "DELETE FROM writes WHERE thread_id = ? AND checkpoint_id < ?",
                            (tid, cutoff_id),
                        )
                        total_deleted += cursor.rowcount
                        cursor = await conn.execute(
                            "DELETE FROM checkpoints WHERE thread_id = ? AND checkpoint_id < ?",
                            (tid, cutoff_id),
                        )
                        total_deleted += cursor.rowcount

                await conn.commit()

                # 3) VACUUM to reclaim disk space
                await conn.execute("VACUUM")

                logger.info(f"DB prune complete: {total_deleted} rows deleted.")

        except Exception as e:
            logger.error(f"DB prune failed: {e}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise ValueError("DISCORD_TOKEN not found in environment variables.")
        
    bot = BeanBot()
    bot.run(token, log_handler=None)