import asyncio
import discord
from discord.ext import commands, tasks
import os
import re
import logging
import base64
import httpx
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from langchain_core.messages import HumanMessage
from bs4 import BeautifulSoup

BOT_TZ = ZoneInfo(os.getenv("BOT_TIMEZONE", "America/Denver"))

# Import our new graph
from src.graph import init_graph
import src.graph as graph_module
# Import basic file reader for the daily report
from src.services.tools import read_knowledge_file, get_open_tasks, find_related_files, search_file_contents, build_library_index, get_library_files, is_onboarding_complete, register_member, get_member_name_by_discord_id, list_members, get_tasks_for_user, get_clearable_knowledge_files, clear_all_knowledge_files, clear_entire_garden, delete_knowledge_file, _sanitize_topic, SYSTEM_FILES, DATA_DIR
from src.services.categorization import categorize_files, suggest_merges
from src.services.weather import fetch_current_weather, fetch_forecast

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("beanbot")

DISCORD_MESSAGE_LIMIT = 2000
INGESTION_CHUNK_SIZE = 50_000
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "60"))


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

CHANNEL_CONTEXT = {
    "journal": "[CONTEXT: User is posting in the JOURNAL channel. Prioritize logging updates and amending knowledge.]\n\n",
    "questions": "[CONTEXT: User is posting in the QUESTIONS channel. You MUST use tools to retrieve info before answering.]\n\n",
    "knowledge_ingest": (
        "[CONTEXT: User is posting content to INGEST into the knowledge library.\n"
        "Your job:\n"
        "1. Identify all gardening/permaculture topics mentioned (plant names, techniques, etc.)\n"
        "2. For EACH topic, call tool_amend_knowledge with the topic name and relevant facts\n"
        "3. Be thorough - extract cultivar info, planting dates, care tips, companion plants, etc.\n"
        "4. When done, reply with ONLY a short confirmation (under 500 chars): list the file names updated and a one-line summary. Do NOT repeat the ingested content back.]\n\n"
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

    async def show_commands(self, ctx):
        """Show all available commands with brief usage."""
        text = (
            "**Beanbot Commands**\n\n"
            "`!briefing` — Trigger the morning briefing (weather, tasks, planting advice)\n"
            "`!debrief` — Start the evening debrief (shows your tasks, opens logging form)\n"
            "`!recap [days]` — Summarize the last N days of garden activity (default 7)\n"
            "`!consolidate` — Categorize all knowledge files and find merge candidates\n"
            "`!consolidate <topic>` — Merge all files about a topic into one clean file\n"
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

    async def _fetch_url_content(self, url: str) -> str:
        """Fetch and extract text from URL."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
            try:
                resp = await client.get(url, follow_redirects=True)
                resp.raise_for_status()
                content_type = resp.headers.get('content-type', '')

                if 'text/html' in content_type:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    # Remove scripts, styles, nav, footer
                    for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
                        tag.decompose()
                    return soup.get_text(separator='\n', strip=True)
                elif 'text/' in content_type or url.endswith(('.md', '.txt')):
                    return resp.text
                else:
                    return f"[Unsupported content type: {content_type}]"
            except Exception as e:
                logger.error(f"Failed to fetch {url}: {e}")
                return f"[Error fetching URL: {e}]"

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

                if urls:
                    # Fetch URL content
                    contents = []
                    for url in urls[:5]:
                        content = await self._fetch_url_content(url)
                        contents.append(f"--- Content from {url} ---\n{content}")
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
                    await self._send_long_reply(message, summary)
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

    @tasks.loop(time=time(hour=8, minute=0, tzinfo=BOT_TZ))
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
        weather_data = await fetch_current_weather(self.weather_api_key, self.weather_lat, self.weather_lon)
        forecast = await fetch_forecast(self.weather_api_key, self.weather_lat, self.weather_lon)

        today = date.today().isoformat()

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
                await channel.send(f"**Morning Briefing:**\n{response}")
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

        if topic:
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
            await ctx.send(f"**Consolidating '{topic}'** — found {len(all_files)} file(s):\n{file_list}\n\nWorking...")

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

    def _format_size(self, size_bytes: int) -> str:
        """Format byte size as human-readable string."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        return f"{size_bytes / 1024:.1f} KB"

    def _build_categories_md(self, categories: dict, merge_suggestions: list, size_map: dict) -> str:
        """Build the categories.md content from categorization results."""
        today = date.today().isoformat()
        lines = [f"# Knowledge Library Categories\n", f"*Last updated: {today}*\n"]

        sorted_cats = sorted(
            categories.items(),
            key=lambda x: (x[0] == "Uncategorized", x[0])
        )

        total = sum(len(files) for files in categories.values())
        lines.append(f"**{total} files** across **{len(categories)} categories**\n")

        for cat_name, cat_files in sorted_cats:
            sorted_files = sorted(cat_files)
            file_lines = "\n".join(
                f"  - {f} ({self._format_size(size_map.get(f, 0))})"
                for f in sorted_files
            )
            section = f"## {cat_name} ({len(cat_files)} files)\n\n{file_lines}"

            cat_merges = [
                m for m in merge_suggestions
                if any(f in cat_files for f in m.get("files", []))
            ]
            if cat_merges:
                merge_lines = []
                for m in cat_merges:
                    target = m.get("target", m["files"][0])
                    others = [f for f in m["files"] if f != target]
                    if others:
                        merge_lines.append(
                            f"  - {' + '.join(m['files'])} -> `!consolidate {target.replace('.md', '')}`"
                        )
                if merge_lines:
                    section += "\n\n  **Merge candidates:**\n" + "\n".join(merge_lines)

            lines.append(section)

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

                # Get merge suggestions (separate, smaller LLM call)
                merge_suggestions = await suggest_merges(categories)

                # Save categories.md
                categories_md = self._build_categories_md(categories, merge_suggestions, size_map)
                categories_path = os.path.join("data", "categories.md")
                with open(categories_path, "w") as f:
                    f.write(categories_md)
                logger.info(f"Saved categorization to {categories_path}")

                # Build a short Discord summary
                total = sum(len(files) for files in categories.values())
                sorted_cats = sorted(
                    categories.items(),
                    key=lambda x: (-len(x[1]), x[0])
                )
                cat_summary = "\n".join(
                    f"  **{name}**: {len(files)} files"
                    for name, files in sorted_cats
                )

                summary = f"Categorized **{total}** files into **{len(categories)}** categories:\n{cat_summary}\n"

                if merge_suggestions:
                    merge_lines = []
                    for m in merge_suggestions[:15]:
                        target = m.get("target", m["files"][0])
                        merge_lines.append(
                            f"  \u2022 {' + '.join(f'`{f}`' for f in m['files'])} \u2192 `!consolidate {target.replace('.md', '')}`"
                        )
                    summary += f"\n**Merge candidates** ({len(merge_suggestions)} found):\n" + "\n".join(merge_lines)
                    if len(merge_suggestions) > 15:
                        summary += f"\n  *(+{len(merge_suggestions) - 15} more — ask me for the full list)*"

                summary += "\n\nAsk me about any category to see its files (e.g. *\"what's in the Trees category?\"*)."
                await self._send_long_reply(ctx.message, summary)

            except Exception as e:
                logger.warning(f"LLM categorization failed: {e}", exc_info=True)
                await ctx.send(f"Categorization failed: {type(e).__name__}: {e}")

    async def run_daily_debrief_logic(self, channel, user_id: int = None):
        """Send the debrief prompt with open tasks and the debrief button."""
        logger.info("Sending daily debrief prompt...")

        if user_id:
            # Manual !debrief — show the calling user's tasks + unassigned
            user_name = get_member_name_by_discord_id(user_id)
            if user_name:
                tasks = get_tasks_for_user(user_name)
                label = f"Your tasks ({user_name.title()})"
            else:
                tasks = get_open_tasks()
                label = "Open tasks"
        else:
            # Scheduled 8 PM debrief — show all open tasks
            tasks = get_open_tasks()
            label = "Open tasks"

        if tasks:
            task_list = "\n".join(tasks)
            msg = f"**Evening Debrief — {date.today().isoformat()}**\n\n{label}:\n{task_list}\n\nClick below to log what you did today."
        else:
            msg = f"**Evening Debrief — {date.today().isoformat()}**\n\nNo open tasks. Click below to log what you did today."

        await channel.send(msg, view=DebriefView(self))

    @tasks.loop(time=time(hour=20, minute=0, tzinfo=BOT_TZ))
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

    @tasks.loop(hours=6)
    async def weather_alerts(self):
        if self.reminders_channel_id == 0:
            return

        channel = self.get_channel(self.reminders_channel_id)
        if not channel:
            return

        forecast = await fetch_forecast(self.weather_api_key, self.weather_lat, self.weather_lon)
        if not forecast.get("frost_risk") and not forecast.get("rain_alert"):
            return

        # Deduplicate: max one alert per day via flag file
        flag_path = os.path.join("data", ".alert_flag")
        today = date.today().isoformat()
        try:
            if os.path.exists(flag_path):
                with open(flag_path) as f:
                    if f.read().strip() == today:
                        return  # Already alerted today
        except Exception:
            pass

        # Build alert message
        parts = ["**⚠ Weather Alert**"]
        if forecast.get("frost_risk"):
            parts.append(
                f"🥶 **Frost risk** — temps dropping to {forecast.get('min_temp_c', '?'):.0f}°C in the next 48 hours. "
                "Consider covering sensitive plants and bringing in tender seedlings."
            )
        if forecast.get("rain_alert"):
            parts.append(
                f"🌧 **Rain expected** — up to {forecast.get('max_rain_prob', 0):.0f}% chance, "
                f"{forecast.get('max_rain_mm', 0):.1f}mm total. You may be able to skip watering."
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

        days = max(1, min(days, 90))  # clamp to reasonable range
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

    @tasks.loop(time=time(hour=20, minute=0, tzinfo=BOT_TZ))
    async def weekly_recap(self):
        """Post a weekly recap every Sunday at 8pm MT."""
        now = datetime.now(BOT_TZ)
        if now.weekday() != 6:  # 6 = Sunday
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

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise ValueError("DISCORD_TOKEN not found in environment variables.")
        
    bot = BeanBot()
    bot.run(token, log_handler=None)