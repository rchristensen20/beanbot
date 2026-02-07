# Ryanbot

A personal gardening assistant Discord bot powered by an agentic AI workflow.

**Built with:** Discord.py | LangGraph | Google Vertex AI (Gemini) | OpenWeatherMap | Docker

## Features

- **Agentic LangGraph workflow** — reason/act/synthesize loop with tool use. The agent decides when to read files, log entries, amend knowledge, or manage tasks.

- **Knowledge library** — markdown files in `data/` that grow organically. Includes plant guides, daily logs, and farm layout info.

- **Task Management** — Persistent todo list (`data/tasks.md`). The agent can schedule tasks with due dates, check them off upon completion, and remind you of urgent items during the daily briefing.

- **Harvest Tracking** — Structured harvest log (`data/harvests.md`). Track crops, amounts, and locations to monitor your garden's yield over time.

- **Planting Calendar** — Auto-generated calendar (`data/planting_calendar.md`) derived from the planting dates found in your knowledge library files.

- **Channel-based routing** — each Discord channel serves a different purpose:
  - **Journal** — log garden updates, harvests, and completed tasks
  - **Questions** — ask anything; the agent reads relevant knowledge files before answering
  - **Reminders** — receives the daily briefing; supports manual task management
  - **Knowledge Ingest** — paste URLs, upload files, or type text for auto-ingestion
  - **DMs** — general conversation

- **Daily briefing** — 8:00 AM MT cron (configurable). Fetches current weather **and a 48-hour forecast**, checks `tasks.md` for due items, reviews the `planting_calendar.md`, and reads recent logs to provide a morning summary. If rain is expected, it advises you may not need to water. If frost is expected, it suggests covering sensitive plants. Supports manual triggering via `!briefing`.

- **Evening debrief** — 8:00 PM MT cron. Posts a summary of open tasks in the journal channel with a **"Log Today's Debrief"** button. Clicking the button opens a Discord modal with 5 optional fields (Activities, Harvests, Pest/Disease, Observations, Task Updates). On submit, the structured data is sent through the LLM agent, which logs activities, records harvests, marks tasks complete, and amends knowledge as needed. Supports manual triggering via `!debrief`. Buttons persist across bot restarts.

- **Knowledge ingestion pipeline** — Send a URL, upload a file, or paste raw text. The bot extracts content, chunks it, and writes structured notes to per-topic markdown files.

- **Weather integration** — OpenWeatherMap API for current conditions and 48-hour forecast.

- **Proactive weather alerts** — Checks the forecast every 6 hours. Posts alerts to the reminders channel when frost (≤ 2°C) or significant rain (≥ 60% chance or ≥ 10mm) is expected. Max one alert per day to avoid spam.

- **Weekly recap** — Automatic 7-day garden summary every Sunday at 8:00 PM MT, posted to the reminders channel. Covers activities, harvests, task progress, and highlights. Also available on-demand via `!recap [days]`.

- **Docker deployment** — single `docker-compose up` with volume persistence.

- **Smart long-message handling** — auto-splits replies at 2,000 characters.

## Setup

### Prerequisites

- Docker & Docker Compose
- Google Cloud project with Vertex AI API enabled
- Discord bot token (with message content intent)
- OpenWeatherMap API key (free tier)

### File Structure

```
ryanbot/
├── src/
│   ├── bot.py                # Discord bot, message routing, daily cron
│   ├── graph.py              # LangGraph state machine (agent ↔ tools loop)
│   └── services/
│       └── tools.py          # File I/O tools (IO, tasks, harvests, calendar)
├── data/                     # Markdown knowledge library (auto-managed)
│   └── backups/              # Consolidation backups (timestamped)
├── privatecredentials/       # Google Cloud service account key (git-ignored)
│   └── credentials.json
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
└── .env                      # Environment variables (git-ignored)
```

### Environment Variables

Create a `.env` file in the project root:

| Variable | Description |
|---|---|
| `DISCORD_TOKEN` | Your Discord bot token |
| `REMINDERS_CHANNEL_ID` | Channel ID for daily briefings |
| `JOURNAL_CHANNEL_ID` | Channel ID for journal updates |
| `QUESTIONS_CHANNEL_ID` | Channel ID for Q&A |
| `KNOWLEDGE_INGEST_CHANNEL_ID` | Channel ID for knowledge ingestion |
| `GCP_PROJECT` | Google Cloud project ID |
| `GCP_LOCATION` | Vertex AI region (default: `us-central1`) |
| `VERTEX_MODEL` | Gemini model name (default: `gemini-2.5-flash`) |
| `OPENWEATHER_API_KEY` | OpenWeatherMap API key |
| `WEATHER_LAT` | Latitude for weather lookups |
| `WEATHER_LON` | Longitude for weather lookups |

### Google Cloud Credentials

1. Create a service account with the **Vertex AI User** role.
2. Download the JSON key, rename it to `credentials.json`.
3. Place it in `privatecredentials/`.

The Docker container mounts this directory and sets `GOOGLE_APPLICATION_CREDENTIALS` automatically.

### Running

```bash
docker-compose up --build -d
```

## How to Use

**Journal / Reminders / Questions**
Interact naturally in these channels. The agent uses tools to persist your data.

*   **Manage Tasks**
    *   "Remind me to prune the roses on Sunday."
    *   "What do I have to do today?"
    *   "I finished the pruning task." (The agent will mark it as complete in `tasks.md`)

*   **Track Harvests**
    *   "Harvested 3 lbs of beans from Bed 2."
    *   "Log a harvest of 10 zucchini."

*   **Consult Calendar**
    *   "When should I plant garlic?" (Checks `planting_calendar.md`)
    *   "Generate a planting calendar." (Scans all plant files to rebuild the calendar)

*   **Log Updates**
    *   "Planted 3 rows of carrots in bed 1." (Logs to `garden_log.md` and `carrots.md`)

*   **Manual Briefing**
    *   Type `!briefing` in the reminders channel to trigger the daily report immediately.

*   **Evening Debrief**
    *   Type `!debrief` in the journal channel to trigger the debrief prompt.
    *   Click the "Log Today's Debrief" button, fill in the modal, and submit.

*   **Consolidate Knowledge**
    *   `!consolidate` — Semantically categorize your entire knowledge library using the LLM. Groups files by type (Trees, Herbs, Vegetables, etc.), identifies merge candidates, and saves results to `categories.md`. Ask the bot *"what categories do I have?"* to see them anytime.
    *   `!consolidate garlic` — Merge all garlic-related files into a single clean `garlic.md`, removing duplicates and `### Update` sections. Backups are created first in `data/backups/`.

*   **Garden Recap**
    *   `!recap` — Summarize the last 7 days of garden activity (journal entries, harvests, task progress).
    *   `!recap 14` — Recap the last 14 days (up to 90).

**Knowledge Ingest Channel**
Paste URLs, upload text files, or type knowledge directly. The bot extracts, chunks, and categorizes it.

## Architecture

| Module | Role |
|---|---|
| `src/bot.py` | Discord bot — routing, commands (`!briefing`, `!debrief`, `!consolidate`, `!recap`), scheduled loops (daily report, debrief, weather alerts, weekly recap), message parsing, debrief UI (modal + persistent view) |
| `src/graph.py` | LangGraph state machine — agent loop with tools, plus direct LLM calls for file categorization (`categorize_files`, `suggest_merges`) |
| `src/services/tools.py` | Core logic — file operations, task management, harvest logging, calendar generation |
| `data/` | Knowledge library — `tasks.md`, `harvests.md`, `planting_calendar.md`, `garden_log.md`, `categories.md`, and topic files |

## Future Enhancements

> These are ideas for future development.

- [x] **Conversation history memory** — Implemented via LangGraph `AsyncSqliteSaver` checkpointer. The agent remembers context across messages within a thread.

- [x] **Image/photo understanding** — Upload garden photos for analysis by Gemini's vision capabilities. Identify plants, pests, and diseases. Garden layout photos update `farm_layout.md` automatically.

- [x] **Farm layout from drawings** — Upload a photo or sketch of your farm layout. The bot extracts spatial info (beds, rows, plantings) into `farm_layout.md`. Supports iterative updates ("I moved the tomatoes to bed 3").

- [x] **PDF ingestion** — Upload PDFs (seed catalogs, extension service guides, soil reports) to the knowledge-ingest channel. Text extracted via `pymupdf`, then run through the chunking and LLM ingestion pipeline.

- [x] **Semantic knowledge categorization** — `!consolidate` uses batched direct LLM calls to categorize the entire knowledge library by plant type and topic, identify merge candidates, and save results to `categories.md`.

- [x] **Proactive weather alerts** — 48-hour forecast integration with frost and rain alerts every 6 hours. Morning briefing includes forecast-based watering and frost protection advice.

- [x] **Weekly recap** — Automatic Sunday evening recap summarizing the week's activities, harvests, and task progress. Also available on-demand via `!recap [days]`.

- [ ] **Onboarding flow** — First-time setup wizard via DM: the bot asks for your location, hardiness zone, what you're growing, and farm size, then auto-creates initial knowledge files (`almanac.md`, `farm_layout.md`) from your responses.
