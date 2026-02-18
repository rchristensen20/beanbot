# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.8.15] - 2026-02-17

## [1.8.14] - 2026-02-17

## [1.8.13] - 2026-02-17

## [1.8.12] - 2026-02-17

## [1.8.11] - 2026-02-17

## [1.8.10] - 2026-02-17

## [1.8.9] - 2026-02-17

## [1.8.8] - 2026-02-17

## [1.8.7] - 2026-02-16

## [1.8.6] - 2026-02-16

## [1.8.5] - 2026-02-15

## [1.8.4] - 2026-02-15

## [1.8.3] - 2026-02-15

## [1.8.2] - 2026-02-15

## [1.8.1] - 2026-02-15

## [1.8.0] - 2026-02-15

## [1.7.1] - 2026-02-13

## [1.7.0] - 2026-02-13

## [1.6.0] - 2026-02-11

## [1.5.5] - 2026-02-09

## [1.5.4] - 2026-02-09

## [1.5.3] - 2026-02-09

## [1.5.2] - 2026-02-09

## [1.5.1] - 2026-02-09

## [1.5.0] - 2026-02-09

## [1.4.2] - 2026-02-07

## [1.4.1] - 2026-02-07

## [1.4.0] - 2026-02-07

## [1.3.0] - 2026-02-07

## [1.2.0] - 2026-02-07

### Added
- Member registry (`data/members.json`) with `!register <name>` and `!members` commands
- Task assignment via `[Assigned: Name]` tags — assign tasks to specific people
- `tool_get_my_tasks` — returns a user's assigned tasks plus unassigned tasks
- `tool_list_members` — lists registered garden members
- User identity injection — registered users' messages are prefixed with `[User: Name]` so the LLM knows who's asking
- Daily briefing groups tasks by assignee with Discord @mentions when members are registered
- `!debrief` shows the calling user's tasks + unassigned tasks (when registered)
- `!commands` — lists all commands with brief usage

## [1.1.0] - 2026-02-07

## [1.0.0] - 2025-07-05

### Added
- Agentic LangGraph workflow with Google Gemini (tool-calling agent loop)
- Markdown-based knowledge library (`data/`) with per-topic files
- Channel-based routing (journal, questions, reminders, knowledge-ingest, DMs)
- Knowledge ingestion pipeline (URLs, PDFs, images, raw text)
- Task management with `tasks.md` (add, complete, due dates)
- Harvest tracking with `harvests.md` (structured table)
- Auto-generated planting calendar from knowledge library
- Daily morning briefing with weather, tasks, and planting advice
- Evening debrief with Discord modal (Activities, Harvests, Pest/Disease, Observations, Task Updates)
- Persistent debrief buttons that survive bot restarts
- Weather integration via OpenWeatherMap (current + 48-hour forecast)
- Proactive weather alerts every 6 hours (frost/rain detection, max one per day)
- Weekly garden recap (automatic Sunday + on-demand via `!recap`)
- Knowledge consolidation (`!consolidate` for single topic or full categorization)
- LLM-based semantic file categorization with merge suggestions
- Image understanding via Gemini vision (plant/pest ID, layout mapping)
- Conversation memory via SQLite checkpointer
- Onboarding flow via DM (`!setup`) — guided location, garden layout, and channel orientation
- `!version` command
- Docker deployment with `docker-compose`
- Configurable timezone via `BOT_TIMEZONE`
