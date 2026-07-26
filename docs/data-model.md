# Data model

Every Iceberg table in `nhl.silver.*` and `nhl.gold.*`, plus the raw bronze paths that feed them. For system topology see [architecture.md](./architecture.md).

Column types are Spark types as declared in the transforms (`spark/jobs/{bronze,silver,gold}/*.py`).

## Bronze (raw JSON on `s3a://nhl-bronze/`)

No schema enforcement; consumers read as text and parse. Paths:

| Path | Contents | Written by |
|---|---|---|
| `schedule/date=<YYYY-MM-DD>/schedule.json` | Daily schedule envelope (array of games) | `ingest` Go CLI |
| `play-by-play/season=<YYYYYYYY>/date=<YYYY-MM-DD>/game_<id>.json` | Per-game envelope with `plays[]` and `rosterSpots[]` arrays | `ingest` Go CLI |
| `tracking/season=<YYYYYYYY>/game_id=<id>/event_id=<id>/tracking.json` | Array of tracking frames (~10 Hz per goal) | `spark/jobs/bronze/tracking_ingest.py` |
| `_runs/run=<runID>/failures.json` | Per-run ingest failure manifest | `ingest` Go CLI |

## Silver

### `nhl.silver.games`

- **Grain**: one row per game
- **Partitioning**: unpartitioned
- **Source**: `s3a://nhl-bronze/play-by-play/**/game_*.json` (envelope header only; skips `plays[]` and `rosterSpots[]` arrays)
- **Load**: full-table overwrite (`createOrReplace()`)

Columns (30):

`game_id (long)`, `season (int)`, `game_type (int)`, `game_date (date)`, `start_time_utc (timestamp)`, `eastern_utc_offset (string)`, `venue_utc_offset (string)`, `venue_name (string)`, `venue_location (string)`, `game_state (string)`, `game_schedule_state (string)`, `home_team_id (int)`, `home_team_abbrev (string)`, `home_team_name (string)`, `home_team_score (int)`, `home_team_sog (int)`, `away_team_id (int)`, `away_team_abbrev (string)`, `away_team_name (string)`, `away_team_score (int)`, `away_team_sog (int)`, `last_period_number (int)`, `last_period_type (string)`, `game_outcome_last_period_type (string)`, `reg_periods (int)`, `max_periods (int)`, `limited_scoring (bool)`, `shootout_in_use (bool)`, `ot_in_use (bool)`, `ingested_at (timestamp)`.

### `nhl.silver.plays`

- **Grain**: one row per play event (~300/game)
- **Partitioning**: `season`
- **Source**: `s3a://nhl-bronze/play-by-play/**/game_*.json` → `plays[]` exploded
- **Load**: full-table overwrite (`createOrReplace()`), partitioned by `season`
- **Filters**: none (all event types retained)

All event-detail columns are nullable — a play row only fills the fields relevant to its `type_desc_key`.

Identity + context: `game_id (long)`, `season (int)`, `event_id (long)`, `sort_order (int)`, `type_code (int)`, `type_desc_key (string)`, `period_number (int)`, `period_type (string)`, `time_in_period (string)`, `time_remaining (string)`, `situation_code (string)`, `home_team_defending_side (string)`, `ppt_replay_url (string)`, `event_owner_team_id (int)`.

Coordinates: `x_coord (int)`, `y_coord (int)`, `zone_code (string)`.

Player references (any of these can be set depending on event type): `player_id`, `blocking_player_id`, `shooting_player_id`, `losing_player_id`, `winning_player_id`, `scoring_player_id`, `assist1_player_id`, `assist2_player_id`, `goalie_in_net_id`, `hitting_player_id`, `hittee_player_id`, `committed_by_player_id`, `drawn_by_player_id`, `served_by_player_id` — all `int`.

Goal stats: `scoring_player_total (int)`, `assist1_player_total (int)`, `assist2_player_total (int)`.

Shot / stoppage / penalty: `shot_type (string)`, `reason (string)`, `secondary_reason (string)`, `penalty_type_code (string)`, `penalty_desc_key (string)`, `penalty_duration (int)`.

Score state at event: `away_score (int)`, `home_score (int)`, `away_sog (int)`, `home_sog (int)`.

Goal highlights: `highlight_clip_sharing_url (string)`, `highlight_clip_sharing_url_fr (string)`, `highlight_clip (long)`, `highlight_clip_fr (long)`, `discrete_clip (long)`, `discrete_clip_fr (long)`.

Derived strength state: `away_goalie_present (bool)`, `away_skaters (int)`, `home_skaters (int)`, `home_goalie_present (bool)`, `strength_state (string)` — one of `EV`/`PP`/`SH`, `is_empty_net (bool)`.

Bookkeeping: `ingested_at (timestamp)`.

### `nhl.silver.players`

- **Grain**: one row per unique `player_id`
- **Partitioning**: unpartitioned
- **Source**: `s3a://nhl-bronze/play-by-play/**/game_*.json` → `rosterSpots[]`
- **Dedup**: SCD-1 via `max_by(col, struct(game_date, game_id))` — latest game wins; ties broken by higher `game_id`
- **Load**: full-table overwrite

Columns: `player_id (int)`, `first_name (string)`, `last_name (string)`, `position_code (string)`, `headshot (string)`, `first_seen_date (date)`, `last_seen_date (date)`, `ingested_at (timestamp)`.

### `nhl.silver.teams`

- **Grain**: one row per unique `team_id`
- **Partitioning**: unpartitioned
- **Source**: `nhl.silver.games` — home + away teams unioned (**silver-from-silver**, the only such dependency)
- **Dedup**: SCD-1 via `max_by()` — handles relocations/rebrands
- **Load**: full-table overwrite

Columns: `team_id (int)`, `abbrev (string)`, `name (string)`, `first_seen_date (date)`, `last_seen_date (date)`, `ingested_at (timestamp)`.

### `nhl.silver.game_rosters`

- **Grain**: one row per `(game_id, player_id)`
- **Partitioning**: `season`
- **Source**: `s3a://nhl-bronze/play-by-play/**/game_*.json` → `rosterSpots[]` exploded
- **Dedup**: none — bridge table; a player appears once per game they were rostered for
- **Load**: full-table overwrite (`createOrReplace()`), partitioned by `season`

Columns: `game_id (long)`, `player_id (int)`, `season (int)`, `team_id (int)`, `sweater_number (int)`, `position_code (string)`, `ingested_at (timestamp)`.

### `nhl.silver.tracking_frames`

- **Grain**: one row per frame per goal
- **Partitioning**: `season`
- **Source**: `s3a://nhl-bronze/tracking/**/tracking.json` (array of frame objects)
- **Load**: full-table overwrite (`createOrReplace()`), partitioned by `season`

Coordinate systems: raw feed uses inches from corner origin (`*_in`); derived `*_ft` fields convert to PBP feet with center origin (`(inches - center) / 12.0`, where center is `(1200, 510)` inches on the 200×85 ft rink).

Columns:

- `game_id (long)`, `event_id (long)`, `season (int)`
- `frame_index (int)` — 0-based per goal, via `row_number` window
- `timestamp_ds (long)` — deciseconds since epoch (source feed unit)
- `rel_seconds (double)` — negative offset from the goal frame; 0.0 on the final frame
- `puck_x_in (double)`, `puck_y_in (double)`, `puck_x_ft (double)`, `puck_y_ft (double)`
- `on_ice: array<struct<player_id: long, sweater: int, team_id: int, team_abbrev: string, x_in: double, y_in: double, x_ft: double, y_ft: double>>` — puck entry filtered out
- `ingested_at (timestamp)`

### `nhl.silver.tracking_attempts`

- **Grain**: one row per `(game_id, event_id)` — current-state audit table
- **Partitioning**: `season`
- **Source**: `nhl.silver.plays` filtered to goals with non-null `ppt_replay_url` → HTTP fetches from `wsr.nhle.com`
- **Load**: merge with key-based overwrite. `--retry-transient` mode re-attempts rows with status in `(http_other, fetch_error, invalid_payload)`.

Columns:

- `game_id (long)`, `event_id (long)`, `season (int)`
- `source_url (string)` — `ppt_replay_url` from plays
- `source_object_key (string)` — S3 key on success, null on failure
- `attempted_at (timestamp)`
- `status (string)` — `success` | `http_404` | `http_other` | `fetch_error` | `invalid_payload`
- `http_code (int)` — nullable, only on HTTP errors
- `frame_count (int)` — nullable, best-effort JSON array element count on success
- `error_message (string)` — nullable

## Gold

### `nhl.gold.player_shots`

- **Grain**: one row per goal
- **Partitioning**: `season`
- **Source**: `nhl.silver.{plays, games, players, teams}`
- **Filters**: `type_desc_key = 'goal'` AND `scoring_player_id IS NOT NULL` AND coords non-null AND `game_type IN (1, 2, 3)` — excludes All-Star (4), 4 Nations (19), other exhibitions
- **Load**: full-table overwrite (`createOrReplace()`), partitioned by `season`

Columns: `event_id (long)`, `game_id (long)`, `game_date (date)`, `game_type (int)`, `home_team_abbrev (string)`, `season (int)`, `player_id (int)`, `player_name (string)` [= `first_name || ' ' || last_name`], `player_headshot (string)`, `team_id (int)`, `team_abbrev (string)`, `period_number (int)`, `period_type (string)`, `time_in_period (string)`, `x_coord (int)`, `y_coord (int)`, `shot_type (string)`, `strength_state (string)`, `is_empty_net (bool)`, `home_score (int)`, `away_score (int)`, `ppt_replay_url (string)`, `ingested_at (timestamp)`.

### `nhl.gold.goal_tracking_status`

- **Grain**: one row per goal
- **Partitioning**: unpartitioned
- **Source**: `nhl.silver.plays` (goals) ⨝ `nhl.silver.tracking_attempts` ⨝ `nhl.silver.tracking_frames` ⨝ `nhl.gold.goal_tracking_sequences`
- **Load**: full-table overwrite

Single-enum column the viz switches on. `tracking_status` values:

| Value | Meaning |
|---|---|
| `no_url` | `silver.plays.ppt_replay_url` is null (goal not tracked by NHL) |
| `not_attempted` | URL exists but no `tracking_attempts` row yet |
| `not_tracked` | Attempted, got `http_404` (NHL hasn't published tracking) |
| `fetch_failed` | Attempted, got `http_other`/`fetch_error`/`invalid_payload` |
| `pending_silver_rebuild` | Fetch succeeded but `silver.tracking_frames` not yet rebuilt |
| `pending_gold_sequence_rebuild` | Silver rebuilt but `gold.goal_tracking_sequences` not yet |
| `available` | Fetched successfully AND a `gold.goal_tracking_sequences` row exists (silver frames are not re-checked at this step — gold is downstream of silver, so in practice both correlate) |

Columns: `game_id (long)`, `event_id (long)`, `season (int)`, `ppt_replay_url (string)`, `tracking_status (string)`, `frame_count (int)` [from `silver.tracking_frames`], `attempted_at (timestamp)`, `fetch_status (string)` [raw status from attempts], `http_code (int)`, `error_message (string)`, `ingested_at (timestamp)`.

### `nhl.gold.goal_tracking_sequences`

- **Grain**: one row per goal (collapses per-frame rows from silver into a single ordered array)
- **Partitioning**: `season`
- **Source**: `nhl.silver.tracking_frames`
- **Load**: full-table overwrite (`createOrReplace()`), partitioned by `season`

Serving-shaped for viz animation playback. Drops derived silver columns (`puck_x_ft`, `puck_y_ft`, `rel_seconds`) — viz derives them client-side if needed.

Columns:

- `game_id (long)`, `event_id (long)`, `season (int)`
- `frame_count (int)`
- `frames: array<struct<frame_index: int, timestamp_ds: long, puck_x_in: double, puck_y_in: double, on_ice: array<struct<player_id: long, sweater: int, team_id: int, team_abbrev: string, x_in: double, y_in: double, x_ft: double, y_ft: double>>>>`
- `ingested_at (timestamp)`

## Lineage cheat sheet

```
bronze/schedule                  (ingest CLI)
bronze/play-by-play ─┬─► silver.games ────────► silver.teams
                     ├─► silver.plays ─────────────┐
                     ├─► silver.players ─┬─────────┤
                     └─► silver.game_rosters ──────┤
                                                   ▼
                                        gold.player_shots
bronze/tracking ────► silver.tracking_frames ────► gold.goal_tracking_sequences
             ▲                       │                    │
             │                       └─────────┬──────────┘
     silver.tracking_attempts ────────────────►│
                                               ▼
                                gold.goal_tracking_status
```
