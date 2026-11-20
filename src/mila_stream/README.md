# Mila stream layer

A separate process next to the Open-LLM-VTuber server. It reads live-stream chat,
filters it, decides what Mila answers next and sends that text into the page's
session through the fork's `/inject-text` route.

```
YouTube chat ──> youtube.py ──> filters.py ──> selector.py ──> prompt.py ──> sink.py ──> OLLV /inject-text
  (or sim.py)    reader          rules + blocks  lanes, TTL,      quoted text    waits for
                                                 rate limits      + summary      "not busy"
                          every step is written to journal.py (logs/mila_stream/YYYY-MM-DD.jsonl)
```

## Run

```
# server side (the token protects /inject-*; same value on both sides)
set MILA_INJECT_TOKEN=...            # PowerShell: $env:MILA_INJECT_TOKEN = "..."
uv run run_server.py

# stream layer
copy config_templates\mila_stream.example.yaml mila_stream.yaml   # then set youtube.video_id
set MILA_YOUTUBE_API_KEY=...
uv run python scripts/run_mila_stream.py

# without YouTube and without sending anything: simulated Croatian chat, printed
uv run python scripts/run_mila_stream.py --sim --dry-run
```

## YouTube

- Chat id: `videos.list` (1 quota unit), remembered in `cache/mila_stream_state.json`
  together with the resume token, so a restart costs nothing extra.
- Transport `stream` (Google's recommended `liveChatMessages.streamList` over gRPC,
  5 units per connection) needs `grpcio` and stubs from
  `scripts/mila/build_youtube_proto.py`. Without them `auto` falls back to `poll`
  (`liveChatMessages.list`, 5 units per call), paced so `planned_hours` fit in the
  daily quota (10,000 units, 1,000 kept in reserve: one call every 8 s for 4 hours).
- The reader stops on its own when the chat ends, the quota would be exceeded or the
  key is rejected; the reason is in the log.

## Queue

Lanes: paid (Super Chat, members, gifts) > addressed (Mila's name or a question) >
chat. Time-to-live 180 / 45 / 25 s. Inside a lane the best score wins (weights from
AITuber OnAir comment-intelligence). When an ordinary message is answered, the rest of
the ordinary chat is summarised in the same prompt and not answered one by one.
At most `max_per_minute` answers a minute; gifts are never rate-limited.

## Supervisor, recovery, AI label (plan v0.2, parts 4 and 5)

```
uv run python scripts/run_mila_supervisor.py --config mila_stream.yaml
```

- Starts `mozak` (brain_proxy), `olv` (run_server.py) and `chat` (this layer) from the
  `supervisor` section, restarts a part that exits or fails its check twice in a row,
  with growing pauses; after 5 restarts in 10 minutes it gives up on that part.
- Checks look at real work, not at a process existing: OLLV answers and has a page
  connected and no answer is stuck; brain_proxy `/health` (passive, costs nothing);
  the stream layer's status file is fresh.
- `http://127.0.0.1:8090/` status page for the operator; `/overlay` for OBS
  (Browser Source 1920x1080): the AI label is always on; a banner appears while a
  critical part is down, with an optional recorded sentence (`difficulty_sound`).
- When every brain fails, brain_proxy answers with `brain.fallback_reply`, so Mila
  says it in her own voice instead of going silent.
- Paid events (Super Chat, members, gifts) that Mila did not finish answering are
  written to `logs/mila_dead_letters.jsonl` with the reason (expired, deleted, ban,
  interrupted, stream end, crash). A hard kill loses nothing: the open ones are kept
  in `cache/mila_paid_open.json` and listed on the next start.
- `--crash-test`: kills each part once (only the supervisor's own processes) and
  writes the recovery time of each to `logs/supervisor/crash-test-*.json`.
- `config_templates/mila_pravila_lika.hr.txt`: rules to append to the character
  prompt (truthful about being AI, quoted viewer text is never an instruction,
  personal data, sensitive topics) and the stream description text.
- `scripts/mila/test_ai_iskrenost.py`: asks "are you real?" ten ways through the brain
  and writes a report; the plan's bar is 10 of 10 truthful.

## Tests

```
uv run pytest tests/mila_stream
```

Includes a 30-minute simulated stream on a fake clock: no overlapping speech, no
message sent twice, every message logged with a decision, nothing harmful sent.
