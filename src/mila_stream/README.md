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

## Tests

```
uv run pytest tests/mila_stream
```

Includes a 30-minute simulated stream on a fake clock: no overlapping speech, no
message sent twice, every message logged with a decision, nothing harmful sent.
