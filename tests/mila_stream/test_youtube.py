import asyncio

import pytest
from fakes import FakeHttp, no_sleep

from mila_stream import events as ev
from mila_stream.http import HttpResponse
from mila_stream.youtube import (
    ChatClosed,
    QuotaExhausted,
    YouTubeConfig,
    YouTubeReader,
    parse_item,
)

T0 = 1_800_000_000.0


def iso(t):
    from datetime import datetime, timezone

    return datetime.fromtimestamp(t, timezone.utc).isoformat().replace("+00:00", "Z")


def text_item(i, text, t=T0 + 10, author="UCa", name="Ana", **au):
    return {
        "id": i,
        "snippet": {
            "type": "textMessageEvent",
            "publishedAt": iso(t),
            "textMessageDetails": {"messageText": text},
            "authorChannelId": author,
        },
        "authorDetails": {"channelId": author, "displayName": name, **au},
    }


VIDEOS_OK = HttpResponse(
    200, {"items": [{"liveStreamingDetails": {"activeLiveChatId": "CHAT1"}}]}
)


def page(items, token, interval=2000, offline=None):
    d = {"items": items, "nextPageToken": token, "pollingIntervalMillis": interval}
    if offline:
        d["offlineAt"] = offline
    return HttpResponse(200, d)


def collect(reader, limit=100):
    async def go():
        out = []
        async for e in reader.events():
            out.append(e)
            if len(out) >= limit:
                break
        return out

    return asyncio.run(go())


def reader_with(http, tmp_path=None, clock=lambda: T0, **cfg):
    c = YouTubeConfig(video_id="VID", transport="poll", **cfg)
    return YouTubeReader(
        c,
        http,
        tmp_path / "state.json" if tmp_path else None,
        api_key="KEY",
        sleep=no_sleep,
        clock=clock,
    )


def test_parse_text_superchat_member_tombstone_ban():
    e = parse_item(
        text_item("m1", "bok Mila", isChatModerator=True, isChatSponsor=True)
    )
    assert (e.kind, e.text, e.author_name) == (ev.MESSAGE, "bok Mila", "Ana")
    assert e.roles == ("moderator", "member")
    sc = parse_item(
        {
            "id": "s1",
            "snippet": {
                "type": "superChatEvent",
                "superChatDetails": {
                    "amountMicros": "5000000",
                    "currency": "EUR",
                    "amountDisplayString": "€5.00",
                    "userComment": "za kavu",
                    "tier": 2,
                },
            },
            "authorDetails": {"displayName": "Ivo"},
        }
    )
    assert (sc.kind, sc.amount, sc.text, sc.amount_display) == (
        ev.SUPERCHAT,
        5.0,
        "za kavu",
        "€5.00",
    )
    # gRPC enum names work too
    m = parse_item(
        {"id": "n1", "snippet": {"type": "NEW_SPONSOR_EVENT"}, "authorDetails": {}}
    )
    assert m.kind == ev.MEMBER
    t = parse_item({"id": "m1", "snippet": {"type": "tombstone"}})
    assert (t.kind, t.target_id) == (ev.TOMBSTONE, "m1")
    b = parse_item(
        {
            "id": "b1",
            "snippet": {
                "type": "userBannedEvent",
                "userBannedDetails": {
                    "bannedUserDetails": {"channelId": "UCbad", "displayName": "Troll"}
                },
            },
        }
    )
    assert (b.kind, b.target_id) == (ev.BAN, "UCbad")
    assert parse_item({"id": "p", "snippet": {"type": "pollEvent"}}) is None


def test_poll_dedupes_resumes_and_stops_on_offline(tmp_path):
    http = FakeHttp(
        {
            "/videos": [VIDEOS_OK],
            "/liveChat/messages": [
                page([text_item("m1", "prva"), text_item("m2", "druga")], "TOK1"),
                page([text_item("m2", "druga"), text_item("m3", "treća")], "TOK2"),
                page([], "TOK3", offline="2026-11-20T20:00:00Z"),
            ],
        }
    )
    r = reader_with(http, tmp_path)
    got = collect(r)
    assert [e.id for e in got if e.kind == ev.MESSAGE] == ["m1", "m2", "m3"]
    assert got[-1].kind == ev.CHAT_ENDED
    calls = [c for c in http.calls if c[1].endswith("/liveChat/messages")]
    assert "pageToken" not in calls[0][2]
    assert calls[1][2]["pageToken"] == "TOK1"
    # quota: 1 (videos) + 3 x 5 (list)
    assert r.state.quota_used == 16
    # state remembered: chat id reused without a new videos.list call
    http2 = FakeHttp(
        {"/liveChat/messages": [page([text_item("m4", "nova")], "TOK4", offline="x")]}
    )
    r2 = reader_with(http2, tmp_path)
    got2 = collect(r2)
    assert [e.id for e in got2 if e.kind == ev.MESSAGE] == ["m4"]
    assert not any(c[1].endswith("/videos") for c in http2.calls)
    assert http2.calls[0][2]["pageToken"] == "TOK3"


def test_skip_history_on_first_connect():
    http = FakeHttp(
        {
            "/videos": [VIDEOS_OK],
            "/liveChat/messages": [
                page(
                    [
                        text_item("old", "stara", t=T0 - 600),
                        text_item("new", "nova", t=T0 + 1),
                    ],
                    "T",
                    offline="x",
                )
            ],
        }
    )
    got = collect(reader_with(http))
    assert [e.id for e in got if e.kind == ev.MESSAGE] == ["new"]


def test_chat_ended_and_quota_errors():
    ended = HttpResponse(403, {"error": {"errors": [{"reason": "liveChatEnded"}]}})
    http = FakeHttp({"/videos": [VIDEOS_OK], "/liveChat/messages": [ended]})
    with pytest.raises(ChatClosed):
        collect(reader_with(http))
    quota = HttpResponse(403, {"error": {"errors": [{"reason": "quotaExceeded"}]}})
    http = FakeHttp({"/videos": [VIDEOS_OK], "/liveChat/messages": [quota]})
    with pytest.raises(QuotaExhausted):
        collect(reader_with(http))
    no_chat = FakeHttp(
        {"/videos": [HttpResponse(200, {"items": [{"liveStreamingDetails": {}}]})]}
    )
    with pytest.raises(ChatClosed):
        collect(reader_with(no_chat))


def test_rate_limit_and_network_errors_retry():
    rl = HttpResponse(403, {"error": {"errors": [{"reason": "rateLimitExceeded"}]}})
    http = FakeHttp(
        {
            "/videos": [VIDEOS_OK],
            "/liveChat/messages": [
                rl,
                ConnectionError("net down"),
                page([text_item("m1", "ok")], "T", offline="x"),
            ],
        }
    )
    got = collect(reader_with(http))
    assert [e.id for e in got if e.kind == ev.MESSAGE] == ["m1"]


def test_local_quota_guard_stops_before_google_does():
    http = FakeHttp({"/videos": [VIDEOS_OK], "/liveChat/messages": [page([], "T")]})
    r = reader_with(http, daily_quota=30, quota_reserve=0)
    with pytest.raises(QuotaExhausted):
        collect(r)
    assert r.state.quota_used <= 30


def test_poll_pacing_fits_planned_hours():
    r = reader_with(
        FakeHttp({}), daily_quota=10_000, quota_reserve=1_000, planned_hours=4
    )
    # 9000 units / 5 per call = 1800 calls over 4 h -> one call every 8 s
    assert r.poll_interval_floor() == pytest.approx(8.0)


def test_stream_transport_with_fake_grpc():
    class FakeStream:
        def __init__(self):
            self.opens = []

        async def open(self, chat_id, token):
            self.opens.append(token)
            if len(self.opens) == 1:
                yield {"items": [text_item("g1", "prva")], "nextPageToken": "S1"}
                raise ConnectionError("dropped")
            yield {
                "items": [text_item("g1", "prva"), text_item("g2", "druga")],
                "nextPageToken": "S2",
                "offlineAt": "x",
            }

    fake = FakeStream()
    c = YouTubeConfig(video_id="VID", transport="auto")
    r = YouTubeReader(
        c,
        FakeHttp({"/videos": [VIDEOS_OK]}),
        None,
        api_key="KEY",
        sleep=no_sleep,
        clock=lambda: T0,
        grpc_stream=fake,
    )
    got = collect(r)
    assert r.transport_used == "stream"
    assert [e.id for e in got if e.kind == ev.MESSAGE] == ["g1", "g2"]
    assert fake.opens == [None, "S1"]  # reconnect resumes from the last token
    assert r.state.quota_used == 1 + 2 * 5
