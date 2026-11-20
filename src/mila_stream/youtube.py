"""YouTube live chat reader.

Primary transport: liveChatMessages.streamList over gRPC (Google's recommended way:
the server pushes messages over one long connection, 5 quota units per connection).
Fallback: liveChatMessages.list polling (5 units per call), paced so a planned stream
stays inside the daily quota.

Docs (checked 2026-11-20):
https://developers.google.com/youtube/data/live/reference/rest/v3/liveChatMessages/streamList
https://developers.google.com/youtube/data/live/streaming-live-chat
https://developers.google.com/youtube/data/determine_quota_cost
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import events as ev
from .events import ChatEvent
from .http import HttpClient

log = logging.getLogger("mila_stream.youtube")

API = "https://www.googleapis.com/youtube/v3"
COST_LIST = 5
COST_STREAM = 5
COST_VIDEOS = 1
GRPC_TARGET = "dns:///youtube.googleapis.com:443"

# gRPC enum names -> REST type names
_GRPC_TYPES = {
    "TEXT_MESSAGE_EVENT": "textMessageEvent",
    "TOMBSTONE": "tombstone",
    "FAN_FUNDING_EVENT": "fanFundingEvent",
    "CHAT_ENDED_EVENT": "chatEndedEvent",
    "SPONSOR_ONLY_MODE_STARTED_EVENT": "sponsorOnlyModeStartedEvent",
    "SPONSOR_ONLY_MODE_ENDED_EVENT": "sponsorOnlyModeEndedEvent",
    "NEW_SPONSOR_EVENT": "newSponsorEvent",
    "MEMBER_MILESTONE_CHAT_EVENT": "memberMilestoneChatEvent",
    "MEMBERSHIP_GIFTING_EVENT": "membershipGiftingEvent",
    "GIFT_MEMBERSHIP_RECEIVED_EVENT": "giftMembershipReceivedEvent",
    "USER_BANNED_EVENT": "userBannedEvent",
    "SUPER_CHAT_EVENT": "superChatEvent",
    "SUPER_STICKER_EVENT": "superStickerEvent",
    "POLL_EVENT": "pollEvent",
    "GIFT_EVENT": "giftEvent",
}


class ChatClosed(Exception):
    """The live chat ended, is disabled or cannot be found. Do not retry."""


class QuotaExhausted(Exception):
    """Daily quota is (or would be) used up. Do not retry today."""


class AuthError(Exception):
    """API key missing or rejected."""


# ---------------------------------------------------------------- parsing


def _ts(iso: str) -> float:
    if not iso:
        return time.time()
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return time.time()


def _micros(v) -> float:
    try:
        return int(v) / 1_000_000
    except (TypeError, ValueError):
        return 0.0


def parse_item(item: dict, received_at: float | None = None) -> ChatEvent | None:
    """Turn one liveChatMessage resource (REST JSON, or gRPC as dict) into a ChatEvent."""
    sn = item.get("snippet") or {}
    au = item.get("authorDetails") or {}
    t = sn.get("type", "")
    t = _GRPC_TYPES.get(t, t)
    roles = []
    if au.get("isChatOwner"):
        roles.append("owner")
    if au.get("isChatModerator"):
        roles.append("moderator")
    if au.get("isChatSponsor"):
        roles.append("member")
    if au.get("isVerified"):
        roles.append("verified")
    e = ChatEvent(
        id=item.get("id", ""),
        platform="youtube",
        author_id=au.get("channelId") or sn.get("authorChannelId", ""),
        author_name=au.get("displayName", ""),
        roles=tuple(roles),
        published_at=_ts(sn.get("publishedAt", "")),
        received_at=received_at if received_at is not None else time.time(),
    )
    if t == "textMessageEvent":
        e.kind = ev.MESSAGE
        e.text = (sn.get("textMessageDetails") or {}).get("messageText") or sn.get(
            "displayMessage", ""
        )
    elif t == "superChatEvent":
        d = sn.get("superChatDetails") or {}
        e.kind = ev.SUPERCHAT
        e.text = d.get("userComment", "")
        e.amount = _micros(d.get("amountMicros"))
        e.currency = d.get("currency", "")
        e.amount_display = d.get("amountDisplayString", "")
        e.tier = int(d.get("tier") or 0)
    elif t == "superStickerEvent":
        d = sn.get("superStickerDetails") or {}
        e.kind = ev.SUPERSTICKER
        e.amount = _micros(d.get("amountMicros"))
        e.currency = d.get("currency", "")
        e.amount_display = d.get("amountDisplayString", "")
        e.tier = int(d.get("tier") or 0)
        e.text = ((d.get("superStickerMetadata") or {}).get("altText")) or ""
    elif t == "newSponsorEvent":
        e.kind = ev.MEMBER
    elif t == "memberMilestoneChatEvent":
        d = sn.get("memberMilestoneChatDetails") or {}
        e.kind = ev.MEMBER_MILESTONE
        e.count = int(d.get("memberMonth") or 0)
        e.text = d.get("userComment", "")
    elif t == "membershipGiftingEvent":
        d = sn.get("membershipGiftingDetails") or {}
        e.kind = ev.GIFT_MEMBERSHIPS
        e.count = int(d.get("giftMembershipsCount") or 0)
    elif t == "giftEvent":
        d = sn.get("giftDetails") or {}
        e.kind = ev.GIFT
        e.text = d.get("giftName", "")
        e.count = int(d.get("comboCount") or 1)
        e.amount = float(d.get("jewelsAmount") or 0)
        e.currency = "jewels"
    elif t == "tombstone":
        e.kind = ev.TOMBSTONE
        e.target_id = e.id
    elif t == "userBannedEvent":
        d = (sn.get("userBannedDetails") or {}).get("bannedUserDetails") or {}
        e.kind = ev.BAN
        e.target_id = d.get("channelId", "")
        e.text = d.get("displayName", "")
    elif t == "chatEndedEvent":
        e.kind = ev.CHAT_ENDED
    else:
        # giftMembershipReceivedEvent, polls, sponsor-only mode, fan funding: ignored
        return None
    return e


def _error_reason(data) -> str:
    try:
        errs = data["error"].get("errors") or []
        if errs:
            return errs[0].get("reason", "")
        return data["error"].get("status", "") or str(data["error"].get("code", ""))
    except Exception:
        return ""


# ---------------------------------------------------------------- quota and state


def _pacific_today() -> str:
    """YouTube quota resets at midnight Pacific Time."""
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("America/Los_Angeles")).date().isoformat()
    except Exception:  # Windows without tzdata: assume PST (UTC-8)
        return (datetime.now(timezone.utc) - timedelta(hours=8)).date().isoformat()


@dataclass
class ReaderState:
    """What survives a restart: chat id, resume token, seen ids, quota used today."""

    video_id: str = ""
    live_chat_id: str = ""
    page_token: str = ""
    quota_day: str = ""
    quota_used: int = 0
    seen: list = field(default_factory=list)

    @classmethod
    def load(cls, path: Path | None) -> ReaderState:
        if path and path.exists():
            try:
                d = json.loads(path.read_text(encoding="utf-8"))
                return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})
            except Exception as e:
                log.warning("state file unreadable, starting fresh: %s", e)
        return cls()

    def save(self, path: Path | None) -> None:
        if not path:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.__dict__, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)


class Quota:
    def __init__(self, state: ReaderState, daily_limit: int, reserve: int):
        self.state = state
        self.daily_limit = daily_limit
        self.reserve = reserve
        self._roll()

    def _roll(self):
        today = _pacific_today()
        if self.state.quota_day != today:
            self.state.quota_day = today
            self.state.quota_used = 0

    @property
    def left(self) -> int:
        self._roll()
        return self.daily_limit - self.reserve - self.state.quota_used

    def spend(self, units: int) -> None:
        self._roll()
        if self.state.quota_used + units > self.daily_limit - self.reserve:
            raise QuotaExhausted(
                f"quota: used {self.state.quota_used} of {self.daily_limit}"
                f" (reserve {self.reserve}); a call costs {units}"
            )
        self.state.quota_used += units


# ---------------------------------------------------------------- reader


@dataclass
class YouTubeConfig:
    video_id: str = ""
    channel_id: str = ""
    api_key_env: str = "MILA_YOUTUBE_API_KEY"
    transport: str = "auto"  # auto | stream | poll
    daily_quota: int = 10_000
    quota_reserve: int = 1_000
    planned_hours: float = 4.0
    skip_history: bool = True
    history_grace_s: float = 5.0
    max_backoff_s: float = 60.0


class YouTubeReader:
    """Yields ChatEvents from one live stream. Remembers chat id and resume token,
    drops duplicates by message id, reconnects with exponential backoff."""

    def __init__(
        self,
        cfg: YouTubeConfig,
        http: HttpClient,
        state_path: Path | None = None,
        api_key: str | None = None,
        sleep: Callable[[float], asyncio.Future] = asyncio.sleep,
        clock: Callable[[], float] = time.time,
        grpc_stream=None,
    ):
        self.cfg = cfg
        self.http = http
        self.state_path = state_path
        self.state = ReaderState.load(state_path)
        self.quota = Quota(self.state, cfg.daily_quota, cfg.quota_reserve)
        self.api_key = (
            api_key if api_key is not None else os.environ.get(cfg.api_key_env, "")
        )
        self._sleep = sleep
        self._clock = clock
        self._seen = deque(self.state.seen, maxlen=5000)
        self._seen_set = set(self._seen)
        self._started = clock()
        self._grpc_stream = grpc_stream  # injectable for tests
        self.transport_used = ""

    # -- helpers

    def _key_params(self) -> dict:
        if not self.api_key:
            raise AuthError(
                f"no API key: set environment variable {self.cfg.api_key_env}"
            )
        return {"key": self.api_key}

    def _save(self):
        self.state.seen = list(self._seen)
        self.state.save(self.state_path)

    def _remember(self, msg_id: str) -> bool:
        """True if new."""
        if not msg_id or msg_id in self._seen_set:
            return False
        if len(self._seen) == self._seen.maxlen:
            self._seen_set.discard(self._seen[0])
        self._seen.append(msg_id)
        self._seen_set.add(msg_id)
        return True

    def poll_interval_floor(self) -> float:
        """Seconds between list calls so that planned_hours fit in the quota left."""
        left = max(1, self.quota.left)
        calls = left / COST_LIST
        return max(1.0, self.cfg.planned_hours * 3600 / calls)

    # -- chat id

    async def resolve_live_chat_id(self) -> str:
        video_id = self.cfg.video_id
        if not video_id and self.cfg.channel_id:
            video_id = await self._find_live_video(self.cfg.channel_id)
        if not video_id:
            raise ChatClosed("set youtube.video_id or youtube.channel_id")
        if self.state.video_id == video_id and self.state.live_chat_id:
            return self.state.live_chat_id  # remembered: no quota spent
        self.quota.spend(COST_VIDEOS)
        r = await self.http.get(
            f"{API}/videos",
            {"part": "liveStreamingDetails", "id": video_id, **self._key_params()},
        )
        self._check(r)
        items = (r.data or {}).get("items") or []
        chat_id = ""
        if items:
            chat_id = (items[0].get("liveStreamingDetails") or {}).get(
                "activeLiveChatId", ""
            )
        if not chat_id:
            raise ChatClosed(f"video {video_id} has no active live chat")
        self.state.video_id = video_id
        self.state.live_chat_id = chat_id
        self.state.page_token = ""
        self._save()
        return chat_id

    async def _find_live_video(self, channel_id: str) -> str:
        # search.list has its own bucket of 100 calls a day, 1 unit each
        r = await self.http.get(
            f"{API}/search",
            {
                "part": "id",
                "channelId": channel_id,
                "eventType": "live",
                "type": "video",
                "maxResults": 1,
                **self._key_params(),
            },
        )
        self._check(r)
        items = (r.data or {}).get("items") or []
        return ((items[0].get("id") or {}).get("videoId", "")) if items else ""

    def _check(self, r) -> None:
        if r.status == 200:
            return
        reason = _error_reason(r.data)
        if reason in ("quotaExceeded", "dailyLimitExceeded"):
            raise QuotaExhausted(reason)
        if (
            reason in ("liveChatEnded", "liveChatDisabled", "liveChatNotFound")
            or r.status == 404
        ):
            raise ChatClosed(reason or str(r.status))
        if r.status in (400, 401) or reason in (
            "keyInvalid",
            "forbidden",
            "API_KEY_INVALID",
        ):
            raise AuthError(reason or str(r.status))
        raise OSError(f"HTTP {r.status} {reason}")

    # -- events

    def _accept(self, item: dict) -> ChatEvent | None:
        e = parse_item(item, received_at=self._clock())
        if e is None:
            return None
        key = f"{e.kind}:{e.id}" if e.kind == ev.TOMBSTONE else e.id
        if not self._remember(key):
            return None
        if (
            self.cfg.skip_history
            and not e.is_control
            and e.published_at < self._started - self.cfg.history_grace_s
        ):
            return None  # history replayed on first connect
        return e

    async def events(self) -> AsyncIterator[ChatEvent]:
        """Run until the chat ends. Raises QuotaExhausted, AuthError or ChatClosed."""
        chat_id = await self.resolve_live_chat_id()
        mode = self.cfg.transport
        if mode == "auto":
            mode = "stream" if (self._grpc_stream or grpc_available()) else "poll"
        self.transport_used = mode
        log.info("YouTube chat %s, transport %s", chat_id, mode)
        source = self._stream(chat_id) if mode == "stream" else self._poll(chat_id)
        async for e in source:
            yield e
            if e.kind == ev.CHAT_ENDED:
                return

    async def _poll(self, chat_id: str) -> AsyncIterator[ChatEvent]:
        backoff = 1.0
        while True:
            params = {
                "liveChatId": chat_id,
                "part": "id,snippet,authorDetails",
                "maxResults": 2000,
                **self._key_params(),
            }
            if self.state.page_token:
                params["pageToken"] = self.state.page_token
            self.quota.spend(COST_LIST)
            try:
                r = await self.http.get(f"{API}/liveChat/messages", params)
            except Exception as e:  # network
                log.warning("poll failed: %s; retry in %.0f s", e, backoff)
                await self._sleep(backoff)
                backoff = min(self.cfg.max_backoff_s, backoff * 2)
                continue
            if r.status != 200:
                reason = _error_reason(r.data)
                if reason == "rateLimitExceeded" or r.status >= 500 or r.status == 429:
                    log.warning(
                        "poll HTTP %s %s; retry in %.0f s", r.status, reason, backoff
                    )
                    await self._sleep(backoff)
                    backoff = min(self.cfg.max_backoff_s, backoff * 2)
                    continue
                if reason == "pageTokenInvalid":
                    self.state.page_token = ""
                    continue
                self._check(r)
            backoff = 1.0
            data = r.data or {}
            for item in data.get("items") or []:
                e = self._accept(item)
                if e:
                    yield e
            self.state.page_token = data.get("nextPageToken", self.state.page_token)
            self._save()
            if data.get("offlineAt"):
                yield ChatEvent(
                    id=f"offline-{chat_id}", platform="youtube", kind=ev.CHAT_ENDED
                )
                return
            wait = max(
                int(data.get("pollingIntervalMillis") or 0) / 1000,
                self.poll_interval_floor(),
            )
            await self._sleep(wait)

    async def _stream(self, chat_id: str) -> AsyncIterator[ChatEvent]:
        stream = self._grpc_stream or GrpcStreamList(self.api_key)
        backoff = 1.0
        while True:
            self.quota.spend(COST_STREAM)
            try:
                async for resp in stream.open(chat_id, self.state.page_token or None):
                    backoff = 1.0
                    for item in resp.get("items") or []:
                        e = self._accept(item)
                        if e:
                            yield e
                    if resp.get("nextPageToken"):
                        self.state.page_token = resp["nextPageToken"]
                        self._save()
                    if resp.get("offlineAt"):
                        yield ChatEvent(
                            id=f"offline-{chat_id}",
                            platform="youtube",
                            kind=ev.CHAT_ENDED,
                        )
                        return
            except (ChatClosed, QuotaExhausted, AuthError):
                raise
            except Exception as e:
                log.warning("stream dropped: %s; reconnect in %.0f s", e, backoff)
            await self._sleep(backoff)
            backoff = min(self.cfg.max_backoff_s, backoff * 2)


# ---------------------------------------------------------------- gRPC transport


def grpc_available() -> bool:
    try:
        import grpc  # noqa: F401

        from .proto import stream_list_pb2, stream_list_pb2_grpc  # noqa: F401

        return True
    except Exception:
        return False


class GrpcStreamList:
    """liveChatMessages.streamList over gRPC, following Google's Python demo.

    Needs `grpcio` and stubs generated from proto/stream_list.proto
    (scripts/mila/build_youtube_proto.py). Not tested against YouTube yet.
    """

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def open(self, chat_id: str, page_token: str | None):
        import grpc
        from google.protobuf.json_format import MessageToDict

        from .proto import stream_list_pb2, stream_list_pb2_grpc

        metadata = (("x-goog-api-key", self.api_key),)
        async with grpc.aio.secure_channel(
            GRPC_TARGET, grpc.ssl_channel_credentials()
        ) as ch:
            stub = stream_list_pb2_grpc.V3DataLiveChatMessageServiceStub(ch)
            req = stream_list_pb2.LiveChatMessageListRequest(
                part=["id", "snippet", "authorDetails"], live_chat_id=chat_id
            )
            if page_token:
                req.page_token = page_token
            try:
                async for resp in stub.StreamList(req, metadata=metadata):
                    yield MessageToDict(resp)
            except grpc.aio.AioRpcError as e:
                code = e.code()
                if code in (
                    grpc.StatusCode.FAILED_PRECONDITION,
                    grpc.StatusCode.NOT_FOUND,
                ):
                    raise ChatClosed(str(code)) from e
                if code in (
                    grpc.StatusCode.PERMISSION_DENIED,
                    grpc.StatusCode.UNAUTHENTICATED,
                ):
                    raise AuthError(str(code)) from e
                raise
