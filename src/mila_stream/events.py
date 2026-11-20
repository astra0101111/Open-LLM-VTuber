"""One shape for every chat event, whatever platform it came from."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

# kinds
MESSAGE = "message"
SUPERCHAT = "superchat"
SUPERSTICKER = "supersticker"
MEMBER = "member"  # new member (YouTube: newSponsorEvent)
MEMBER_MILESTONE = "member_milestone"
GIFT_MEMBERSHIPS = "gift_memberships"  # someone bought memberships for others
GIFT = "gift"  # YouTube Jewels gift
TOMBSTONE = "tombstone"  # a message was deleted; target_id = deleted message id
BAN = "ban"  # a viewer was banned; target_id = banned viewer id
CHAT_ENDED = "chat_ended"
OTHER = "other"

PAID_KINDS = frozenset(
    {SUPERCHAT, SUPERSTICKER, MEMBER, MEMBER_MILESTONE, GIFT_MEMBERSHIPS, GIFT}
)
CONTROL_KINDS = frozenset({TOMBSTONE, BAN, CHAT_ENDED})


@dataclass
class ChatEvent:
    id: str
    platform: str
    kind: str = MESSAGE
    author_id: str = ""
    author_name: str = ""
    text: str = ""
    roles: tuple[str, ...] = ()  # owner, moderator, member, verified
    amount: float = 0.0
    currency: str = ""
    amount_display: str = ""
    tier: int = 0
    count: int = 0  # gifted memberships, member months, gift combo
    published_at: float = field(default_factory=time.time)
    received_at: float = field(default_factory=time.time)
    target_id: str = ""
    label: str = ""  # ground truth from the simulator, for measuring the filter

    @property
    def is_paid(self) -> bool:
        return self.kind in PAID_KINDS

    @property
    def is_control(self) -> bool:
        return self.kind in CONTROL_KINDS

    def short(self) -> dict:
        """Compact form for the log."""
        d = {
            "id": self.id,
            "platform": self.platform,
            "kind": self.kind,
            "author_id": self.author_id,
            "author": self.author_name,
            "text": self.text,
        }
        if self.roles:
            d["roles"] = list(self.roles)
        if self.amount:
            d["amount"] = self.amount
            d["currency"] = self.currency
        if self.target_id:
            d["target_id"] = self.target_id
        if self.label:
            d["label"] = self.label
        return d
