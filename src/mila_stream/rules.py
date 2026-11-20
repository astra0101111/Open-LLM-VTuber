"""Rule-based chat filter (Croatian, Serbian, Bosnian, English): spam, insults, hate speech,
prompt injection, personal data. Ported from the 2026-11-19 demo (chat/filters.py).

Filtar spama, uvreda i osjetljivih podataka za poruke iz chata (hrvatski/srpski/bosanski + engleski).

Odluke su lokalne i pravilima voljene (bez poziva prema LLM-u): brze su, besplatne i ne troše kvotu.
Svaka odluka ima razlog, pa se moze mjeriti i podesavati.
"""

import re
import unicodedata
from collections import defaultdict, deque

URL_RE = re.compile(
    r"(https?://\S+|www\.\S+|\b[a-z0-9-]+\.(?:com|net|org|hr|rs|ba|me|io|gg|tv|ly|me|xyz)\b/?\S*)",
    re.IGNORECASE,
)
PROMO_RE = re.compile(
    r"(follow\s*me|sub\s*4\s*sub|prati\s*me|pratite\s*me|check\s*my|my\s*channel|moj\s*kanal|discord\.gg|t\.me|bit\.ly|"
    r"free\s*(?:skins|robux|vbucks)|cheap\s*viewers|buy\s*followers|promo\s*code|bonus\s*kod)",
    re.IGNORECASE,
)
EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d[\s/-]?){8,14}\d(?!\d)")
IBAN_RE = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b")
INJECTION_RE = re.compile(
    r"(ignor\w*\s+(?:sve\s+)?(?:prethodn\w+|upute|pravila)|zaboravi\s+(?:(?:sve|sva|svoje|svoja|tvoje|tvoja)\s+)?(?:upute|pravila|ulogu)|"
    r"ignore\s+(?:all\s+)?(?:previous|prior)\s+instructions|system\s*prompt|ti\s+si\s+sada\s+|"
    r"from\s+now\s+on\s+you|act\s+as\s+|jailbreak|\bdan\s+(?:mode|nacin|način)\b|do\s+anything\s+now|reveal\s+your\s+prompt|otkrij\s+(?:svoje\s+)?upute)",
    re.IGNORECASE,
)

# Razina 2: govor mrznje, prijetnje, poticanje na samoozljedjivanje, seksualno uznemiravanje (uvijek se odbacuje)
SLUR_STEMS = [
    "peder",
    "pedercin",
    "cigan",
    "ciganin",
    "siptar",
    "balij",
    "ustas",
    "cetnik",
    "nigger",
    "nigga",
    "faggot",
    "fagot",
    "retard",
    "ubijse",
    "ubitcu",
    "ubicute",
    "ubijemte",
    "zaklatcu",
    "kys",
    "killyourself",
    "umrisvinjo",
    "ubicu",
    "zaklacu",
    "silovat",
    "silujem",
    "kurvo",
    "drolja",
    "drolj",
    "pizdo",
    "kurvetin",
]
# Razina 1: psovke i izravne uvrede (odbacuju se, ali se broje drugacije)
PROFANITY_STEMS = [
    "jebe",
    "jebi",
    "jebo",
    "jebat",
    "jebem",
    "pick",
    "pizd",
    "kurac",
    "kurca",
    "kurcu",
    "kurv",
    "govn",
    "sranj",
    "sere",
    "seres",
    "debil",
    "idiot",
    "kreten",
    "budal",
    "glupan",
    "glupa",
    "glupac",
    "glupač",
    "majmun",
    "smece",
    "smradu",
    "odjebi",
    "odjeb",
    "gubi",
    "usrani",
    "fuck",
    "shit",
    "bitch",
    "asshole",
    "dick",
    "cunt",
    "whore",
    "stfu",
    "moron",
    "loser",
    "mrzim te",
    "mrzim te",
]
LEET = str.maketrans(
    {
        "0": "o",
        "1": "i",
        "!": "i",
        "3": "e",
        "4": "a",
        "5": "s",
        "7": "t",
        "@": "a",
        "$": "s",
        "€": "e",
        "|": "i",
    }
)
STOP_WORDS_OK = {"sere", "sereš"}  # kratki korijeni koji se smiju samo kao cijela rijec


def unaccent(s: str) -> str:
    s = s.replace("đ", "d").replace("Đ", "D")
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def squash(s: str) -> str:
    """Normalizacija za otkrivanje zamaskiranih psovki: mala slova, bez dijakritika, leet -> slova, bez razmaka i ponavljanja."""
    s = unaccent(s.lower()).translate(LEET)
    s = re.sub(r"[^a-z]", "", s)
    return re.sub(r"(.)\1{2,}", r"\1\1", s)


def tokens(s: str):
    s = unaccent(s.lower()).translate(LEET)
    return re.findall(r"[a-z]+", re.sub(r"(.)\1{2,}", r"\1\1", s))


def toxicity(text: str):
    """Vraca (razina, pronadjeni_korijen): 0 uredno, 1 psovka/uvreda, 2 govor mrznje/prijetnja."""
    sq = squash(text)
    for stem in SLUR_STEMS:
        if stem in sq:
            return 2, stem
    toks = tokens(text)
    for stem in PROFANITY_STEMS:
        for t in toks:
            if t.startswith(stem) or (
                len(stem) >= 4 and stem in t and t not in STOP_WORDS_OK
            ):
                if stem in (
                    "sere",
                    "gubi",
                    "dick",
                    "shit",
                    "pick",
                    "loser",
                    "moron",
                    "smece",
                ) and not (t == stem or t.startswith(stem)):
                    continue
                return 1, stem
    return 0, ""


def redact(text: str) -> str:
    """Uklanja osobne podatke prije slanja modelu (e-pošta, telefon, IBAN) i poveznice."""
    text = EMAIL_RE.sub("[uklonjeno]", text)
    text = IBAN_RE.sub("[uklonjeno]", text)
    text = PHONE_RE.sub("[uklonjeno]", text)
    text = URL_RE.sub("[poveznica]", text)
    return text


class SpamState:
    """Pamti nedavne poruke radi otkrivanja ponavljanja, kopiranih poruka i prebrzog pisanja."""

    def __init__(self):
        self.user_times = defaultdict(deque)  # korisnik -> vremena prihvacenih poruka
        self.user_last = {}  # korisnik -> (normalizirani tekst, vrijeme)
        self.recent = deque(maxlen=400)  # (norm, vrijeme, korisnik)
        self.first_seen = {}  # korisnik -> vrijeme prve poruke

    def norm(self, text: str) -> str:
        return re.sub(
            r"\s+", " ", re.sub(r"[^\w\s]", "", unaccent(text.lower()))
        ).strip()


def check(
    ev, st: SpamState, now: float | None = None, min_len: int = 2, max_len: int = 220
):
    """Vraca (verdict, reason, flags, ociscen_tekst). verdict: accept | drop."""
    now = now if now is not None else ev.received_at
    flags = []
    if ev.kind != "message":
        return "accept", "dogadjaj", tuple(flags), ev.text
    user = ev.author_id or ev.author_name
    raw = (ev.text or "").strip()
    if not raw:
        return "drop", "prazno", tuple(flags), raw
    if raw.startswith(("!", "/")):
        return "drop", "naredba_bota", tuple(flags), raw
    lvl, _stem = toxicity(raw)
    if lvl == 2:
        return "drop", "govor_mrznje_ili_prijetnja", tuple(flags), raw
    if lvl == 1:
        return "drop", "psovka_ili_uvreda", tuple(flags), raw
    # emotikoni / sama interpunkcija
    letters = re.findall(r"[^\W\d_]", raw)
    if len(letters) < min_len:
        return "drop", "bez_teksta", tuple(flags), raw
    if URL_RE.search(
        EMAIL_RE.sub("", raw)
    ):  # an e-mail is redacted below, not dropped as a link
        return "drop", "poveznica", tuple(flags), raw
    if PROMO_RE.search(raw):
        return "drop", "reklama", tuple(flags), raw
    if len(raw) > 400:
        return "drop", "predugo", tuple(flags), raw
    if re.search(r"(.)\1{7,}", raw) or re.search(
        r"(\b\w+\b)(?:\s+\1){4,}", raw, re.IGNORECASE
    ):
        return "drop", "ponavljanje_znakova", tuple(flags), raw
    caps = sum(1 for c in letters if c.isupper())
    if len(letters) >= 12 and caps / len(letters) > 0.8:
        return "drop", "sve_velikim_slovima", tuple(flags), raw
    n = st.norm(raw)
    # kopirana poruka: isti tekst od >= 3 razlicita korisnika u 20 s
    same = {u for (t, tm, u) in st.recent if t == n and now - tm <= 20}
    if len(n) >= 3 and len(same | {user}) >= 3:
        st.recent.append((n, now, user))
        return "drop", "kopirana_poruka", tuple(flags), raw
    # isti korisnik ponavlja istu poruku u 60 s
    last = st.user_last.get(user)
    if last and last[0] == n and now - last[1] <= 60:
        return "drop", "ponovljena_poruka", tuple(flags), raw
    # prebrzo pisanje: vise od 4 poruke u 20 s od istog korisnika
    q = st.user_times[user]
    while q and now - q[0] > 20:
        q.popleft()
    if len(q) >= 4:
        return "drop", "prebrzo_pisanje", tuple(flags), raw
    q.append(now)
    st.user_last[user] = (n, now)
    st.recent.append((n, now, user))
    if INJECTION_RE.search(raw):
        flags.append("pokusaj_preuzimanja_uloge")
    if EMAIL_RE.search(raw) or PHONE_RE.search(raw) or IBAN_RE.search(raw):
        flags.append("osobni_podaci_uklonjeni")
    return "accept", "uredno", tuple(flags), redact(raw)[:max_len]
