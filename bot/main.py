import os
import re
import time
import html
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import feedparser


# -------------------- SUPABASE --------------------

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# NIENTE on_conflict qui: lo aggiungiamo nella request (una sola volta)
TABLE_URL = f"{SUPABASE_URL}/rest/v1/startups"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    # Upsert: richiede UNIQUE su external_id + on_conflict=external_id
    # return=minimal = meno payload, più veloce
    "Prefer": "resolution=merge-duplicates,return=minimal",
}

FEEDS = [
    "https://techcrunch.com/feed/",
    "https://sifted.eu/feed/",
    "https://www.eu-startups.com/feed/",
]

# -------------------- 1) FILTRO NOMI “PRO” --------------------

# geografie / macro-parole che NON sono nomi azienda
GEO = {
    "UK","US","EU","UAE","USA","Europe","European","London","Paris","Berlin","Germany","German",
    "France","French","Spain","Spanish","Italy","Italian","China","India","World","Global"
}

# parole tipiche da titolo che non sono aziende
NOISE = {
    "AI","This","That","These","Those","Ask","Are","How","Why","What","When","Where","Who",
    "Ex","Breaking","Report","Opinion","Analysis","Startup","Startups","VC","CEO","IPO"
}

# stopwords in minuscolo
BAD_LOWER = {
    "startup","startups","company","companies","firm","fund","funding","raises","raise","raised",
    "round","seed","series","pre-seed","deal","launch","launches","announces","announce","says","said",
    "adds","added","report","interview","podcast","newsletter","today","yesterday","tomorrow"
}

ORG_SUFFIX = {"Inc","Ltd","PLC","LLC","GmbH","SAS","Srl","Srls","BV","AB","Oy","SA","AG","NV"}

# Verbi “da news” che spesso seguono il nome azienda
ACTION_VERBS = (
    "raises|raised|acquires|acquired|buys|bought|launches|launched|announces|announced|secures|secured|"
    "lands|landed|partners|partnered|backs|backed|files|filed|unveils|unveiled|introduces|introduced"
)

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def clean_text(s: str) -> str:
    # decode html entities + strip tags
    s = html.unescape(s or "")
    s = re.sub(r"<[^>]+>", " ", s)
    return normalize_spaces(s)

def token_ok(w: str) -> bool:
    if not w:
        return False

    w = w.strip()

    # scarta robe troppo corte (via X, UK, AI ecc)
    if len(w) < 3:
        return False

    # scarta token con numeri
    if any(ch.isdigit() for ch in w):
        return False

    # scarta pure acronimi corti all-caps (CEO, IPO)
    if w.isupper() and len(w) <= 4:
        return False

    # blacklist
    if w in GEO or w in NOISE:
        return False
    if w.lower() in BAD_LOWER:
        return False

    return True

def strip_org_suffix(name: str) -> str:
    parts = name.split()
    parts = [p for p in parts if p not in ORG_SUFFIX]
    return " ".join(parts).strip()

def extract_company_name(title: str) -> str | None:
    """
    Estrae un nome azienda “realistico”:
    1) pattern forti in testa (Name's..., Name raises..., Name acquires...)
    2) fallback: migliori sequenze Capitalized 1-3 parole con score
    """
    t = normalize_spaces((title or "").replace("’", "'"))

    # -------- pattern forti (molto precisi) --------
    strong_patterns = [
        rf"^([A-Z][A-Za-z0-9&.-]+(?:\s[A-Z][A-Za-z0-9&.-]+){{0,2}})'s\b",
        rf"^([A-Z][A-Za-z0-9&.-]+(?:\s[A-Z][A-Za-z0-9&.-]+){{0,2}})\s+({ACTION_VERBS})\b",
    ]

    for pat in strong_patterns:
        m = re.search(pat, t)
        if m:
            cand = strip_org_suffix(normalize_spaces(m.group(1)))
            if cand and all(token_ok(p) for p in cand.split()):
                # evita che sia solo GEO / NOISE come multiword
                if cand in GEO or cand in NOISE:
                    return None
                return cand

    # -------- fallback: candidate sequences --------
    # prendi sequenze 1-3 parole che iniziano con maiuscola (includi OpenAI, Midjourney, DeepMind ecc)
    seqs = re.findall(r"\b([A-Z][A-Za-z0-9&.-]+(?:\s[A-Z][A-Za-z0-9&.-]+){0,2})\b", t)
    seqs = [strip_org_suffix(normalize_spaces(s)) for s in seqs]
    seqs = [s for s in seqs if s]

    if not seqs:
        return None

    scored: list[tuple[int, str]] = []
    for s in seqs[:20]:
        parts = s.split()
        if not parts:
            continue
        if not all(token_ok(p) for p in parts):
            continue

        score = 0

        # multi-word (es. "Open AI") è spesso più “nome”
        if len(parts) >= 2:
            score += 6

        # lunghezza utile
        if len(s) >= 6:
            score += 3

        # bonus se nel titolo subito dopo compare un verbo “da news”
        after = t[len(s):].lstrip()
        if re.match(rf"^({ACTION_VERBS})\b", after, re.IGNORECASE):
            score += 8

        # penalizza se sembra una location/geo
        if s in GEO:
            score -= 20

        scored.append((score, s))

    if not scored:
        return None

    scored.sort(key=lambda x: x[0], reverse=True)
    best = scored[0][1]

    # ultima difesa: evita “This / World / Europe / London …”
    if best in GEO or best in NOISE:
        return None

    return best


# -------------------- 2) ESTRAZIONE DOMINI --------------------

DOMAIN_RE = re.compile(
    r"\b((?:[a-zA-Z0-9-]+\.)+(?:com|io|ai|co|net|org|app|dev|info|eu|it|fr|de|es|uk|me|xyz))\b",
    re.IGNORECASE,
)

def get_domain_from_url(url: str) -> str | None:
    try:
        host = urlparse(url).netloc.lower().split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host or None
    except Exception:
        return None

def extract_domains(text: str) -> list[str]:
    if not text:
        return []
    found = DOMAIN_RE.findall(text)
    out = []
    for d in found:
        d = d.lower()
        if d.startswith("www."):
            d = d[4:]
        out.append(d)
    return sorted(set(out))


# -------------------- 3) SCORING (semplice ma “serio”) --------------------

KEYWORDS_POS = {
    "raises": 18, "raised": 18, "funding": 14, "seed": 10, "series": 10,
    "acquires": 16, "acquired": 16, "launches": 10, "launched": 10,
    "secures": 14, "secured": 14, "lands": 12, "landed": 12,
    "partners": 8, "partnership": 8,
}

KEYWORDS_NEG = {
    "opinion": -8, "podcast": -6, "newsletter": -6, "how to": -8,
    "analysis": -4, "explainer": -6,
}

def compute_score(title: str, summary: str, domains: list[str]) -> int:
    text = f"{title} {summary}".lower()
    score = 50

    for k, w in KEYWORDS_POS.items():
        if k in text:
            score += w
    for k, w in KEYWORDS_NEG.items():
        if k in text:
            score += w

    # segnale forte: presenza domini in testo/summary
    if domains:
        score += 12
        if any(d.endswith(".info") for d in domains):
            score += 4

    return max(0, min(100, score))


# -------------------- 4) UPSERT ROBUSTO --------------------

def generate_external_id(link: str) -> str:
    return hashlib.md5((link or "").encode("utf-8")).hexdigest()

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def upsert_startup(row: dict) -> tuple[int, str]:
    # UNA sola volta: on_conflict=external_id
    url = f"{TABLE_URL}?on_conflict=external_id"
    r = requests.post(url, json=row, headers=HEADERS, timeout=20)
    return r.status_code, r.text


# -------------------- MAIN --------------------

def process():
    total = 0
    kept = 0

    for feed_url in FEEDS:
        feed = feedparser.parse(feed_url)

        for entry in feed.entries[:15]:
            title = clean_text(getattr(entry, "title", "") or "")
            link = getattr(entry, "link", "") or ""
            summary = clean_text(getattr(entry, "summary", "") or "")

            total += 1

            name = extract_company_name(title)
            if not name:
                continue  # rumore

            domains = set()
            domains.update(extract_domains(title))
            domains.update(extract_domains(summary))

            host = get_domain_from_url(link)
            if host:
                domains.add(host)

            domains = sorted(d for d in domains if d and "." in d)

            rank_score = compute_score(title, summary, domains)

            row = {
                "external_id": generate_external_id(link),
                "name": name,
                "raw_title": title[:400],
                "description": summary[:400],
                "source_url": link,
                "source_type": "WebScan",
                "rank_score": rank_score,
                "tm_risk": "MED",
                "dom_risk": "MED",
                "verification_status": "UNVERIFIED",
                "confidence": 0,
                "created_at": utc_now_iso(),
            }

            code, _ = upsert_startup(row)
            print(code, name, "| score:", rank_score, "| domains:", (domains[:3] if domains else []))

            kept += 1
            time.sleep(0.2)

    print("DONE. total_seen:", total, "kept:", kept)


if __name__ == "__main__":
    process()
