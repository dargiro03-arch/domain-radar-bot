import os
import re
import time
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import feedparser


SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

TABLE_URL = f"{SUPABASE_URL}/rest/v1/startups?on_conflict=external_id"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    # IMPORTANT: upsert + on_conflict (lavora con UNIQUE su external_id)
   "Prefer": "resolution=merge-duplicates,return=minimal"
}

FEEDS = [
    "https://techcrunch.com/feed/",
    "https://sifted.eu/feed/",
    "https://www.eu-startups.com/feed/"
]

# --- 1) FILTRI SERI ---------------------------------------------------------

GEO = {
    "UK","US","EU","UAE","USA","Europe","European","London","Paris","Berlin","Germany","German",
    "France","French","Spain","Spanish","Italy","Italian","China","India","World","Global"
}

NOISE = {
    "AI","This","That","These","Those","Ask","Are","How","Why","What","When","Where","Who",
    "Ex","Breaking","Report","Opinion","Analysis","Startup","Startups"
}

BAD_LOWER = {
    "startup","startups","company","companies","firm","fund","funding","raises","raise","raised",
    "round","seed","series","pre-seed","deal","launch","launches","announces","says","adds",
    "report","interview","podcast","newsletter"
}

ORG_SUFFIX = {
    "Inc","Ltd","PLC","LLC","GmbH","SAS","Srl","Srls","BV","AB","Oy","SA","AG","NV"
}

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def token_ok(w: str) -> bool:
    if not w or len(w) < 3:
        return False
    if any(ch.isdigit() for ch in w):
        return False
    if w in GEO or w in NOISE:
        return False
    if w.lower() in BAD_LOWER:
        return False
    # evita parole tutte maiuscole tipo "CEO" o "IPO"
    if w.isupper() and len(w) <= 4:
        return False
    return True

def extract_company_name(title: str) -> str | None:
    """
    Estrae un nome azienda "realistico":
    - cattura sequenze di parole Capitalized (anche multi-word: "Open AI" => "Open AI")
    - scarta geografie/noise
    - preferisce pattern tipo "X raises", "X acquires", "X launches", "X’s"
    """
    t = normalize_spaces(title.replace("’", "'"))

    # Pattern forti: "Name's ..." oppure "Name raises ..."
    strong = [
        r"^([A-Z][A-Za-z]+(?:\s[A-Z][A-Za-z]+){0,2})'s\b",
        r"^([A-Z][A-Za-z]+(?:\s[A-Z][A-Za-z]+){0,2})\s+(raises|raised|acquires|acquired|buys|bought|launches|launched|announces|announced|secures|secured|lands|landed)\b",
    ]
    for pat in strong:
        m = re.search(pat, t)
        if m:
            cand = m.group(1)
            parts = cand.split()
            if all(token_ok(p) for p in parts):
                return cand

    # Fallback: prendi tutte le sequenze Capitalized (1-3 parole), scegli la migliore
    seqs = re.findall(r"\b([A-Z][A-Za-z]+(?:\s[A-Z][A-Za-z]+){0,2})\b", t)
    seqs = [normalize_spaces(s) for s in seqs]

    scored = []
    for s in seqs[:12]:
        parts = s.split()
        if not parts:
            continue
        if any(p in ORG_SUFFIX for p in parts):
            parts = [p for p in parts if p not in ORG_SUFFIX]
            s = " ".join(parts)
        if not s:
            continue
        if not all(token_ok(p) for p in s.split()):
            continue
        # penalizza se è un singolo token troppo comune (tipo "World")
        score = 0
        score += 3 if len(s.split()) >= 2 else 0
        score += 2 if len(s) >= 6 else 0
        score += 2 if s == s.title() else 0
        scored.append((score, s))

    if not scored:
        return None

    scored.sort(reverse=True, key=lambda x: x[0])
    return scored[0][1]


# --- 2) ESTRAZIONE DOMINI ---------------------------------------------------

DOMAIN_RE = re.compile(
    r"\b((?:[a-zA-Z0-9-]+\.)+(?:com|io|ai|co|net|org|app|dev|info|eu|it|fr|de|es|uk|me|xyz))\b",
    re.IGNORECASE
)

def get_domain_from_url(url: str) -> str | None:
    try:
        host = urlparse(url).netloc.lower()
        host = host.split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host if host else None
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


# --- 3) SCORING “SERIO” (semplice ma utile) --------------------------------

KEYWORDS_POS = {
    "raises": 18, "raised": 18, "funding": 14, "seed": 10, "series": 10,
    "acquires": 16, "acquired": 16, "launches": 10, "launched": 10,
    "secures": 14, "secured": 14, "lands": 12, "landed": 12,
    "partners": 8, "partnership": 8
}

KEYWORDS_NEG = {
    "opinion": -8, "podcast": -6, "newsletter": -6, "how to": -8,
    "why": -4, "what": -4, "analysis": -4
}

def compute_score(title: str, summary: str, domains: list[str]) -> int:
    text = f"{title} {summary}".lower()
    score = 50  # base
    for k, w in KEYWORDS_POS.items():
        if k in text:
            score += w
    for k, w in KEYWORDS_NEG.items():
        if k in text:
            score += w

    # presenza domini = segnale forte
    if domains:
        score += 10
        # .info spesso “risky” (solo euristica)
        if any(d.endswith(".info") for d in domains):
            score += 4

    # clamp
    score = max(0, min(100, score))
    return score


# --- 4) UPSERT ROBUSTO ------------------------------------------------------

def generate_external_id(link: str) -> str:
    return hashlib.md5(link.encode("utf-8")).hexdigest()

def upsert_startup(row: dict) -> tuple[int, str]:
    # on_conflict richiede constraint UNIQUE su external_id
    url = f"{TABLE_URL}?on_conflict=external_id"
    r = requests.post(url, json=row, headers=HEADERS, timeout=20)
    return r.status_code, r.text

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def process():
    total = 0
    kept = 0

    for feed_url in FEEDS:
        feed = feedparser.parse(feed_url)

        for entry in feed.entries[:15]:
            title = getattr(entry, "title", "") or ""
            link = getattr(entry, "link", "") or ""
            summary = getattr(entry, "summary", "") or ""

            total += 1

            name = extract_company_name(title)
            if not name:
                # scarta rumore
                continue

            # domini: dal summary + title + link host
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
                "description": normalize_spaces(re.sub(r"<[^>]+>", "", summary))[:400],
                "source_url": link,
                "source_type": "WebScan",
                "rank_score": rank_score,
                "tm_risk": "MED",
                "dom_risk": "MED",
                "verification_status": "UNVERIFIED",
                "confidence": 0,
                "created_at": utc_now_iso(),
                # extra opzionale: salva lista domini in testo (se non hai colonna, commenta)
                # "domains": ",".join(domains)[:4000],
            }

            code, text = upsert_startup(row)
            print(code, name, "| score:", rank_score, "| domains:", (domains[:3] if domains else []))

            kept += 1
            time.sleep(0.2)  # gentilezza

    print("DONE. total_seen:", total, "kept:", kept)


if __name__ == "__main__":
    process()
