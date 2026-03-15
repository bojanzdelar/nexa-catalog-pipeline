import json
import time
import random
import os
from typing import Dict, Any, Optional, Iterable, Set
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from dotenv import load_dotenv

from config import (
    FILTERED_DIR,
    ENRICHED_DIR,
    MOVIE_MIN_VOTES,
    TV_MIN_VOTES,
    LANG,
    MOVIES_FILTERED_FILE,
    TV_FILTERED_FILE,
    MOVIE_DETAILS_FILE,
    TV_DETAILS_FILE,
    TV_SEASONS_FILE,
    MAX_RETRIES,
    MAX_WORKERS,
    SEASON_DELAY,
)

from constants import TMDB_API_BASE_URL
from utils.log import info, progress, done

load_dotenv()

MOVIE_IDS_FILE = FILTERED_DIR / MOVIES_FILTERED_FILE
TV_IDS_FILE = FILTERED_DIR / TV_FILTERED_FILE

OUT_DIR = ENRICHED_DIR

TMDB_TOKEN = os.getenv("TMDB_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

if not TMDB_TOKEN and not TMDB_API_KEY:
    raise SystemExit("Set TMDB_TOKEN or TMDB_API_KEY")

session = requests.Session()

adapter = requests.adapters.HTTPAdapter(
    pool_connections=50,
    pool_maxsize=50
)

session.mount("https://", adapter)


def tmdb_get(path: str, params: Optional[Dict[str, Any]] = None):

    url = f"{TMDB_API_BASE_URL}{path}"
    params = dict(params or {})
    headers = {"Accept": "application/json"}

    if TMDB_TOKEN:
        headers["Authorization"] = f"Bearer {TMDB_TOKEN}"
    else:
        params["api_key"] = TMDB_API_KEY

    for attempt in range(MAX_RETRIES):

        resp = session.get(url, headers=headers, params=params, timeout=30)

        if resp.status_code in (429, 500, 502, 503, 504):

            retry_after = resp.headers.get("Retry-After")

            sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else (
                2 ** attempt) + random.random()

            time.sleep(sleep_s)
            continue

        if resp.status_code == 404:
            return None

        resp.raise_for_status()
        return resp.json()

    return None


def write_jsonl(path: Path, obj: Dict[str, Any]):

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def iter_ids_from_jsonl(path: Path) -> Iterable[int]:

    with open(path, "r", encoding="utf-8") as f:
        for line in f:

            if not line.strip():
                continue

            obj = json.loads(line)
            tmdb_id = obj.get("id")

            if isinstance(tmdb_id, int):
                yield tmdb_id


def dedup_ids(ids: Iterable[int]) -> list[int]:

    seen: Set[int] = set()
    out = []

    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)

    return out


def fetch_movie(movie_id: int):

    details = tmdb_get(
        f"/movie/{movie_id}",
        params={"append_to_response": "credits,recommendations"},
    )

    if not details:
        return None

    if details.get("original_language") != LANG:
        return None

    if details.get("vote_count", 0) < MOVIE_MIN_VOTES:
        return None

    return {
        "id": movie_id,
        "details": details
    }


def fetch_tv(tv_id: int):

    details = tmdb_get(
        f"/tv/{tv_id}",
        params={"append_to_response": "credits,recommendations"},
    )

    if not details:
        return None

    if details.get("original_language") != LANG:
        return None

    if details.get("vote_count", 0) < TV_MIN_VOTES:
        return None

    seasons_payload = []

    for s in details.get("seasons", []):

        sn = s.get("season_number")

        if sn == 0:
            continue

        if isinstance(sn, int):

            data = tmdb_get(f"/tv/{tv_id}/season/{sn}")

            if data:
                seasons_payload.append({
                    "season_number": sn,
                    "data": data
                })

            time.sleep(SEASON_DELAY)

    return {
        "id": tv_id,
        "details": details,
        "seasons": seasons_payload,
    }


def main():

    movie_ids = dedup_ids(iter_ids_from_jsonl(MOVIE_IDS_FILE))
    tv_ids = dedup_ids(iter_ids_from_jsonl(TV_IDS_FILE))

    info(f"Loaded {len(movie_ids)} movies")
    info(f"Loaded {len(tv_ids)} tv shows")

    movie_details_out = OUT_DIR / MOVIE_DETAILS_FILE
    tv_details_out = OUT_DIR / TV_DETAILS_FILE
    tv_seasons_out = OUT_DIR / TV_SEASONS_FILE

    kept_movies = 0
    kept_tv = 0

    with ThreadPoolExecutor(MAX_WORKERS) as executor:

        futures = [executor.submit(fetch_movie, mid) for mid in movie_ids]

        for i, future in enumerate(as_completed(futures), 1):

            result = future.result()

            if result:

                kept_movies += 1

                write_jsonl(movie_details_out, {
                    "id": result["id"],
                    "data": result["details"]
                })

            if i % 25 == 0:
                progress(i, len(movie_ids), "movies processed")

    with ThreadPoolExecutor(MAX_WORKERS) as executor:

        futures = [executor.submit(fetch_tv, tid) for tid in tv_ids]

        for i, future in enumerate(as_completed(futures), 1):

            result = future.result()

            if result:

                kept_tv += 1

                write_jsonl(tv_details_out, {
                    "id": result["id"],
                    "data": result["details"]
                })

                for s in result["seasons"]:
                    write_jsonl(tv_seasons_out, {
                        "tv_id": result["id"],
                        "season_number": s["season_number"],
                        "data": s["data"]
                    })

            if i % 25 == 0:
                progress(i, len(tv_ids), "tv processed")

    print()
    info(f"Movies kept: {kept_movies}")
    info(f"TV kept: {kept_tv}")

    done()


if __name__ == "__main__":
    main()
