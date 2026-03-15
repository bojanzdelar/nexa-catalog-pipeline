import json
import requests
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

from config import (
    DYNAMO_DIR,
    IMAGE_DIR,
    CATALOG_FILE,
    CATALOG_WITH_IMAGES_FILE,
    MAX_WORKERS
)

from constants import TMDB_IMAGE_BASE_URL
from utils.log import info, warn, progress, done


INPUT_FILE = DYNAMO_DIR / CATALOG_FILE
OUTPUT_FILE = DYNAMO_DIR / CATALOG_WITH_IMAGES_FILE


session = requests.Session()
stats = Counter()
failed = []


FOLDERS = {
    "poster": IMAGE_DIR / "posters",
    "backdrop": IMAGE_DIR / "backdrops",
    "thumbnail": IMAGE_DIR / "thumbnails",
    "profile": IMAGE_DIR / "profiles",
}


def tmdb_url(size, path):
    return urljoin(TMDB_IMAGE_BASE_URL, f"{size}{path}")


def local_path(folder, filename):
    return f"/{folder}/{filename}"


def filename_from_path(path):
    return path.split("/")[-1]


def download_task(url, dest):
    try:
        if dest.exists():
            stats["skipped"] += 1
            return

        r = session.get(url, timeout=20)
        r.raise_for_status()

        dest.write_bytes(r.content)
        stats["downloaded"] += 1

    except Exception as e:
        stats["failed"] += 1
        failed.append((url, str(e)))


def main():

    for p in FOLDERS.values():
        p.mkdir(parents=True, exist_ok=True)

    download_jobs = set()
    updated_lines = []
    meta_images = {}

    with INPUT_FILE.open("r", encoding="utf-8") as infile:

        for line in infile:

            item = json.loads(line)
            sk = item.get("SK", "")

            if sk == "META":

                key = (item["type"], item["id"])

                poster = item.get("poster_path")
                backdrop = item.get("backdrop_path")

                meta_images[key] = (poster, backdrop)

                if poster:
                    filename = filename_from_path(poster)
                    dest = FOLDERS["poster"] / filename

                    download_jobs.add((tmdb_url("w300", poster), dest))
                    item["poster_path"] = local_path("posters", filename)

                if backdrop:
                    filename = filename_from_path(backdrop)
                    dest = FOLDERS["backdrop"] / filename

                    download_jobs.add((tmdb_url("original", backdrop), dest))
                    item["backdrop_path"] = local_path("backdrops", filename)

            elif sk == "CREDITS":

                for cast in item.get("cast", []):

                    if cast.get("profile_path"):

                        filename = filename_from_path(cast["profile_path"])
                        dest = FOLDERS["profile"] / filename

                        download_jobs.add(
                            (tmdb_url("w300", cast["profile_path"]), dest)
                        )
                        cast["profile_path"] = local_path("profiles", filename)

            elif sk.startswith("SEASON#"):

                for ep in item.get("episodes", []):

                    if ep.get("still_path"):

                        filename = filename_from_path(ep["still_path"])
                        dest = FOLDERS["thumbnail"] / filename

                        download_jobs.add(
                            (tmdb_url("w300", ep["still_path"]), dest)
                        )
                        ep["still_path"] = local_path("thumbnails", filename)

            elif sk == "RECOMMENDATIONS":

                for rec in item.get("results", []):

                    if rec.get("poster_path"):

                        filename = filename_from_path(rec["poster_path"])
                        dest = FOLDERS["poster"] / filename

                        download_jobs.add(
                            (tmdb_url("w300", rec["poster_path"]), dest)
                        )
                        rec["poster_path"] = local_path("posters", filename)

            if item.get("PK", "").startswith("CAT#"):

                key = (item["type"], item["id"])

                if key in meta_images:

                    poster, backdrop = meta_images[key]

                    if poster:
                        item["poster_path"] = local_path(
                            "posters", filename_from_path(poster)
                        )

                    if backdrop:
                        item["backdrop_path"] = local_path(
                            "backdrops", filename_from_path(backdrop)
                        )

            updated_lines.append(json.dumps(item, ensure_ascii=False))

    download_jobs = list(download_jobs)
    total = len(download_jobs)

    info(f"Starting {total} downloads")

    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [
            executor.submit(download_task, url, dest)
            for url, dest in download_jobs
        ]

        for _ in as_completed(futures):

            completed += 1

            if completed % 25 == 0 or completed == total:
                progress(
                    completed,
                    total,
                    f"downloaded={stats['downloaded']} skipped={stats['skipped']} failed={stats['failed']}",
                )

    info(f"Downloaded: {stats['downloaded']}")
    info(f"Skipped: {stats['skipped']}")
    info(f"Failed: {stats['failed']}")

    if failed:
        warn("Failed URLs (first 20):")
        for url, err in failed[:20]:
            warn(f"{url} | {err}")

    with OUTPUT_FILE.open("w", encoding="utf-8") as out:
        out.write("\n".join(updated_lines))

    info(f"JSON written to {OUTPUT_FILE}")
    done()


if __name__ == "__main__":
    main()
