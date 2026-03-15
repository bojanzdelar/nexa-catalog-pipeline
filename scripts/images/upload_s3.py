import boto3
import os
from pathlib import Path
import mimetypes
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import IMAGE_DIR, MAX_WORKERS
from utils.log import info, warn, progress, done

load_dotenv()

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_REGION")

if not BUCKET_NAME:
    raise SystemExit("S3_BUCKET_NAME environment variable is not set")

if not REGION:
    raise SystemExit("AWS_REGION environment variable is not set")


s3 = boto3.client("s3", region_name=REGION)


def upload_file(local_path: Path):
    key = str(local_path.relative_to(IMAGE_DIR)).replace("\\", "/")

    content_type, _ = mimetypes.guess_type(local_path.name)
    content_type = content_type or "application/octet-stream"

    try:
        s3.upload_file(
            str(local_path),
            BUCKET_NAME,
            key,
            ExtraArgs={
                "ContentType": content_type,
                "CacheControl": "public, max-age=31536000, immutable",
            },
        )
        return ("ok", key)

    except Exception as e:
        return ("fail", key, str(e))


def main():

    info("Uploading images to S3")

    files = [p for p in IMAGE_DIR.rglob("*.*") if p.is_file()]
    total = len(files)

    info(f"Found {total} files")

    completed = 0
    ok = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [executor.submit(upload_file, f) for f in files]

        for future in as_completed(futures):

            result = future.result()
            completed += 1

            if result[0] == "ok":
                ok += 1
            else:
                fail += 1
                warn(f"Upload failed: {result[1]} | {result[2]}")

            if completed % 50 == 0 or completed == total:
                progress(completed, total, f"uploaded={ok} failed={fail}")

    info("Upload complete")
    done()


if __name__ == "__main__":
    main()
