import json
import os
from decimal import Decimal
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from config import DYNAMO_DIR, CATALOG_WITH_IMAGES_FILE
from utils.log import info, warn, error, done

load_dotenv()

TABLE_NAME = os.getenv("DYNAMO_TABLE_NAME")
AWS_REGION = os.getenv("AWS_REGION")

if not TABLE_NAME:
    raise SystemExit("DYNAMO_TABLE_NAME environment variable is not set")

if not AWS_REGION:
    raise SystemExit("AWS_REGION environment variable is not set")

INPUT_FILE = DYNAMO_DIR / CATALOG_WITH_IMAGES_FILE


def import_items():
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(TABLE_NAME)

    total = 0
    ok = 0
    failed = 0

    try:
        with table.batch_writer(overwrite_by_pkeys=["PK", "SK"]) as batch:
            with INPUT_FILE.open("r", encoding="utf-8") as f:

                for line_no, line in enumerate(f, start=1):
                    line = line.strip()

                    if not line:
                        continue

                    total += 1

                    try:
                        item = json.loads(line, parse_float=Decimal)
                    except json.JSONDecodeError:
                        warn(f"line {line_no}: invalid JSON, skipping")
                        failed += 1
                        continue

                    if "PK" not in item or "SK" not in item:
                        warn(f"line {line_no}: missing PK/SK, skipping")
                        failed += 1
                        continue

                    batch.put_item(Item=item)
                    ok += 1

                    if ok % 100 == 0:
                        info(
                            f"imported {ok} items (total lines processed: {total})")

    except (BotoCoreError, ClientError) as e:
        error(f"DynamoDB error: {e}")
        return

    done()
    info(f"Lines processed: {total}")
    info(f"Items imported: {ok}")
    info(f"Items skipped: {failed}")


if __name__ == "__main__":
    import_items()
