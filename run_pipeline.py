import subprocess
import sys

from utils.log import info, warn, done


STEPS = {
    "download_ids": "scripts.tmdb.download_ids",
    "filter_ids": "scripts.tmdb.filter_ids",
    "fetch_metadata": "scripts.tmdb.fetch_metadata",
    "build_catalog": "scripts.catalog.build_catalog",
    "download_images": "scripts.images.download_images",
    "upload_s3": "scripts.images.upload_s3",
    "load_dynamodb": "scripts.catalog.load_dynamodb",
}

PIPELINE_ORDER = [
    "download_ids",
    "filter_ids",
    "fetch_metadata",
    "build_catalog",
    "download_images",
    "upload_s3",
    "load_dynamodb",
]

AWS_STEPS = {"upload_s3", "load_dynamodb"}


def run_script(step):
    module = STEPS[step]

    info(f"Step: {step}")
    subprocess.run([sys.executable, "-m", module], check=True)
    print()


def confirm_aws_upload():
    warn("The next steps will upload data to AWS:")
    warn("  - upload images to S3")
    warn("  - import catalog into DynamoDB")

    answer = input("Continue with AWS upload? (y/N): ").strip().lower()

    if answer not in ("y", "yes"):
        warn("Aborted before AWS upload.")
        sys.exit(0)


def main():
    if len(sys.argv) < 2:
        print("Usage: python run_pipeline.py <step>")
        print("\nAvailable steps:")

        for step in STEPS:
            print(f"  - {step}")

        print("  - all")
        return

    step = sys.argv[1]

    if step == "all":
        aws_confirmed = False

        for s in PIPELINE_ORDER:

            if s in AWS_STEPS and not aws_confirmed:
                confirm_aws_upload()
                aws_confirmed = True

            run_script(s)

        done("Pipeline completed.")
        return

    if step not in STEPS:
        warn(f"Unknown step: {step}")
        return

    run_script(step)


if __name__ == "__main__":
    main()
