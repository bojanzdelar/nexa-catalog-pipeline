from datetime import datetime
import shutil
import gzip
import requests

from config import RAW_DIR
from constants import TMDB_EXPORT_BASE_URL
from utils.log import info, done


def main():
    today = datetime.now().strftime("%m_%d_%Y")

    files = {
        "movies": f"movie_ids_{today}.json.gz",
        "tv": f"tv_series_ids_{today}.json.gz",
    }

    for name, filename in files.items():

        url = f"{TMDB_EXPORT_BASE_URL}/{filename}"

        gz_path = RAW_DIR / filename
        json_path = RAW_DIR / filename.replace(".gz", "")

        info(f"Downloading {url}")

        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(gz_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        info(f"Extracting {filename}")

        with gzip.open(gz_path, "rb") as f_in:
            with open(json_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        gz_path.unlink()

        info(f"Saved to {json_path}")

    done()


if __name__ == "__main__":
    main()
