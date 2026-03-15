import json

from config import (
    RAW_DIR,
    FILTERED_DIR,
    POPULARITY_THRESHOLD,
    MOVIES_FILTERED_FILE,
    TV_FILTERED_FILE,
)

from utils.log import info, done


FILES = {
    "movies": "movie_ids_*.json",
    "tv": "tv_series_ids_*.json",
}


def main():

    for content_type, pattern in FILES.items():

        input_file = sorted(RAW_DIR.glob(pattern))[-1]

        filename = MOVIES_FILTERED_FILE if content_type == "movies" else TV_FILTERED_FILE
        output_file = FILTERED_DIR / filename

        info(f"Filtering {input_file} → {output_file}")

        with open(input_file, "r", encoding="utf-8") as fin, \
                open(output_file, "w", encoding="utf-8") as fout:

            for line in fin:

                line = line.strip()
                if not line:
                    continue

                obj = json.loads(line)

                if obj.get("popularity", 0) <= POPULARITY_THRESHOLD:
                    continue

                fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

        info(f"Saved {output_file}")

    done()


if __name__ == "__main__":
    main()
