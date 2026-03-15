from pathlib import Path


# Filtering
POPULARITY_THRESHOLD = 8.0
MOVIE_MIN_VOTES = 1500
TV_MIN_VOTES = 500
LANG = "en"

# Catalog
MAX_CAST = 20
MAX_RECOMMENDATIONS = 20
EPISODE_OVERVIEW_MAX_LEN = 300

MOVIE_GENRES = {28, 18, 35, 53, 878, 12}
TV_GENRES = {18, 35, 80, 10765, 9648, 10759}

# API behavior
MAX_RETRIES = 6
SEASON_DELAY = 0.02

# Concurrency
MAX_WORKERS = 20

# Paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
FILTERED_DIR = DATA_DIR / "filtered"
ENRICHED_DIR = DATA_DIR / "enriched"
DYNAMO_DIR = DATA_DIR / "dynamo"
IMAGE_DIR = DATA_DIR / "images"

# Files
CATALOG_FILE = "catalog.jsonl"
CATALOG_WITH_IMAGES_FILE = "catalog_with_s3_images.jsonl"

MOVIE_DETAILS_FILE = "movie_details.jsonl"
TV_DETAILS_FILE = "tv_details.jsonl"
TV_SEASONS_FILE = "tv_seasons.jsonl"

MOVIES_FILTERED_FILE = "movies_filtered.json"
TV_FILTERED_FILE = "tv_filtered.json"

# Ensure directories exist
for directory in [RAW_DIR, FILTERED_DIR, ENRICHED_DIR, IMAGE_DIR, DYNAMO_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
