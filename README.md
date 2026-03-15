# Nexa Catalog Pipeline

Catalog ingestion pipeline for Nexa. Fetches TMDB metadata, builds a content catalog, processes images, and loads the dataset into DynamoDB and S3.

## Pipeline overview

The pipeline performs the following steps:

1. Download TMDB ID exports
2. Filter titles by popularity
3. Fetch full metadata from TMDB
4. Build the catalog dataset (DynamoDB format)
5. Download and process image assets
6. Upload images to S3
7. Import titles into DynamoDB

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

## Environment variables

Create a `.env` file in the project root:

```env
TMDB_TOKEN=your_tmdb_read_token
TMDB_API_KEY=your_tmdb_api_key
S3_BUCKET_NAME=your_s3_bucket_name
DYNAMO_TABLE_NAME=your_dynamodb_table_name
AWS_REGION=your_aws_region
```

## Configuration

Some aspects of the pipeline can be tuned via values in `config.py`.

### Dataset filtering

Controls which titles are included in the catalog.

```python
POPULARITY_THRESHOLD = 8.0
MOVIE_MIN_VOTES = 1500
TV_MIN_VOTES = 500
LANG = "en"
```

- `POPULARITY_THRESHOLD` – minimum TMDB popularity score
- `MOVIE_MIN_VOTES` – minimum number of votes required for movies
- `TV_MIN_VOTES` – minimum number of votes required for TV shows
- `LANG` – filter titles by original language

## Running the pipeline

The pipeline can be executed step-by-step or run end-to-end using `run_pipeline.py`.

### Run the full pipeline

```bash
python run_pipeline.py all
```

Before the AWS steps run, the script will ask for confirmation because it will:

- upload images to S3
- import the catalog into DynamoDB

### Run individual steps

You can run specific steps if needed:

```bash
python run_pipeline.py download_ids
python run_pipeline.py filter_ids
python run_pipeline.py fetch_metadata
python run_pipeline.py build_catalog
python run_pipeline.py download_images
python run_pipeline.py upload_s3
python run_pipeline.py load_dynamodb
```

## License

This project is licensed under the **MIT License**.
