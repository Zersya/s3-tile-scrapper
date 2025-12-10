# Indonesia Tile Scraper

High-performance tile scraper for downloading and uploading map tiles to AWS S3 for Indonesia region.

## Features

- ✅ Complete Indonesia coverage (Sabang to Papua)
- ✅ Configurable zoom levels (2-16)
- ✅ Parallel downloads with thread pooling
- ✅ Progress tracking with tqdm
- ✅ Automatic retry on failures
- ✅ Speed benchmarking and time estimation
- ✅ Skip existing tiles (resume capability)
- ✅ Docker containerized

## Prerequisites

- Python 3.13+ or Docker
- AWS credentials with S3 access
- Source tile URL pattern

## Configuration

Create a `.env` file:

```env
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_S3_BUCKET=your_bucket_name
SOURCE_URL_PATTERN=https://source.example.com/tiles/{z}/{x}/{y}.png
```

## Usage

### Option 1: Docker (Recommended)

#### Build and run with docker-compose:

```bash
# Dry run (estimate only)
docker-compose run --rm tile-scrapper --dry-run --max-zoom 14

# Run scraper
docker-compose up

# Run with custom arguments
docker-compose run --rm tile-scrapper --max-zoom 12 --workers 30 --no-confirm
```

#### Build and run with docker:

```bash
# Build image
docker build -t tile-scrapper .

# Dry run
docker run --rm --env-file .env tile-scrapper --dry-run --max-zoom 14

# Run scraper
docker run --rm --env-file .env -v $(pwd)/logs:/app/logs tile-scrapper --max-zoom 14

# Run in background
docker run -d --name scrapper --env-file .env -v $(pwd)/logs:/app/logs tile-scrapper --max-zoom 14 --no-confirm
```

### Option 2: Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Dry run (estimate only)
python tile-scrapper.py --dry-run --max-zoom 14

# Run scraper
python tile-scrapper.py --max-zoom 14
```

## Command Line Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--min-zoom` | 2 | Minimum zoom level |
| `--max-zoom` | 16 | Maximum zoom level |
| `--workers` | 20 | Number of concurrent threads |
| `--dry-run` | false | Only show estimation, don't scrape |
| `--no-confirm` / `-y` | false | Skip confirmation prompt |

## Examples

### Estimate tile count for zoom 2-14:
```bash
python tile-scrapper.py --dry-run --max-zoom 14
```

### Scrape zoom 2-12 with 30 workers:
```bash
python tile-scrapper.py --max-zoom 12 --workers 30
```

### Run without confirmation (automation):
```bash
python tile-scrapper.py --max-zoom 14 -y
# or
python tile-scrapper.py --max-zoom 14 --no-confirm
```

## Tile Estimates

| Zoom Range | Total Tiles | Est. Size |
|------------|-------------|-----------|
| 2-11 | 36,413 | 533 MB |
| 2-12 | 143,948 | 2.1 GB |
| 2-13 | 572,018 | 8.4 GB |
| 2-14 | 2,286,158 | 32.7 GB |
| 2-15 | 9,141,116 | 130 GB |
| 2-16 | 36,549,187 | 523 GB |

## Monitoring

### Logs
- Console: Real-time progress bar
- File: `missing_tiles.log` (404s and errors)

### Docker logs
```bash
# Follow logs
docker logs -f scrapper

# docker-compose logs
docker-compose logs -f
```

## Stopping

### Docker
```bash
docker stop scrapper
# or
docker-compose down
```

### Python
Press `Ctrl+C` to gracefully stop

## Resume Capability

The scraper automatically skips tiles that already exist in S3, so you can safely restart it to resume interrupted downloads.

## Performance Tips

- Increase `--workers` for faster downloads (test optimal value for your network)
- Use `--max-zoom 14` for balanced detail/size (32 GB)
- For very large scrapes (zoom 15+), consider running on a cloud VM closer to S3 region
- Monitor S3 costs (requests + storage)

## Troubleshooting

**"Could not benchmark speed"**: Check AWS credentials and source URL

**Slow performance**: Increase workers or check network bandwidth

**High memory usage**: Reduce workers or add Docker memory limits

## License

MIT
