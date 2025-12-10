import math
import requests
import boto3
import logging
import concurrent.futures
import os
import sys
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
from tqdm import tqdm
import time

# --- CONFIGURATION ---
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

# --- SCRAPER CONFIGURATION ---
SOURCE_URL_PATTERN = os.getenv("SOURCE_URL_PATTERN")
DESTINATION_PREFIX = "raster"

# Indonesia Complete Bounding Box (extended to cover all territories)
# Includes: Sabang (Aceh), Papua, Rote Island, Sangihe-Talaud Islands
# Order: West (Min Lon), South (Min Lat), East (Max Lon), North (Max Lat)
BOUNDING_BOX = [94.5, -11.5, 141.5, 6.0]

# Zoom levels to scrape (extended for detailed tiles)
# Zoom 2-11: Overview tiles
# Zoom 12-14: Detailed coastal/marine tiles
# Zoom 15-16: Very detailed tiles (use with caution - millions of tiles)
MIN_ZOOM = 2
MAX_ZOOM = 16  # Increase this for more detail (WARNING: exponential growth)

# Number of concurrent threads
MAX_WORKERS = 20  # Increased for faster scraping

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# Statistics (thread-safe)
import threading
stats_lock = threading.Lock()
stats = {
    "success": 0,
    "skipped": 0,
    "failed": 0,
    "not_found": 0
}

# Global args
NO_CONFIRM = False

# --- LOGGING SETUP ---
logging.basicConfig(
    filename='missing_tiles.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- S3 CLIENT SETUP ---
s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def deg2num(lat_deg, lon_deg, zoom):
    """
    Converts Lat/Lon to Tile X/Y coordinates for a specific zoom level.
    Based on Web Mercator projection.
    """
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return (xtile, ytile)

def check_exists(s3_key):
    """
    Checks if the file already exists in S3 to avoid re-uploading.
    Returns True if exists, False otherwise.
    """
    try:
        s3_client.head_object(Bucket=AWS_S3_BUCKET, Key=s3_key)
        return True
    except ClientError as e:
        # If error code is 404, it means the object does not exist
        if e.response['Error']['Code'] == "404":
            return False
        # If it's another error (e.g. 403 Forbidden), re-raise or log it
        print(f"[WARNING] S3 Check Error for {s3_key}: {e}")
        return False

def update_stats(key):
    """Thread-safe stats update."""
    with stats_lock:
        stats[key] += 1

def upload_tile(z, x, y, pbar=None):
    """
    Downloads a single tile and uploads it to S3 with retry logic.
    """
    url = SOURCE_URL_PATTERN.format(z=z, x=x, y=y)
    s3_key = f"{DESTINATION_PREFIX}/{z}/{x}/{y}.png"

    if check_exists(s3_key):
        update_stats("skipped")
        if pbar:
            pbar.update(1)
        return "skipped"

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, stream=True, timeout=15)
            
            if response.status_code == 200:
                content = response.content
                if len(content) > 0:
                    s3_client.put_object(
                        Bucket=AWS_S3_BUCKET,
                        Key=s3_key,
                        Body=content,
                        ContentType='image/png'
                    )
                    update_stats("success")
                    if pbar:
                        pbar.update(1)
                    return "success"
                else:
                    logging.warning(f"Empty content: {z}/{x}/{y}")
                    
            elif response.status_code == 404:
                logging.info(f"404 Not Found: {z}/{x}/{y}")
                update_stats("not_found")
                if pbar:
                    pbar.update(1)
                return "not_found"
            else:
                logging.warning(f"HTTP {response.status_code}: {z}/{x}/{y}")
                
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout (attempt {attempt+1}): {z}/{x}/{y}")
        except Exception as e:
            logging.error(f"Exception (attempt {attempt+1}): {z}/{x}/{y} - {str(e)}")
        
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)

    update_stats("failed")
    logging.error(f"Failed after {MAX_RETRIES} attempts: {z}/{x}/{y}")
    if pbar:
        pbar.update(1)
    return "failed"

def calculate_tile_count(min_zoom, max_zoom, bbox):
    """Calculate total number of tiles for given zoom range and bounding box."""
    total = 0
    details = []
    min_lon, min_lat, max_lon, max_lat = bbox
    
    for z in range(min_zoom, max_zoom + 1):
        x_min, y_min = deg2num(max_lat, min_lon, z)
        x_max, y_max = deg2num(min_lat, max_lon, z)
        
        start_x, end_x = min(x_min, x_max), max(x_min, x_max)
        start_y, end_y = min(y_min, y_max), max(y_min, y_max)
        
        count = (end_x - start_x + 1) * (end_y - start_y + 1)
        total += count
        details.append((z, count, start_x, end_x, start_y, end_y))
    
    return total, details

def estimate_size(tile_count, avg_tile_size_kb=15):
    """Estimate total download size."""
    size_mb = (tile_count * avg_tile_size_kb) / 1024
    size_gb = size_mb / 1024
    return size_mb, size_gb

def benchmark_speed(num_samples=10):
    """
    Benchmark download+upload speed by testing a few sample tiles.
    Returns average time per tile in seconds.
    """
    print(f"\nBenchmarking speed with {num_samples} sample tiles...")
    
    # Sample tiles from different zoom levels
    sample_tiles = []
    min_lon, min_lat, max_lon, max_lat = BOUNDING_BOX
    
    for z in [8, 10, 12]:  # Test different zoom levels
        x_min, y_min = deg2num(max_lat, min_lon, z)
        x_max, y_max = deg2num(min_lat, max_lon, z)
        mid_x = (x_min + x_max) // 2
        mid_y = (y_min + y_max) // 2
        sample_tiles.append((z, mid_x, mid_y))
        sample_tiles.append((z, mid_x + 1, mid_y))
        sample_tiles.append((z, mid_x, mid_y + 1))
        if len(sample_tiles) >= num_samples:
            break
    
    sample_tiles = sample_tiles[:num_samples]
    times = []
    
    for z, x, y in sample_tiles:
        url = SOURCE_URL_PATTERN.format(z=z, x=x, y=y)
        s3_key = f"{DESTINATION_PREFIX}/{z}/{x}/{y}.png"
        
        start = time.time()
        try:
            # Download
            response = requests.get(url, stream=True, timeout=15)
            if response.status_code == 200:
                content = response.content
                # Upload to S3
                s3_client.put_object(
                    Bucket=AWS_S3_BUCKET,
                    Key=s3_key,
                    Body=content,
                    ContentType='image/png'
                )
            elapsed = time.time() - start
            times.append(elapsed)
            print(f"  Sample {len(times)}/{num_samples}: {elapsed:.3f}s")
        except Exception as e:
            print(f"  Sample failed: {e}")
            continue
    
    if not times:
        return None
    
    avg_time = sum(times) / len(times)
    return avg_time

def format_duration(seconds):
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    elif seconds < 86400:
        hours = seconds / 3600
        return f"{hours:.1f} hours"
    else:
        days = seconds / 86400
        hours = (seconds % 86400) / 3600
        return f"{days:.0f} days {hours:.0f} hours"

def estimate_time(total_tiles, avg_time_per_tile, workers):
    """Estimate total time considering parallel workers."""
    # Effective time = (total_tiles * avg_time) / workers
    # Add 20% overhead for thread scheduling and S3 existence checks
    effective_time = (total_tiles * avg_time_per_tile) / workers * 1.2
    return effective_time

def main():
    global stats
    
    if not AWS_ACCESS_KEY_ID or not AWS_S3_BUCKET:
        print("Error: AWS credentials not found. Please check your .env file.")
        return

    print("=" * 60)
    print("INDONESIA TILE SCRAPER - Enhanced Version")
    print("=" * 60)
    print(f"Source: {SOURCE_URL_PATTERN}")
    print(f"Target: s3://{AWS_S3_BUCKET}/{DESTINATION_PREFIX}/")
    print(f"Bounding Box: {BOUNDING_BOX}")
    print(f"Zoom Range: {MIN_ZOOM} to {MAX_ZOOM}")
    print("-" * 60)
    
    total_tiles, details = calculate_tile_count(MIN_ZOOM, MAX_ZOOM, BOUNDING_BOX)
    est_mb, est_gb = estimate_size(total_tiles)
    
    print("\nTile Count by Zoom Level:")
    for z, count, sx, ex, sy, ey in details:
        print(f"  Zoom {z:2d}: {count:>10,} tiles  (X: {sx}-{ex}, Y: {sy}-{ey})")
    
    print("-" * 60)
    print(f"TOTAL TILES: {total_tiles:,}")
    print(f"Estimated Size: {est_mb:,.0f} MB ({est_gb:.2f} GB)")
    print("-" * 60)
    
    # Benchmark to estimate time
    avg_time = benchmark_speed(num_samples=10)
    if avg_time:
        estimated_seconds = estimate_time(total_tiles, avg_time, MAX_WORKERS)
        print(f"\nBenchmark Results:")
        print(f"  Avg time per tile: {avg_time:.3f}s")
        print(f"  Workers: {MAX_WORKERS}")
        print(f"  Estimated total time: {format_duration(estimated_seconds)}")
    else:
        print("\nWarning: Could not benchmark speed (check network/credentials)")
    print("-" * 60)
    
    if not NO_CONFIRM:
        confirm = input("\nProceed with scraping? (yes/no): ").strip().lower()
        if confirm not in ['yes', 'y']:
            print("Aborted by user.")
            return
    
    print("\nStarting scrape...")
    start_time = time.time()
    
    with tqdm(total=total_tiles, desc="Scraping tiles", unit="tile") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            
            for z in range(MIN_ZOOM, MAX_ZOOM + 1):
                min_lon, min_lat, max_lon, max_lat = BOUNDING_BOX
                
                x_min, y_min = deg2num(max_lat, min_lon, z)
                x_max, y_max = deg2num(min_lat, max_lon, z)

                start_x, end_x = min(x_min, x_max), max(x_min, x_max)
                start_y, end_y = min(y_min, y_max), max(y_min, y_max)

                for x in range(start_x, end_x + 1):
                    for y in range(start_y, end_y + 1):
                        futures.append(executor.submit(upload_tile, z, x, y, pbar))

            concurrent.futures.wait(futures)
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("SCRAPE COMPLETE")
    print("=" * 60)
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"Success:   {stats['success']:,}")
    print(f"Skipped:   {stats['skipped']:,}")
    print(f"Not Found: {stats['not_found']:,}")
    print(f"Failed:    {stats['failed']:,}")
    print("=" * 60)

def parse_args():
    """Parse command line arguments."""
    import argparse
    parser = argparse.ArgumentParser(description='Indonesia Tile Scraper')
    parser.add_argument('--min-zoom', type=int, default=MIN_ZOOM,
                        help=f'Minimum zoom level (default: {MIN_ZOOM})')
    parser.add_argument('--max-zoom', type=int, default=MAX_ZOOM,
                        help=f'Maximum zoom level (default: {MAX_ZOOM})')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS,
                        help=f'Number of concurrent workers (default: {MAX_WORKERS})')
    parser.add_argument('--dry-run', action='store_true',
                        help='Only show tile count estimation, do not scrape')
    parser.add_argument('--no-confirm', '-y', action='store_true',
                        help='Skip confirmation prompt')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    MIN_ZOOM = args.min_zoom
    MAX_ZOOM = args.max_zoom
    MAX_WORKERS = args.workers
    NO_CONFIRM = args.no_confirm
    
    if args.dry_run:
        total_tiles, details = calculate_tile_count(MIN_ZOOM, MAX_ZOOM, BOUNDING_BOX)
        est_mb, est_gb = estimate_size(total_tiles)
        print(f"Bounding Box: {BOUNDING_BOX}")
        print(f"Zoom Range: {MIN_ZOOM} to {MAX_ZOOM}")
        print("\nTile Count by Zoom Level:")
        for z, count, sx, ex, sy, ey in details:
            print(f"  Zoom {z:2d}: {count:>10,} tiles  (X: {sx}-{ex}, Y: {sy}-{ey})")
        print(f"\nTOTAL: {total_tiles:,} tiles")
        print(f"Estimated Size: {est_mb:,.0f} MB ({est_gb:.2f} GB)")
    else:
        main()