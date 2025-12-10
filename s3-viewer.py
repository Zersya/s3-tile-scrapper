import streamlit as st
import boto3
import os
import pandas as pd
from botocore.exceptions import ClientError
import folium
from streamlit_folium import st_folium
from dotenv import load_dotenv
import math


load_dotenv()

# --- Configuration & Setup ---
st.set_page_config(page_title="S3 Map & Data Viewer", layout="wide", page_icon="🗺️")

# Load environment variables
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

# --- AWS Client Initialization ---
@st.cache_resource
def get_s3_client():
    try:
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            return boto3.client(
                's3',
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
        else:
            return boto3.client('s3', region_name=AWS_REGION)
    except Exception as e:
        st.error(f"Failed to initialize S3 client: {e}")
        return None

s3 = get_s3_client()

# --- Helper Functions ---

def format_size(size_bytes):
    """Convert bytes to readable format."""
    if size_bytes == 0: return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

def get_presigned_url(bucket, key, expiration=3600):
    try:
        return s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiration
        )
    except ClientError:
        return None

def calculate_folder_stats(bucket, prefix):
    """
    Paginate through S3 to get total size and count.
    Warning: specific to 'prefix', can be slow for millions of files.
    """
    total_size = 0
    total_count = 0
    paginator = s3.get_paginator('list_objects_v2')
    
    # Show a spinner because this can take time
    with st.spinner(f"Calculating total size for '{prefix}'..."):
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_size += obj['Size']
                        total_count += 1
        except ClientError as e:
            st.error(f"Error calculating stats: {e}")
            return 0, 0
            
    return total_size, total_count

def infer_tile_url(bucket, region, prefix, example_key):
    """
    Attempt to construct the Z/X/Y template URL from a file key.
    Example Key: maps/layer1/5/10/20.png
    Target: https://bucket.s3.region.amazonaws.com/maps/layer1/{z}/{x}/{y}.png
    """
    # Base S3 URL
    base = f"https://{bucket}.s3.{region}.amazonaws.com"
    
    # Try to find extension
    ext = example_key.split('.')[-1]
    
    # Simple heuristic: remove the specific numbers and replace with {z}, {x}, {y}
    # This assumes standard layout: .../{z}/{x}/{y}.ext
    parts = example_key.split('/')
    if len(parts) >= 3:
        # Reconstruct path replacing last 3 parts with templates
        # This is a naive guesser, user might need to adjust
        parts[-3] = "{z}"
        parts[-2] = "{x}"
        parts[-1] = "{y}." + ext if not parts[-1].endswith("}") else "{y}"
        # Fix the extension part if it got messed up
        if not parts[-1].endswith(ext):
             parts[-1] = "{y}." + ext
        
        template_path = "/".join(parts)
        return f"{base}/{template_path}"
    
    return f"{base}/{prefix}{{z}}/{{x}}/{{y}}.{ext}"

# --- Main App Interface ---

st.title("🗺️ S3 Tile & Data Explorer")

if not s3:
    st.stop()

# Sidebar Setup
st.sidebar.header("Configuration")
bucket_options = []
if AWS_S3_BUCKET:
    bucket_options = [AWS_S3_BUCKET]
else:
    try:
        response = s3.list_buckets()
        bucket_options = [b['Name'] for b in response.get('Buckets', [])]
    except ClientError:
        st.sidebar.warning("Could not list buckets. Enter name manually.")

if not bucket_options:
    bucket_input = st.sidebar.text_input("Bucket Name")
    if bucket_input: bucket_options = [bucket_input]

selected_bucket = st.sidebar.selectbox("Select Bucket", bucket_options) if bucket_options else None
prefix = st.sidebar.text_input("Folder Prefix (e.g., 'maps/v1/')", "")

if selected_bucket:
    # --- Tabs for Data vs Map ---
    tab1, tab2 = st.tabs(["📂 File Explorer & Stats", "🌍 Map Preview"])

    # === TAB 1: FILE EXPLORER ===
    with tab1:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader(f"Contents: `{selected_bucket}/{prefix}`")
        with col2:
            if st.button("Calculate Total Size"):
                t_size, t_count = calculate_folder_stats(selected_bucket, prefix)
                st.metric("Total Size", format_size(t_size))
                st.metric("File Count", f"{t_count:,}")

        # List first 1000 files for the table view
        try:
            response = s3.list_objects_v2(Bucket=selected_bucket, Prefix=prefix, MaxKeys=1000)
            if 'Contents' in response:
                files = []
                for obj in response['Contents']:
                    files.append({
                        "Key": obj['Key'],
                        "Size": format_size(obj['Size']),
                        "Last Modified": obj['LastModified']
                    })
                df = pd.DataFrame(files)
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No files found.")
        except ClientError as e:
            st.error(f"Error listing objects: {e}")

    # === TAB 2: MAP PREVIEW ===
    with tab2:
        st.markdown("### Tile Layer Preview")
        st.caption("Inspects the bucket to find map tiles and attempts to render them via Leaflet.")

        # 1. Try to find a sample file to infer structure
        sample_key = None
        try:
            # Find a file that looks like an image
            sample_res = s3.list_objects_v2(Bucket=selected_bucket, Prefix=prefix, MaxKeys=50)
            if 'Contents' in sample_res:
                for obj in sample_res['Contents']:
                    if obj['Key'].lower().endswith(('.png', '.jpg', '.jpeg', '.webp', '.pbf')):
                        sample_key = obj['Key']
                        break
        except:
            pass

        if sample_key:
            st.success(f"Detected sample tile: `{sample_key}`")
            
            # Infer URL
            inferred_url = infer_tile_url(selected_bucket, AWS_REGION, prefix, sample_key)
            
            # Controls
            col_m1, col_m2 = st.columns([2, 1])
            with col_m1:
                tile_url_input = st.text_input("Tile Layer URL Template", value=inferred_url)
                st.caption("Ensure `{z}`, `{x}`, and `{y}` are in the correct positions.")
            
            with col_m2:
                zoom_start = st.slider("Start Zoom", 0, 20, 5)
                opacity = st.slider("Opacity", 0.0, 1.0, 1.0)

            # --- Authentication Warning ---
            st.info("💡 **Note on Private Buckets:** Standard map viewers (Leaflet) fetch tiles via the browser. If your S3 bucket is private, the map below may appear blank (403 Forbidden).")
            
            # Map Rendering
            try:
                m = folium.Map(location=[0, 0], zoom_start=zoom_start)
                
                folium.TileLayer(
                    tiles=tile_url_input,
                    attr=f'S3 Bucket: {selected_bucket}',
                    name='S3 Tiles',
                    overlay=True,
                    opacity=opacity
                ).add_to(m)
                
                # Add a base layer control
                folium.LayerControl().add_to(m)

                st_folium(m, width="100%", height=600)

            except Exception as e:
                st.error(f"Error creating map: {e}")

        else:
            st.warning("Could not find any image files (png/jpg/pbf) in this prefix to auto-detect a map structure.")

else:
    st.info("Please select a bucket to begin.")