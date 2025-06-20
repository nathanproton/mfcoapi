import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import secrets
import string

import boto3
from botocore.config import Config
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# Load environment variables from .env/.example.env
load_dotenv()

DO_ACCESS_KEY_ID = os.getenv("DO_ACCESS_KEY_ID")
DO_SECRET_KEY = os.getenv("DO_SECRET_KEY")
DO_ENDPOINT = os.getenv("DO_ENDPOINT")
DO_BUCKET = os.getenv("DO_BUCKET")

if not all([DO_ACCESS_KEY_ID, DO_SECRET_KEY, DO_ENDPOINT, DO_BUCKET]):
    missing = [k for k, v in {
        "DO_ACCESS_KEY_ID": DO_ACCESS_KEY_ID,
        "DO_SECRET_KEY": DO_SECRET_KEY,
        "DO_ENDPOINT": DO_ENDPOINT,
        "DO_BUCKET": DO_BUCKET,
    }.items() if not v]
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Directories & paths
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
SNAPSHOT_FILE = DATA_DIR / "snapshot.json"
CHANGELOG_FILE = DATA_DIR / "changelog.jsonl"
PERMANENT_URI_MAP_FILE = DATA_DIR / "permanent_uri_map.json"

# Init FastAPI
app = FastAPI(title="DO Spaces Browser")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Create the boto3 S3 client for DigitalOcean Spaces
s3 = boto3.client(
    "s3",
    aws_access_key_id=DO_ACCESS_KEY_ID,
    aws_secret_access_key=DO_SECRET_KEY,
    endpoint_url=DO_ENDPOINT,
    config=Config(signature_version="s3v4"),
)

# ---------------------------------------------------------------------------
# Helper utilities (updated)
# ---------------------------------------------------------------------------

UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]

def human_size(num: int) -> str:
    """Return human-readable file size."""
    for unit in UNITS:
        if num < 1024:
            return f"{num:.0f} {unit}"
        num /= 1024.0
    return f"{num:.0f} EB"


def human_date(iso: str) -> str:
    """Convert ISO timestamp to readable date string."""
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso

# Register Jinja filters
templates.env.filters["human_size"] = human_size
templates.env.filters["human_date"] = human_date


def generate_nanoid(length: int = 21) -> str:
    """Generate a nanoid-like string using URL-safe characters."""
    alphabet = string.ascii_letters + string.digits + "-_"
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def load_permanent_uri_map() -> Dict[str, str]:
    """Load the permanent URI map (id -> s3_key)."""
    if PERMANENT_URI_MAP_FILE.exists():
        return json.loads(PERMANENT_URI_MAP_FILE.read_text())
    return {}


def save_permanent_uri_map(uri_map: Dict[str, str]):
    """Save the permanent URI map."""
    PERMANENT_URI_MAP_FILE.write_text(json.dumps(uri_map, indent=2))


def get_permanent_uri_for_key(s3_key: str, uri_map: Dict[str, str]) -> str:
    """Get or create a permanent URI ID for an S3 key."""
    # Check if this key already has an ID
    for uri_id, key in uri_map.items():
        if key == s3_key:
            return uri_id
    
    # Generate new ID
    new_id = generate_nanoid()
    # Ensure uniqueness (very unlikely collision with 21 chars)
    while new_id in uri_map:
        new_id = generate_nanoid()
    
    uri_map[new_id] = s3_key
    return new_id


def update_permanent_uri_mappings(old_snapshot: Dict[str, Dict], new_snapshot: Dict[str, Dict], uri_map: Dict[str, str]):
    """Update permanent URI mappings when files are moved/renamed."""
    changes_made = False
    
    # Handle moved files by comparing ETags
    old_etags = {obj["ETag"]: key for key, obj in old_snapshot.items()}
    new_etags = {obj["ETag"]: key for key, obj in new_snapshot.items()}
    
    # Find files that were moved (same ETag, different key)
    for etag, old_key in old_etags.items():
        if etag in new_etags:
            new_key = new_etags[etag]
            if old_key != new_key:
                # File was moved - update the mapping
                for uri_id, mapped_key in uri_map.items():
                    if mapped_key == old_key:
                        uri_map[uri_id] = new_key
                        changes_made = True
                        logger.info(f"Updated permanent URI mapping: {uri_id} now points to {new_key} (was {old_key})")
                        break
    
    # Remove mappings for deleted files
    deleted_keys = set(old_snapshot.keys()) - set(new_snapshot.keys())
    for uri_id, key in list(uri_map.items()):
        if key in deleted_keys:
            # Check if this file was actually deleted (not just moved)
            if key not in [obj["Key"] for obj in new_snapshot.values()]:
                old_etag = old_snapshot.get(key, {}).get("ETag")
                if old_etag and old_etag not in new_etags:
                    del uri_map[uri_id]
                    changes_made = True
                    logger.info(f"Removed permanent URI mapping for deleted file: {uri_id} -> {key}")
    
    # Add mappings for new files
    for key in new_snapshot.keys():
        if key not in old_snapshot:
            get_permanent_uri_for_key(key, uri_map)
            changes_made = True
    
    return changes_made


def list_prefix(prefix: str = "") -> Tuple[List[Dict], List[Dict]]:
    """Return (folders, files) under the given prefix."""
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    paginator = s3.get_paginator("list_objects_v2")
    folders: List[Dict] = []
    files: List[Dict] = []
    for page in paginator.paginate(Bucket=DO_BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            full_prefix = cp["Prefix"]
            name = full_prefix[len(prefix):].rstrip("/")
            folders.append({"name": name, "prefix": full_prefix})
        for obj in page.get("Contents", []):
            if obj["Key"] == prefix:
                # This is the directory marker itself, skip
                continue
            
            # Skip .DS_Store files (case insensitive)
            key_lower = obj["Key"].lower()
            if key_lower.endswith("/.ds_store") or key_lower == ".ds_store":
                continue
            
            obj["display_name"] = obj["Key"][len(prefix):]
            obj["LastModified"] = obj["LastModified"].isoformat()
            files.append(obj)
    # Sort folders then files alphabetically
    folders.sort(key=lambda x: x["name"].lower())
    files.sort(key=lambda x: x["display_name"].lower())
    return folders, files


def build_breadcrumbs(prefix: str) -> List[Tuple[str, str | None]]:
    """Return list of (name, url/None) tuples for breadcrumb navigation.
    Collapses middle parts if path depth > 5.
    """
    crumbs: List[Tuple[str, str | None]] = [("Home", "/browse/")]
    if not prefix:
        return crumbs
    parts = prefix.strip("/").split("/")
    # Always keep trailing slash in constructed URLs
    def _url(path: str) -> str:
        return f"/browse/{path}"

    if len(parts) <= 5:
        path_accum = ""
        for part in parts:
            path_accum += f"{part}/"
            crumbs.append((part, _url(path_accum)))
    else:
        # first two, ellipsis, last two
        first_slice = parts[:2]
        last_slice = parts[-2:]
        path_accum = ""
        for part in first_slice:
            path_accum += f"{part}/"
            crumbs.append((part, _url(path_accum)))
        # Ellipsis (no link)
        if len(parts) > 4:
            crumbs.append(("…", None))
        # Build remaining URLs iteratively starting from first_slice+middle
        path_accum = "/".join(first_slice + parts[2:-2])
        if path_accum:
            path_accum += "/"
        for part in last_slice:
            path_accum += f"{part}/"
            crumbs.append((part, _url(path_accum)))
    return crumbs


def diff_snapshots(old: Dict[str, Dict], new: Dict[str, Dict]):
    """Generate a diff between two object maps (key->metadata)."""
    changes = []
    old_keys, new_keys = set(old.keys()), set(new.keys())

    added = new_keys - old_keys
    deleted = old_keys - new_keys
    possible_modified = old_keys & new_keys

    for key in added:
        changes.append({"action": "added", "key": key, "time": datetime.utcnow().isoformat()})
    for key in deleted:
        changes.append({"action": "deleted", "key": key, "time": datetime.utcnow().isoformat()})
    for key in possible_modified:
        if old[key]["ETag"] != new[key]["ETag"] or old[key]["Size"] != new[key]["Size"]:
            changes.append({"action": "modified", "key": key, "time": datetime.utcnow().isoformat()})
    return changes


def load_snapshot() -> Dict[str, Dict]:
    if SNAPSHOT_FILE.exists():
        return json.loads(SNAPSHOT_FILE.read_text())
    return {}


def save_snapshot(snapshot: Dict[str, Dict]):
    SNAPSHOT_FILE.write_text(json.dumps(snapshot))


def append_changelog(changes: List[Dict]):
    if not changes:
        return
    with CHANGELOG_FILE.open("a") as f:
        for entry in changes:
            f.write(json.dumps(entry) + "\n")
    logger.info("Recorded %d change(s) to %s", len(changes), CHANGELOG_FILE)

# ---------------------------------------------------------------------------
# Routes (updated)
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return await browse(request, prefix="")


@app.get("/browse", response_class=HTMLResponse)
@app.get("/browse/{prefix:path}", response_class=HTMLResponse)
async def browse(request: Request, prefix: str = ""):
    folders, files = list_prefix(prefix)
    breadcrumbs = build_breadcrumbs(prefix)
    
    # Add permanent URI IDs to files
    uri_map = load_permanent_uri_map()
    for file_obj in files:
        s3_key = file_obj["Key"]
        file_obj["permanent_uri_id"] = get_permanent_uri_for_key(s3_key, uri_map)
    
    # Save updated URI map if new IDs were created
    save_permanent_uri_map(uri_map)
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "bucket": DO_BUCKET,
            "prefix": prefix,
            "folders": folders,
            "files": files,
            "breadcrumbs": breadcrumbs,
        },
    )


@app.get("/sign-url/{key:path}")
async def sign_url(key: str, expires_in: int = 3600):
    try:
        url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": DO_BUCKET, "Key": key},
            ExpiresIn=expires_in,
        )
    except Exception as e:
        logger.exception("Failed generating presigned URL for %s", key)
        raise HTTPException(status_code=500, detail="Failed to generate presigned URL") from e
    return RedirectResponse(url)


@app.get("/file/{uri_id}")
async def get_file_by_permanent_uri(uri_id: str, expires_in: int = 3600):
    """Serve a file by its permanent URI ID."""
    uri_map = load_permanent_uri_map()
    
    if uri_id not in uri_map:
        raise HTTPException(status_code=404, detail="Permanent URI not found")
    
    s3_key = uri_map[uri_id]
    
    try:
        # Check if file still exists
        s3.head_object(Bucket=DO_BUCKET, Key=s3_key)
        
        # Generate presigned URL and redirect
        url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": DO_BUCKET, "Key": s3_key},
            ExpiresIn=expires_in,
        )
        return RedirectResponse(url)
    except s3.exceptions.NoSuchKey:
        # File no longer exists, remove from mapping
        uri_map = load_permanent_uri_map()
        if uri_id in uri_map:
            del uri_map[uri_id]
            save_permanent_uri_map(uri_map)
        raise HTTPException(status_code=404, detail="File no longer exists")
    except Exception as e:
        logger.exception("Failed generating presigned URL for permanent URI %s -> %s", uri_id, s3_key)
        raise HTTPException(status_code=500, detail="Failed to generate presigned URL") from e

# ---------------------------------------------------------------------------
# Background bucket monitor
# ---------------------------------------------------------------------------


async def bucket_monitor(interval: int = 60):
    """Periodically poll the bucket and update changelog."""
    logger.info("Starting bucket monitor with %d s interval", interval)
    while True:
        try:
            # Monitor the entire bucket (flat list)
            all_files: List[Dict] = []
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=DO_BUCKET):
                all_files.extend(page.get("Contents", []))
            snapshot = {obj["Key"]: obj for obj in all_files}
            previous = load_snapshot()
            changes = diff_snapshots(previous, snapshot)
            
            # Update permanent URI mappings
            uri_map = load_permanent_uri_map()
            mappings_changed = update_permanent_uri_mappings(previous, snapshot, uri_map)
            
            if changes:
                append_changelog(changes)
                save_snapshot(snapshot)
            
            if mappings_changed:
                save_permanent_uri_map(uri_map)
                logger.info("Updated permanent URI mappings")
                
        except Exception:
            logger.exception("Error while monitoring bucket")
        await asyncio.sleep(interval)


@app.on_event("startup")
async def startup_event():
    # Launch monitor task in background
    asyncio.create_task(bucket_monitor())

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=5052,
        reload=True,
    ) 