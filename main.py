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
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# Load environment variables from .env/.example.env
load_dotenv()

DO_ACCESS_KEY_ID = os.getenv("DO_ACCESS_KEY_ID")
DO_SECRET_KEY = os.getenv("DO_SECRET_KEY")
DO_ENDPOINT = os.getenv("DO_ENDPOINT")
DO_BUCKET = os.getenv("DO_BUCKET")
USER_NS_KEY = os.getenv("USER_NS_KEY")

if not all([DO_ACCESS_KEY_ID, DO_SECRET_KEY, DO_ENDPOINT, DO_BUCKET, USER_NS_KEY]):
    missing = [k for k, v in {
        "DO_ACCESS_KEY_ID": DO_ACCESS_KEY_ID,
        "DO_SECRET_KEY": DO_SECRET_KEY,
        "DO_ENDPOINT": DO_ENDPOINT,
        "DO_BUCKET": DO_BUCKET,
        "USER_NS_KEY": USER_NS_KEY,
    }.items() if not v]
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Directories & paths
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
URI_MAP_FILE = DATA_DIR / "uri.json"

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


def load_uri_map() -> Dict[str, str]:
    """Load the URI map (id -> s3_key)."""
    if URI_MAP_FILE.exists():
        return json.loads(URI_MAP_FILE.read_text())
    return {}


def save_uri_map(uri_map: Dict[str, str]):
    """Save the URI map."""
    URI_MAP_FILE.write_text(json.dumps(uri_map, indent=2))


def get_uri_for_key(s3_key: str, uri_map: Dict[str, str]) -> str:
    """Get or create a URI ID for an S3 key (only creates if doesn't exist)."""
    # Check if this key already has an ID
    for uri_id, key in uri_map.items():
        if key == s3_key:
            return uri_id
    
    # Generate new ID only if key doesn't exist
    new_id = generate_nanoid()
    # Ensure uniqueness (very unlikely collision with 21 chars)
    while new_id in uri_map:
        new_id = generate_nanoid()
    
    uri_map[new_id] = s3_key
    return new_id


def add_uris_for_new_files(all_files: List[Dict], uri_map: Dict[str, str]) -> bool:
    """Add URI mappings for any new files that don't already have them."""
    changes_made = False
    existing_keys = set(uri_map.values())
    
    for file_obj in all_files:
        s3_key = file_obj["Key"]
        
        # Skip .DS_Store files (case insensitive)
        key_lower = s3_key.lower()
        if key_lower.endswith("/.ds_store") or key_lower == ".ds_store":
            continue
            
        # Skip directory markers
        if s3_key.endswith("/"):
            continue
            
        # Only add URI if this file doesn't already have one
        if s3_key not in existing_keys:
            get_uri_for_key(s3_key, uri_map)
            changes_made = True
            logger.info(f"Added new URI mapping for: {s3_key}")
    
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
    
    # Add URI IDs to files
    uri_map = load_uri_map()
    for file_obj in files:
        s3_key = file_obj["Key"]
        file_obj["permanent_uri_id"] = get_uri_for_key(s3_key, uri_map)
    
    # Save updated URI map if new IDs were created
    save_uri_map(uri_map)
    
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
    """Serve a file by its URI ID."""
    uri_map = load_uri_map()
    
    if uri_id not in uri_map:
        raise HTTPException(status_code=404, detail="URI not found")
    
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
        uri_map = load_uri_map()
        if uri_id in uri_map:
            del uri_map[uri_id]
            save_uri_map(uri_map)
        raise HTTPException(status_code=404, detail="File no longer exists")
    except Exception as e:
        logger.exception("Failed generating presigned URL for URI %s -> %s", uri_id, s3_key)
        raise HTTPException(status_code=500, detail="Failed to generate presigned URL") from e


@app.post("/api/validate-access-key")
async def validate_access_key(request: Request):
    """Validate the access key submitted by the user."""
    try:
        body = await request.json()
        access_key = body.get("access_key", "").strip()
        
        if not access_key:
            raise HTTPException(status_code=400, detail="Access key is required")
        
        if access_key == USER_NS_KEY:
            return JSONResponse({"valid": True, "message": "Access key validated successfully"})
        else:
            return JSONResponse({"valid": False, "message": "Invalid access key"}, status_code=401)
    
    except Exception as e:
        logger.exception("Error validating access key")
        raise HTTPException(status_code=500, detail="Internal server error") from e


def get_recursive_file_tree(prefix: str = "") -> List[Dict]:
    """Get all files recursively from a given prefix."""
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    
    all_files = []
    paginator = s3.get_paginator("list_objects_v2")
    
    for page in paginator.paginate(Bucket=DO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            # Skip directory markers and .DS_Store files
            if obj["Key"].endswith("/"):
                continue
            
            key_lower = obj["Key"].lower()
            if key_lower.endswith("/.ds_store") or key_lower == ".ds_store":
                continue
            
            # Calculate relative path from the starting prefix
            relative_path = obj["Key"][len(prefix):] if prefix else obj["Key"]
            file_name = relative_path.split("/")[-1]
            
            all_files.append({
                "file_name": file_name,
                "full_path": obj["Key"],
                "relative_path": relative_path,
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat()
            })
    
    return sorted(all_files, key=lambda x: x["full_path"])


@app.get("/tree", response_class=HTMLResponse)
@app.get("/tree/{prefix:path}", response_class=HTMLResponse)
async def tree_view(request: Request, prefix: str = "", access_key: str = None):
    """Generate a tree view of all files in the current directory and subdirectories. Requires authentication."""
    # Check authentication via header or query parameter
    auth_header = request.headers.get("X-Access-Key")
    auth_param = access_key
    
    if (not auth_header or auth_header != USER_NS_KEY) and (not auth_param or auth_param != USER_NS_KEY):
        raise HTTPException(status_code=401, detail="Authentication required")
    
    files = get_recursive_file_tree(prefix)
    
    # Add URI IDs to files using the new function
    uri_map = load_uri_map()
    for file_obj in files:
        s3_key = file_obj["full_path"]
        file_obj["permanent_uri_id"] = get_uri_for_key(s3_key, uri_map)
    
    # Save any new URI mappings that were created
    save_uri_map(uri_map)
    
    # Generate HTML table
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>File Tree - {prefix or 'Root'}</title>
        <style>
            @font-face {{
                font-family: 'TX02';
                src: url('/static/fonts/TX-02-Regular.woff2') format('woff2');
                font-weight: normal;
                font-style: normal;
                font-display: swap;
            }}
            body {{ 
                margin: 20px; 
                font-family: 'TX02', sans-serif; 
                background-color: #f5f5f5;
            }}
            h1 {{ 
                color: #333; 
                margin-bottom: 20px;
                font-family: 'TX02', sans-serif;
            }}
            table {{ 
                border-collapse: collapse; 
                width: 100%; 
                background-color: white;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }}
            th, td {{ 
                border: 1px solid #ddd; 
                padding: 8px 12px; 
                text-align: left; 
                font-family: 'TX02', sans-serif;
            }}
            th {{ 
                background-color: #f8f9fa; 
                font-weight: bold;
                position: sticky;
                top: 0;
            }}
            tr:nth-child(even) {{ 
                background-color: #f9f9f9; 
            }}
            tr:hover {{ 
                background-color: #e3f2fd; 
            }}
            a {{ 
                color: #104BC4; 
                text-decoration: none; 
            }}
            a:hover {{ 
                text-decoration: underline; 
            }}
            .path-cell {{
                font-family: monospace;
                font-size: 0.9em;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <h1>File Tree: {prefix or 'Root Directory'}</h1>
        <p style="color: #666; margin-bottom: 20px;">Total files: {len(files)}</p>
        
        <table>
            <thead>
                <tr>
                    <th>File Name</th>
                    <th>Path</th>
                    <th>Permanent URI</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for file_obj in files:
        permanent_uri = ""
        if file_obj["permanent_uri_id"]:
            permanent_uri = f'<a href="https://mfcoapi.com/file/{file_obj["permanent_uri_id"]}" target="_blank">https://mfcoapi.com/file/{file_obj["permanent_uri_id"]}</a>'
        else:
            permanent_uri = '<span style="color: #999; font-style: italic;">URI pending</span>'
        
        html_content += f"""
                <tr>
                    <td>{file_obj["file_name"]}</td>
                    <td class="path-cell">{file_obj["full_path"]}</td>
                    <td>{permanent_uri}</td>
                </tr>
        """
    
    html_content += """
            </tbody>
        </table>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)

# ---------------------------------------------------------------------------
# Background bucket monitor
# ---------------------------------------------------------------------------


async def bucket_uri_indexer(interval: int = 3600):  # 1 hour = 3600 seconds
    """Periodically scan bucket and add URI mappings for new files."""
    logger.info("Starting URI indexer with %d s interval (hourly)", interval)
    while True:
        try:
            logger.info("Running hourly URI indexing...")
            # Get all files from bucket
            all_files: List[Dict] = []
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=DO_BUCKET):
                all_files.extend(page.get("Contents", []))
            
            # Load current URI map and add mappings for new files
            uri_map = load_uri_map()
            changes_made = add_uris_for_new_files(all_files, uri_map)
            
            if changes_made:
                save_uri_map(uri_map)
                logger.info("Updated URI mappings during hourly scan")
            else:
                logger.info("No new files found during hourly scan")
                
        except Exception:
            logger.exception("Error during URI indexing")
        await asyncio.sleep(interval)


def index_new_files() -> Dict[str, int]:
    """Manually scan for new files and add URI mappings. Returns stats."""
    try:
        logger.info("Manual indexing started...")
        
        # Get all files from bucket
        all_files: List[Dict] = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=DO_BUCKET):
            all_files.extend(page.get("Contents", []))
        
        # Load current URI map and add mappings for new files
        uri_map = load_uri_map()
        initial_count = len(uri_map)
        changes_made = add_uris_for_new_files(all_files, uri_map)
        
        if changes_made:
            save_uri_map(uri_map)
        
        final_count = len(uri_map)
        new_files = final_count - initial_count
        
        logger.info(f"Manual indexing completed. Added {new_files} new URI mappings")
        
        return {
            "total_files_scanned": len(all_files),
            "existing_uris": initial_count,
            "new_uris_added": new_files,
            "total_uris": final_count
        }
        
    except Exception as e:
        logger.exception("Error during manual indexing")
        raise e


@app.post("/index-new")
async def index_new_route(request: Request):
    """Manually trigger indexing of new files. Requires authentication."""
    # Check authentication via header
    auth_header = request.headers.get("X-Access-Key")
    if not auth_header or auth_header != USER_NS_KEY:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    try:
        stats = index_new_files()
        return {
            "success": True,
            "message": f"Indexing completed. Added {stats['new_uris_added']} new URI mappings.",
            "stats": stats
        }
    except Exception as e:
        logger.exception("Manual indexing failed")
        raise HTTPException(status_code=500, detail=f"Indexing failed: {str(e)}")


@app.on_event("startup")
async def startup_event():
    # Launch hourly URI indexer in background
    asyncio.create_task(bucket_uri_indexer())

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=5052,
        reload=True,
    ) 