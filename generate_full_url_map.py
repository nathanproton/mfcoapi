#!/usr/bin/env python3
"""
Script to create a copy of permanent_uri_map.json with full URLs and original paths.
This will transform the mapping from nanoid -> s3_key to nanoid -> {url, path}.
"""

import json
import logging
from pathlib import Path

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Paths
DATA_DIR = Path("data")
PERMANENT_URI_MAP_FILE = DATA_DIR / "permanent_uri_map.json"
FULL_URL_MAP_FILE = DATA_DIR / "permanent_uri_map_full_urls.json"

BASE_URL = "https://mfcoapi.com/file/"

def load_permanent_uri_map() -> dict:
    """Load the permanent URI map (id -> s3_key)."""
    if PERMANENT_URI_MAP_FILE.exists():
        return json.loads(PERMANENT_URI_MAP_FILE.read_text())
    return {}

def generate_full_url_map():
    """Generate a full URL version of the permanent URI map."""
    logger.info("Loading existing permanent URI map...")
    uri_map = load_permanent_uri_map()
    
    logger.info(f"Found {len(uri_map)} permanent URI mappings")
    
    # Transform the map: nanoid -> s3_key becomes nanoid -> {url, path}
    full_url_map = {}
    for nanoid, s3_key in uri_map.items():
        full_url = f"{BASE_URL}{nanoid}"
        full_url_map[nanoid] = {
            "url": full_url,
            "path": s3_key
        }
    
    # Save the full URL map
    logger.info(f"Saving full URL map to {FULL_URL_MAP_FILE}...")
    FULL_URL_MAP_FILE.write_text(json.dumps(full_url_map, indent=2))
    
    logger.info(f"Successfully created full URL map with {len(full_url_map)} URLs")
    
    # Print a few examples
    logger.info("Examples of generated URL mappings:")
    for i, (nanoid, mapping) in enumerate(list(full_url_map.items())[:5]):
        logger.info(f"  {nanoid}:")
        logger.info(f"    URL: {mapping['url']}")
        logger.info(f"    Path: {mapping['path']}")
    
    return full_url_map

if __name__ == "__main__":
    try:
        logger.info("Creating full URL version of permanent URI map...")
        full_url_map = generate_full_url_map()
        logger.info("Full URL map generation completed successfully!")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise 