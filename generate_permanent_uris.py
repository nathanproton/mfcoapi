#!/usr/bin/env python3
"""
Script to generate permanent URIs for all files in the APXV1-ALICE repository.
This will recursively scan all files and generate permanent URI mappings.
"""

import json
import logging
import os
import secrets
import string
from pathlib import Path
from typing import Dict, Set

import boto3
from botocore.config import Config
from dotenv import load_dotenv

# Load environment variables
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

# Paths
DATA_DIR = Path("data")
PERMANENT_URI_MAP_FILE = DATA_DIR / "permanent_uri_map.json"

# Create the boto3 S3 client for DigitalOcean Spaces
s3 = boto3.client(
    "s3",
    aws_access_key_id=DO_ACCESS_KEY_ID,
    aws_secret_access_key=DO_SECRET_KEY,
    endpoint_url=DO_ENDPOINT,
    config=Config(signature_version="s3v4"),
)

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

def list_all_files_in_prefix(prefix: str) -> list:
    """List all files recursively under the given prefix."""
    all_files = []
    
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=DO_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                # Skip directory markers and .DS_Store files
                key = obj["Key"]
                key_lower = key.lower()
                
                if key.endswith("/"):
                    continue  # Directory marker
                if key_lower.endswith("/.ds_store") or key_lower == ".ds_store":
                    continue  # .DS_Store files
                
                all_files.append(obj)
                
    except Exception as e:
        logger.error(f"Error listing files in prefix '{prefix}': {e}")
        raise
    
    return all_files

def generate_uris_for_repository():
    """Generate permanent URIs for all files in the APXV1-ALICE repository."""
    repository_prefix = "appomattox/repositories/APXV1-ALICE/"
    
    logger.info(f"Loading existing permanent URI map...")
    uri_map = load_permanent_uri_map()
    initial_count = len(uri_map)
    
    logger.info(f"Scanning files in '{repository_prefix}'...")
    all_files = list_all_files_in_prefix(repository_prefix)
    
    logger.info(f"Found {len(all_files)} files in the repository")
    
    new_mappings = 0
    existing_keys: Set[str] = set(uri_map.values())
    
    for i, file_obj in enumerate(all_files, 1):
        s3_key = file_obj["Key"]
        
        if s3_key not in existing_keys:
            uri_id = get_permanent_uri_for_key(s3_key, uri_map)
            new_mappings += 1
            logger.info(f"Generated URI {uri_id} for {s3_key}")
        else:
            logger.debug(f"URI already exists for {s3_key}")
        
        if i % 100 == 0:
            logger.info(f"Processed {i}/{len(all_files)} files...")
    
    # Save the updated map
    logger.info(f"Saving updated permanent URI map...")
    save_permanent_uri_map(uri_map)
    
    final_count = len(uri_map)
    logger.info(f"Completed! Added {new_mappings} new permanent URIs")
    logger.info(f"Total URIs: {initial_count} -> {final_count}")
    
    return uri_map, new_mappings

if __name__ == "__main__":
    try:
        logger.info("Starting permanent URI generation for APXV1-ALICE repository...")
        uri_map, new_count = generate_uris_for_repository()
        
        if new_count > 0:
            logger.info(f"Successfully generated {new_count} new permanent URIs!")
        else:
            logger.info("No new URIs needed - all files already have permanent URIs")
            
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise 