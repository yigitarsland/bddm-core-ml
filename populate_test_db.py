import requests
import psycopg2
import time
import json
import sys
import codecs
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib.parse


load_dotenv()


# Force UTF-8 for the console output to prevent print() errors on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Fetch DB credentials from environment
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DB_CONNECTION = f"postgresql+psycopg2://{DB_USER}:TCeVjNX%23T98YbUCwq%406p@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# We add connect_args to force the connection to use UTF-8
engine = create_engine(
    DB_CONNECTION, 
    connect_args={'client_encoding': 'utf8'}
)

# ORCID API Config (Use Public API)
# Ideally, register a Public API client to get higher rate limits
CLIENT_ID = None  # Optional: 'APP-XXXXXXXX'
CLIENT_SECRET = None # Optional: 'std-secret-xxxx'
BASE_URL = "https://pub.orcid.org/v3.0"

# Limit: 1 GB in bytes
LIMIT_BYTES = 1 * 1024 * 1024 * 1024 
current_bytes = 0

# Generic search to find people (finding people with 'university' in affiliation is a good catch-all)
SEARCH_QUERY = 'affiliation-org-name:"University"'

# --- DATABASE CONNECTION ---
try:
    # 1. Decode the password (convert %23 -> # and %40 -> @)
    # We use 'unquote' because your .env password is URL encoded.
    raw_password = urllib.parse.unquote(DB_PASSWORD)

    # 2. Connect using explicit arguments (fixes the "mapping" error)
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=raw_password, 
        options="-c client_encoding=UTF8"
    )
    
    conn.autocommit = True
    cursor = conn.cursor()
    print("Connected to Database (UTF-8 mode).")

except Exception as e:
    print(f"DB Connection failed: {e}")
    sys.exit(1)

# --- HELPER FUNCTIONS ---

def get_headers():
    headers = {"Accept": "application/json"}
    # If you have credentials, generate a token (skipped here for simplicity of Public API)
    return headers

def parse_safely(data, path, default=None):
    """Safe dictionary navigation."""
    try:
        for key in path:
            data = data[key]
        return data if data is not None else default
    except (KeyError, TypeError, IndexError):
        return default

def insert_author(orcid_id, given, family, affiliation):
    sql = """
        INSERT INTO public.test_author 
        (orcid_id, given_name, family_name, raw_affiliation_string, is_control_group)
        VALUES (%s, %s, %s, %s, TRUE)
        ON CONFLICT (orcid_id) DO UPDATE SET orcid_id=EXCLUDED.orcid_id
        RETURNING id;
    """
    # Note: The ON CONFLICT... UPDATE is a hack to get the ID back if it exists
    cursor.execute(sql, (orcid_id, given, family, affiliation))
    result = cursor.fetchone()
    return result[0] if result else None

def insert_alias(author_id, alias):
    sql = """
        INSERT INTO public.test_author_alias (author_id, alias_name)
        VALUES (%s, %s);
    """
    cursor.execute(sql, (author_id, alias))

import re

def normalize_title(title):
    """
    Simple normalization to improve matching odds.
    Removes extra spaces and converts to lowercase.
    """
    if not title:
        return ""
    # Remove non-alphanumeric characters (optional, but helps with "Study of X." vs "Study of X")
    # For now, let's just strip whitespace and lowercase
    return " ".join(title.split()).lower()

def insert_publication(doi, title, year, venue):
    """
    Inserts a publication with waterfall deduplication:
    1. Check exact DOI match.
    2. If no DOI match (or incoming DOI is None), check (Normalized Title + Year) match.
    3. Insert new if neither found.
    """
    
    # --- STEP 1: DOI Lookup (The Gold Standard) ---
    if doi:
        cursor.execute("SELECT id FROM public.test_publication WHERE doi = %s", (doi,))
        res = cursor.fetchone()
        if res:
            return res[0]

    # --- STEP 2: Title/Year Lookup (The Silver Standard) ---
    # We use this if:
    # a) The incoming data has no DOI.
    # b) The incoming data HAS a DOI, but we didn't find it in Step 1. 
    #    (This catches cases where we already have the paper but our DB entry is missing the DOI).
    
    # We require at least a title to attempt this
    if title:
        norm_title = normalize_title(title)
        
        # We search for a match where the title is roughly the same.
        # Note: We use the Postgres LOWER() function to ensure case-insensitivity db-side
        sql_title_check = """
            SELECT id, doi FROM public.test_publication 
            WHERE LOWER(title) = %s 
            AND (publication_year = %s OR publication_year IS NULL OR %s IS NULL)
        """
        # Note: The year check is loose (allows NULLs) to prevent duplicates if one record lacks a date
        cursor.execute(sql_title_check, (norm_title, year, year))
        res = cursor.fetchone()
        
        if res:
            existing_id, existing_doi = res
            
            # OPTIONAL UPGRADE:
            # If we just found the paper via Title, but the DB record had no DOI 
            # and we HAVE a DOI now, we should update the DB record!
            if doi and not existing_doi:
                cursor.execute(
                    "UPDATE public.test_publication SET doi = %s WHERE id = %s", 
                    (doi, existing_id)
                )
            
            return existing_id

    # --- STEP 3: Insert New Record ---
    sql_insert = """
        INSERT INTO public.test_publication (doi, title, publication_year, venue_name)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
    """
    try:
        cursor.execute(sql_insert, (doi, title, year, venue))
        return cursor.fetchone()[0]
    except psycopg2.IntegrityError:
        # This catches race conditions (another thread inserted the DOI milliseconds ago)
        conn.rollback() 
        # Recursive retry - simplified
        return insert_publication(doi, title, year, venue)

def link_authorship(author_id, pub_id):
    sql = """
        INSERT INTO public.test_authorship (author_id, publication_id)
        VALUES (%s, %s)
        ON CONFLICT (author_id, publication_id) DO NOTHING;
    """
    cursor.execute(sql, (author_id, pub_id))

# --- MAIN SCRAPING LOGIC ---

def process_orcid_record(orcid_id):
    global current_bytes
    
    # 1. Fetch Person Details (Name, Aliases, Employment)
    url = f"{BASE_URL}/{orcid_id}/person"
    resp = requests.get(url, headers=get_headers(), timeout=10)
    current_bytes += len(resp.content)
    
    if resp.status_code != 200:
        return

    data = resp.json()
    
    # Extract Author Data
    given_name = parse_safely(data, ['name', 'given-names', 'value'])
    family_name = parse_safely(data, ['name', 'family-name', 'value'])
    
    # Extract Affiliation (Get the first one if available)
    # ORCID separates this into /activities, but summaries are often in the person or expanded profile
    # For deep affiliation data, we technically need the /employments endpoint, 
    # but to save bandwidth, we check the summary logic here if available or skip.
    # Let's do a quick separate call for employment summary as required by schema.
    
    aff_url = f"{BASE_URL}/{orcid_id}/employments"
    aff_resp = requests.get(aff_url, headers=get_headers(), timeout=10)
    current_bytes += len(aff_resp.content)
    aff_data = aff_resp.json()
    
    raw_affiliation = None
    summaries = parse_safely(aff_data, ['employment-summary'], [])
    if summaries and len(summaries) > 0:
        raw_affiliation = parse_safely(summaries[0], ['organization', 'name'])

    # Insert Author
    db_author_id = insert_author(orcid_id, given_name, family_name, raw_affiliation)
    if not db_author_id: 
        return # Skip if failed

    # Extract Aliases
    other_names = parse_safely(data, ['other-names', 'other-name'], [])
    for alias_entry in other_names:
        alias = parse_safely(alias_entry, ['content'])
        if alias:
            insert_alias(db_author_id, alias)

    # 2. Fetch Works (Publications)
    # querying /works gives a summary list. 
    works_url = f"{BASE_URL}/{orcid_id}/works"
    works_resp = requests.get(works_url, headers=get_headers(), timeout=10)
    current_bytes += len(works_resp.content)
    
    if works_resp.status_code == 200:
        works_data = works_resp.json()
        groups = parse_safely(works_data, ['group'], [])
        
        for group in groups:
            # ORCID groups duplicates. We take the first work-summary
            summaries = parse_safely(group, ['work-summary'], [])
            if not summaries: continue
            
            work = summaries[0]
            
            title = parse_safely(work, ['title', 'title', 'value'])
            venue = parse_safely(work, ['journal-title', 'value'])
            
            # Year logic
            pub_year_str = parse_safely(work, ['publication-date', 'year', 'value'])
            pub_year = int(pub_year_str) if pub_year_str and pub_year_str.isdigit() else None
            
            # DOI Extraction
            doi = None
            ext_ids = parse_safely(work, ['external-ids', 'external-id'], [])
            for eid in ext_ids:
                if parse_safely(eid, ['external-id-type']) == 'doi':
                    doi = parse_safely(eid, ['external-id-value'])
                    break
            
            # Constraint: We need a DOI to satisfy the UNIQUE constraint effectively in schema
            # Or valid metadata.
            if title: 
                pub_db_id = insert_publication(doi, title, pub_year, venue)
                if pub_db_id:
                    link_authorship(db_author_id, pub_db_id)

def run_scraper():
    global current_bytes
    start_index = 0
    batch_size = 20  # Reduced from 100 to see progress faster
    
    print(f"Starting Scrape. Stop limit: {LIMIT_BYTES / (1024*1024):.2f} MB", flush=True)

    while current_bytes < LIMIT_BYTES:
        print(f"\nFetching batch starting at {start_index}...", flush=True)
        
        search_url = f"{BASE_URL}/search/?q={SEARCH_QUERY}&start={start_index}&rows={batch_size}"
        
        try:
            # Added timeout=30 to prevent network hangs
            resp = requests.get(search_url, headers=get_headers(), timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"Search Request Failed: {e}")
            time.sleep(5)
            continue

        current_bytes += len(resp.content)
        
        if resp.status_code != 200:
            print(f"Search failed (Status {resp.status_code}). Sleeping...", flush=True)
            time.sleep(5)
            continue
            
        search_data = resp.json()
        results = parse_safely(search_data, ['result'], [])
        
        if not results:
            print("No more results found.")
            break
            
        print(f"Found {len(results)} authors. Processing...", flush=True)

        for i, result in enumerate(results):
            orcid_id = parse_safely(result, ['orcid-identifier', 'path'])
            if orcid_id:
                # PROGRESS INDICATOR: Prints "Processing [ID]... Done"
                print(f"  [{i+1}/{len(results)}] {orcid_id}...", end=" ", flush=True)
                
                try:
                    process_orcid_record(orcid_id)
                    print("Done.", flush=True)
                    time.sleep(0.5) 
                except Exception as e:
                    print(f"Skipped ({e})", flush=True)
                
            if current_bytes >= LIMIT_BYTES:
                print("\n1 GB Limit Reached. Stopping.")
                return

        start_index += batch_size
        time.sleep(1)

if __name__ == "__main__":
    run_scraper()
    cursor.close()
    conn.close()
    print("Done.")