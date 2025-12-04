import psycopg2
from psycopg2.extras import Json
import networkx as nx
from thefuzz import fuzz # For string similarity
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost")  # default to localhost if not set
}

# ---------------------------------------------------------
# 1. HELPER FUNCTIONS
# ---------------------------------------------------------

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def extract_author_metadata(raw_record):
    """
    Parses your JSONB to find name components.
    You must adapt this based on the specific JSON structure of ORCID vs DBLP.
    """
    data = raw_record[3] # raw_data column
    source_name = raw_record[4] # joined source name
    
    family = ""
    given = ""
    
    # EXAMPLE LOGIC - Adapt to your actual JSON keys
    try:
        if source_name == 'orcid':
            # ORCID usually has 'name': {'family-name': ..., 'given-names': ...}
            family = data.get('name', {}).get('family-name', {}).get('value', '')
            given = data.get('name', {}).get('given-names', {}).get('value', '')
        elif source_name == 'dblp':
            # DBLP often has just 'author': 'Name String'
            full_name = data.get('author', '')
            parts = full_name.split()
            if parts:
                family = parts[-1]
                given = " ".join(parts[:-1])
    except Exception:
        pass

    return {
        "id": raw_record[0],
        "family": (family or "").strip().lower(),
        "given": (given or "").strip().lower(),
        "original_family": family,
        "original_given": given
    }

def generate_blocking_key(author_meta):
    """
    Creates a key to group potential duplicates.
    Strategy: First 3 letters of Last Name + First Initial of First Name.
    Example: 'Smith', 'John' -> 'smij'
    """
    if not author_meta['family']:
        return None
    
    fam_part = author_meta['family'][:4] # First 4 chars of last name
    given_part = author_meta['given'][0] if author_meta['given'] else 'z'
    
    return f"{fam_part}_{given_part}"

# ---------------------------------------------------------
# 2. MAIN ALGORITHM
# ---------------------------------------------------------

def run_disambiguation():
    conn = get_db_connection()
    cursor = conn.cursor()

    print("--- Fetching Raw Authors ---")
    # Fetch only records that haven't been processed yet (master_author_id is NULL)
    query = """
        SELECT r.id, r.data_source_id, r.source_specific_id, r.raw_data, ds.name
        FROM raw_author_record r
        JOIN data_source ds ON r.data_source_id = ds.id
        WHERE r.master_author_id IS NULL
        LIMIT 10000; -- Process in batches for safety
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    if not rows:
        print("No new records to process.")
        return

    # --- Step 2: Blocking ---
    print(f"Blocking {len(rows)} records...")
    blocks = {}
    
    metadata_map = {} # Store parsed data to avoid re-parsing

    for row in rows:
        meta = extract_author_metadata(row)
        metadata_map[meta['id']] = meta
        
        b_key = generate_blocking_key(meta)
        if b_key:
            if b_key not in blocks:
                blocks[b_key] = []
            blocks[b_key].append(meta['id'])

    # --- Step 3: Pairwise Matching & Graph Building ---
    print("Building Similarity Graph...")
    G = nx.Graph()

    # Add all nodes first
    for uid in metadata_map:
        G.add_node(uid)

    for b_key, ids in blocks.items():
        # If block has only 1 record, it's a singleton (no edges needed)
        if len(ids) < 2:
            continue
        
        # Compare every pair in this block
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                id_a = ids[i]
                id_b = ids[j]
                
                person_a = metadata_map[id_a]
                person_b = metadata_map[id_b]
                
                # --- MATCHING LOGIC ---
                # 1. Family name must be very close
                fam_score = fuzz.ratio(person_a['family'], person_b['family'])
                
                # 2. Given name: check token set ratio to handle "J. Smith" vs "John Smith"
                given_score = fuzz.token_sort_ratio(person_a['given'], person_b['given'])
                
                # Thresholds (Tweak these based on results!)
                if fam_score > 90 and given_score > 85:
                    # They are likely the same person -> Add Edge
                    G.add_edge(id_a, id_b)

    # --- Step 4: Clustering (Connected Components) ---
    print("Clustering and generating Master Records...")
    
    # connected_components yields sets of IDs that are linked
    clusters = list(nx.connected_components(G))
    
    print(f"Found {len(clusters)} unique clusters.")

    # --- Step 5: Write to DB ---
    try:
        for cluster in clusters:
            cluster_ids = list(cluster)
            
            # Pick a representative name (e.g., the longest one usually has the most info)
            best_candidate = metadata_map[cluster_ids[0]]
            for cid in cluster_ids:
                curr = metadata_map[cid]
                if len(curr['original_given']) > len(best_candidate['original_given']):
                    best_candidate = curr

            # 1. Create Master Author
            cursor.execute("""
                INSERT INTO master_author (preferred_family_name, preferred_given_name)
                VALUES (%s, %s)
                RETURNING id;
            """, (best_candidate['original_family'], best_candidate['original_given']))
            
            new_master_id = cursor.fetchone()[0]
            
            # 2. Update Raw Records
            # We use distinct SQL for performance (bulk update is better, but this is clearer)
            cursor.execute("""
                UPDATE raw_author_record
                SET master_author_id = %s,
                    updated_at = NOW()
                WHERE id = ANY(%s);
            """, (new_master_id, cluster_ids))
            
        conn.commit()
        print("Database updated successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_disambiguation()