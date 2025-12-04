import pandas as pd
import networkx as nx
from sqlalchemy import create_engine, text
import textdistance
import os
from dotenv import load_dotenv
from tqdm import tqdm
import urllib.parse

load_dotenv()

# Fetch DB credentials from environment
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

# ==============================================================================
# CONFIGURATION
# ==============================================================================
encoded_password = urllib.parse.quote_plus(DB_PASSWORD)

DB_CONNECTION = f"postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(
    DB_CONNECTION, 
    connect_args={'client_encoding': 'utf8'}
)

# THRESHOLDS
INITIAL_MATCH_THRESHOLD = 0.4  # (0-1) How similar names must be to even be CHECKED
FINAL_ACCEPT_THRESHOLD = 70.0  # (0-100) Score needed to merge into Master
COAUTHOR_BOOST_POINTS = 50.0   # Points added for shared co-authors

def main():
    print("--- 1. PREPARING DATABASE ENVIRONMENT ---")
    setup_candidate_table()

    print("--- 2. GENERATING CANDIDATES (BLOCKING) ---")
    # We find people with the same Last Name and First Initial
    # and insert them into a temporary list for processing.
    generate_candidates()

    print("--- 3. CALCULATING CO-AUTHORSHIP & SCORING ---")
    score_and_boost()

    print("--- 4. CLUSTERING & MERGING (MASTER RECORDS) ---")
    cluster_and_merge()

# ==============================================================================
# STEP 1: SETUP
# ==============================================================================
def setup_candidate_table():
    """Creates a table to hold our potential matches during processing."""
    print("   -> Resetting candidate table...")
    with engine.begin() as conn:
        # 1. DROP the table if it exists (Force a clean slate)
        conn.execute(text("DROP TABLE IF EXISTS public.match_candidates"))

        # 2. CREATE it with the exact columns Python expects
        conn.execute(text("""
            CREATE TABLE public.match_candidates (
                author_id_a INT,
                author_id_b INT,
                name_score FLOAT DEFAULT 0,
                coauthor_boost FLOAT DEFAULT 0,
                total_score FLOAT DEFAULT 0,
                status VARCHAR(20) DEFAULT 'pending',
                PRIMARY KEY (author_id_a, author_id_b)
            );
        """))

# ==============================================================================
# STEP 2: FIND POTENTIAL MATCHES (BLOCKING)
# ==============================================================================
def generate_candidates():
    """
    Since we don't have pg_trgm, we cannot ask the DB for fuzzy matches.
    Instead, we fetch groups of people with the SAME Last Name 
    and calculate the fuzzy score in Python.
    """
    print("   -> Fetching blocking keys (unique last names)...")
    
    # 1. Get all unique Blocking Keys (Last Name + First Initial)
    # This prevents us from loading the whole DB into RAM.
    # We use standard SQL lower() and substring() which exist in all DBs.
    keys_sql = text("""
        SELECT DISTINCT 
            lower(family_name) as lname, 
            lower(substring(given_name, 1, 1)) as fname_init
        FROM public.test_author
        WHERE master_author_id IS NULL -- Only check unprocessed
    """)
    
    with engine.connect() as conn:
        blocks = conn.execute(keys_sql).fetchall()
        
    print(f"   -> Found {len(blocks)} blocks to process.")
    
    total_candidates_found = 0
    batch_inserts = []
    
    # WRAP 'blocks' with tqdm() to create the progress bar
    for block in tqdm(blocks, desc="Blocking Authors"):
        lname = block.lname
        init = block.fname_init
        
        if not lname or not init:
            continue

        # Fetch all authors in this specific block
        # We perform the "Self-Join" in memory (Python) for this small slice of data
        records_sql = text("""
            SELECT id, given_name, family_name
            FROM public.test_author
            WHERE lower(family_name) = :lname 
              AND lower(substring(given_name, 1, 1)) = :init
              AND master_author_id IS NULL
        """)
        
        df = pd.read_sql(records_sql, engine, params={"lname": lname, "init": init})
        
        if len(df) < 2:
            continue # No pairs possible

        # 3. Pairwise Comparison in Python
        # We convert to a list of dicts for faster iteration than DataFrame
        records = df.to_dict('records')
        
        for i in range(len(records)):
            for j in range(i + 1, len(records)):
                rec_a = records[i]
                rec_b = records[j]
                
                # Create Full Name Strings
                name_a = f"{rec_a['given_name']} {rec_a['family_name']}".lower()
                name_b = f"{rec_b['given_name']} {rec_b['family_name']}".lower()
                
                # PYTHON FUZZY MATCH (Jaro-Winkler is excellent for names)
                # Score is 0.0 to 1.0, so we multiply by 100
                score = textdistance.jaro_winkler(name_a, name_b) * 100
                
                # Only keep if it looks like a match
                if score > (INITIAL_MATCH_THRESHOLD * 100):
                    batch_inserts.append({
                        "a": rec_a['id'],
                        "b": rec_b['id'],
                        "score": score
                    })
                    total_candidates_found += 1

        # Optimization: Insert in batches of 1000 to save memory
        if len(batch_inserts) > 1000:
            flush_candidates(batch_inserts)
            batch_inserts = []

    # Flush remaining
    if batch_inserts:
        flush_candidates(batch_inserts)

    print(f"   -> Generated {total_candidates_found} candidates using Python scoring.")

def flush_candidates(batch):
    """Helper to bulk insert candidates"""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO public.match_candidates (author_id_a, author_id_b, name_score)
            VALUES (:a, :b, :score)
        """), batch)
        
# ==============================================================================
# STEP 3: CO-AUTHOR BOOST
# ==============================================================================
def score_and_boost():
    """
    Iterates through candidates. Queries the graph to see if they share co-authors.
    """
    # 1. Fetch all pending candidates
    fetch_sql = text("SELECT author_id_a, author_id_b, name_score FROM public.match_candidates")
    df_candidates = pd.read_sql(fetch_sql, engine)
    
    if df_candidates.empty:
        print("   -> No candidates found. Exiting.")
        return

    updates = []
    
    # 2. Check overlap for each pair
    print(f"   -> Analyzing {len(df_candidates)} pairs for co-author overlap...")
    
    for idx, row in tqdm(df_candidates.iterrows(), total=df_candidates.shape[0], desc="Checking Co-Authors"):
        # Fix IDs (You already did this - Good!)
        id_a = int(row['author_id_a'])
        id_b = int(row['author_id_b'])
        
        # LOGIC: Check for shared co-authors
        overlap_sql = text("""
            SELECT COUNT(DISTINCT t1.author_id)
            FROM public.test_authorship t1
            JOIN public.test_authorship t2 ON t1.author_id = t2.author_id
            WHERE t1.publication_id IN (SELECT publication_id FROM public.test_authorship WHERE author_id = :id_a)
              AND t2.publication_id IN (SELECT publication_id FROM public.test_authorship WHERE author_id = :id_b)
              AND t1.author_id NOT IN (:id_a, :id_b)
        """)
        
        with engine.connect() as conn:
            shared_count = conn.execute(overlap_sql, {"id_a": id_a, "id_b": id_b}).scalar()
        
        boost = 0
        if shared_count > 0:
            # Cap the boost at 60 points
            boost = min(shared_count * 20, 60)

        # --- CRITICAL FIX HERE ---
        # 1. Calculate the math
        raw_final_score = row['name_score'] + boost
        
        # 2. CONVERT TO PYTHON FLOAT (Removes the 'np' error)
        final_boost = float(boost)
        final_total = float(raw_final_score)
        # -------------------------
        
        updates.append({
            "a": id_a,
            "b": id_b,
            "boost": final_boost, # Use the converted float
            "total": final_total  # Use the converted float
        })

    # 3. Bulk Update scores
    print("   -> Updating scores in database...")
    with engine.begin() as conn:
        for up in updates:
            conn.execute(text("""
                UPDATE public.match_candidates
                SET coauthor_boost = :boost,
                    total_score = :total
                WHERE author_id_a = :a AND author_id_b = :b
            """), up)

# ==============================================================================
# STEP 4: CLUSTER & MERGE
# ==============================================================================
def cluster_and_merge():
    # 1. Load High-Scoring Pairs
    sql = text(f"""
        SELECT author_id_a, author_id_b 
        FROM public.match_candidates 
        WHERE total_score >= {FINAL_ACCEPT_THRESHOLD}
    """)
    df_edges = pd.read_sql(sql, engine)
    
    if df_edges.empty:
        print("   -> No matches passed the threshold.")
        return

    # 2. Build Graph
    G = nx.Graph()
    G.add_edges_from(zip(df_edges.author_id_a, df_edges.author_id_b))
    
    clusters = list(nx.connected_components(G))
    print(f"   -> Found {len(clusters)} unique authors to create.")

    # 3. Create Master Records
    with engine.begin() as conn:
        for cluster in clusters:
            ids_tuple = tuple(cluster)
            ids_str = str(ids_tuple) if len(ids_tuple) > 1 else f"({ids_tuple[0]})"
            
            # A. Pick Best Name
            name_res = conn.execute(text(f"""
                SELECT given_name, family_name, orcid_id 
                FROM public.test_author WHERE id IN {ids_str}
            """)).fetchall()
            
            best_name = "Unknown"
            best_orcid = None
            longest = 0
            
            for r in name_res:
                full = f"{r.given_name or ''} {r.family_name or ''}".strip()
                if len(full) > longest:
                    longest = len(full)
                    best_name = full
                if r.orcid_id: 
                    best_orcid = r.orcid_id

            # B. Insert Master
            # Note: We handle the Unique Constraint on ORCID carefully
            if best_orcid:
                # Upsert logic if ORCID exists
                ins_sql = text("""
                    INSERT INTO public.master_author (canonical_name, primary_orcid)
                    VALUES (:name, :orcid)
                    ON CONFLICT (primary_orcid) DO UPDATE 
                    SET canonical_name = EXCLUDED.canonical_name
                    RETURNING id
                """)
                # If conflict exists, it updates and returns ID.
                # If using older Postgres, RETURNING might not fire on conflict, need fallback.
                try:
                    mid = conn.execute(ins_sql, {"name": best_name, "orcid": best_orcid}).scalar()
                except:
                    # Fallback if scalar returns None on conflict
                    mid = conn.execute(text("SELECT id FROM public.master_author WHERE primary_orcid=:o"), {"o":best_orcid}).scalar()
            else:
                # No ORCID, just insert
                ins_sql = text("""
                    INSERT INTO public.master_author (canonical_name)
                    VALUES (:name)
                    RETURNING id
                """)
                mid = conn.execute(ins_sql, {"name": best_name}).scalar()

            # C. Link Records
            conn.execute(text(f"""
                UPDATE public.test_author 
                SET master_author_id = :mid 
                WHERE id IN {ids_str}
            """), {"mid": mid})

    print("--- PROCESS COMPLETE ---")

if __name__ == "__main__":
    main()