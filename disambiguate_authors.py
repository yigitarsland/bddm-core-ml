import pandas as pd
import networkx as nx
from sqlalchemy import create_engine, text

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DB_CONNECTION = "postgresql+psycopg2://username:password@localhost:5432/your_db"
engine = create_engine(DB_CONNECTION)

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
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.match_candidates (
                author_id_a INT,
                author_id_b INT,
                name_score FLOAT DEFAULT 0,
                coauthor_boost FLOAT DEFAULT 0,
                total_score FLOAT DEFAULT 0,
                status VARCHAR(20) DEFAULT 'pending',
                PRIMARY KEY (author_id_a, author_id_b)
            );
            -- Clear previous runs if you want a fresh start
            TRUNCATE TABLE public.match_candidates; 
        """))

# ==============================================================================
# STEP 2: FIND POTENTIAL MATCHES (BLOCKING)
# ==============================================================================
def generate_candidates():
    """
    We cannot compare everyone to everyone. We only compare people who
    share the same normalized Last Name and First Initial.
    """
    sql = text("""
        INSERT INTO public.match_candidates (author_id_a, author_id_b, name_score)
        SELECT 
            t1.id, 
            t2.id, 
            -- Calculate Basic Name Similarity (0 to 100)
            similarity(
                (t1.given_name || ' ' || t1.family_name), 
                (t2.given_name || ' ' || t2.family_name)
            ) * 100
        FROM public.test_author t1
        JOIN public.test_author t2 
            -- BLOCKING LOGIC:
            ON lower(t1.family_name) = lower(t2.family_name) 
            AND lower(substring(t1.given_name, 1, 1)) = lower(substring(t2.given_name, 1, 1))
            AND t1.id < t2.id -- Prevent duplicates (A-B vs B-A)
        WHERE 
            t1.master_author_id IS NULL -- Only check unlinked authors
            AND t2.master_author_id IS NULL
            -- OPTIMIZATION: Only keep pairs where names are reasonably close
            AND similarity(
                (t1.given_name || ' ' || t1.family_name), 
                (t2.given_name || ' ' || t2.family_name)
            ) > :thresh
    """)
    
    with engine.begin() as conn:
        conn.execute(sql, {"thresh": INITIAL_MATCH_THRESHOLD})
    
    # Check how many we found
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM public.match_candidates")).scalar()
    print(f"   -> Found {count} pairs to analyze.")

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
    # Note: We do this in a loop, but for millions of records, you'd want to batch this query.
    print(f"   -> Analyzing {len(df_candidates)} pairs for co-author overlap...")
    
    for idx, row in df_candidates.iterrows():
        id_a = row['author_id_a']
        id_b = row['author_id_b']
        
        # LOGIC:
        # Find all authors (t1.author_id) who are on papers with A
        # AND on papers with B.
        overlap_sql = text("""
            SELECT COUNT(DISTINCT t1.author_id)
            FROM public.test_authorship t1
            JOIN public.test_authorship t2 ON t1.author_id = t2.author_id
            WHERE t1.publication_id IN (SELECT publication_id FROM public.test_authorship WHERE author_id = :id_a)
              AND t2.publication_id IN (SELECT publication_id FROM public.test_authorship WHERE author_id = :id_b)
              AND t1.author_id NOT IN (:id_a, :id_b) -- Exclude the candidates themselves
        """)
        
        with engine.connect() as conn:
            shared_count = conn.execute(overlap_sql, {"id_a": id_a, "id_b": id_b}).scalar()
        
        boost = 0
        if shared_count > 0:
            boost = COAUTHOR_BOOST_POINTS
            # Optional: Scale boost by number of shared co-authors? 
            # boost = min(shared_count * 20, 60)
        
        final_score = row['name_score'] + boost
        
        updates.append({
            "a": id_a,
            "b": id_b,
            "boost": boost,
            "total": final_score
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