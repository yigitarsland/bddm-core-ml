import pandas as pd
import networkx as nx
import textdistance
import json
from sqlalchemy import create_engine, text
from itertools import combinations

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DB_CONNECTION = "postgresql+psycopg2://username:password@localhost:5432/your_db"
THRESHOLD_SCORE = 75  # Points required to be considered a match
BLOCK_SIZE_LIMIT = 500 # Safety: Don't compare blocks larger than this in RAM

# WEIGHTS
W_NAME = 30
W_AFFILIATION = 20
W_COAUTHOR = 50  # High weight: If they share co-authors, they are the same person.

engine = create_engine(DB_CONNECTION)

# ==============================================================================
# 1. SCORING FUNCTIONS (PHASE 3: FUZZY MATH)
# ==============================================================================

def get_similarity_score(record_a, record_b):
    """
    Compares two JSON records and returns a score from 0 to 100.
    """
    data_a = record_a['raw_data']
    data_b = record_b['raw_data']
    
    score = 0
    
    # --- A. NAME SCORING (Jaro-Winkler is great for short strings) ---
    name_a = f"{data_a.get('given_name', '')} {data_a.get('family_name', '')}".strip().lower()
    name_b = f"{data_b.get('given_name', '')} {data_b.get('family_name', '')}".strip().lower()
    
    # Jaro-Winkler gives a boost to strings that match at the beginning
    name_sim = textdistance.jaro_winkler(name_a, name_b)
    score += name_sim * W_NAME

    # --- B. AFFILIATION SCORING (Token Set Ratio) ---
    # Handles "Univ. of Oxford" vs "Oxford University"
    aff_a = data_a.get('affiliation')
    aff_b = data_b.get('affiliation')
    
    if aff_a and aff_b:
        # Simple Jaccard on words
        set_a = set(str(aff_a).lower().split())
        set_b = set(str(aff_b).lower().split())
        intersection = len(set_a.intersection(set_b))
        union = len(set_a.union(set_b))
        
        if union > 0:
            aff_sim = intersection / union
            score += aff_sim * W_AFFILIATION

    # --- C. CO-AUTHOR OVERLAP (The Magic Sauce) ---
    # Assuming raw_data has a list like: "coauthors": ["Bob Jones", "Alice Wu"]
    co_a = set(data_a.get('coauthors', []))
    co_b = set(data_b.get('coauthors', []))
    
    if co_a and co_b:
        shared = len(co_a.intersection(co_b))
        # If they share even 1 co-author (and names match), it's a huge signal
        if shared >= 1:
            # Boost score based on how many they share, maxing out at full weight
            boost = min(shared * 25, W_COAUTHOR) 
            score += boost
            
    return score

# ==============================================================================
# 2. MAIN EXECUTION LOOP
# ==============================================================================

def run_disambiguation():
    print("--- Starting Disambiguation Process ---")
    
    # 1. FETCH BLOCKS
    # We fetch all distinct blocking keys for unprocessed records
    # This prevents loading the whole DB into RAM.
    block_query = text("""
        SELECT DISTINCT block_lastname, block_initial 
        FROM public.raw_author_record 
        WHERE processing_status = 'unprocessed'
    """)
    
    with engine.connect() as conn:
        blocks = conn.execute(block_query).fetchall()
        
    print(f"Found {len(blocks)} blocks to process.")

    for block in blocks:
        lname, initial = block
        if not lname: continue # Skip empty blocks

        # 2. LOAD DATA FOR THIS BLOCK
        # Only fetch records for 'Smith', 'J'
        records_query = text("""
            SELECT id, raw_data 
            FROM public.raw_author_record 
            WHERE block_lastname = :lname 
              AND block_initial = :initial
              AND processing_status = 'unprocessed'
        """)
        
        df = pd.read_sql(records_query, engine, params={"lname": lname, "initial": initial})
        
        # Safety check: If block is massive (e.g., 'Wang', 'Y'), 
        # N^2 comparison will freeze python.
        if len(df) > BLOCK_SIZE_LIMIT:
            print(f"Skipping block {lname} {initial}: Too huge ({len(df)} records)")
            continue
            
        if len(df) < 2:
            continue # Nothing to compare

        # 3. BUILD GRAPH (PHASE 4: CLUSTERING)
        G = nx.Graph()
        # Add all nodes first
        G.add_nodes_from(df['id'].tolist())
        
        # Pairwise comparison within the block
        records = df.to_dict('records') # Convert to list of dicts for speed
        
        for rec_a, rec_b in combinations(records, 2):
            final_score = get_similarity_score(rec_a, rec_b)
            
            if final_score >= THRESHOLD_SCORE:
                # Add edge to graph
                G.add_edge(rec_a['id'], rec_b['id'], weight=final_score)

        # 4. FIND CONNECTED COMPONENTS
        # If A-B match and B-C match, this returns {A, B, C}
        clusters = list(nx.connected_components(G))
        
        # 5. WRITE TO DB
        process_clusters(clusters, df)

def process_clusters(clusters, df_source):
    """
    Takes clusters of IDs, creates Master records, and updates Raw records.
    """
    with engine.begin() as conn: # Transactional
        for cluster in clusters:
            cluster_ids = list(cluster)
            
            # If cluster has only 1 item, it's a "Singleton".
            # Decision: Do we create a Master Record for singletons? 
            # Usually YES, because they are valid authors, just no duplicates found yet.
            
            # A. CANONICALIZATION (Find best name)
            # Get the rows for this cluster from our local dataframe
            cluster_data = df_source[df_source['id'].isin(cluster_ids)]
            
            best_first = ""
            best_last = ""
            longest_len = 0
            
            for _, row in cluster_data.iterrows():
                rd = row['raw_data']
                fn = rd.get('given_name', '')
                ln = rd.get('family_name', '')
                
                if len(fn) + len(ln) > longest_len:
                    longest_len = len(fn) + len(ln)
                    best_first = fn
                    best_last = ln
            
            # B. INSERT MASTER
            # Use raw SQL for performance
            ins_sql = text("""
                INSERT INTO public.master_author (preferred_given_name, preferred_family_name)
                VALUES (:fn, :ln)
                RETURNING id
            """)
            result = conn.execute(ins_sql, {"fn": best_first, "ln": best_last})
            master_id = result.fetchone()[0]
            
            # C. LINK RAW RECORDS
            # Convert list of IDs to tuple for SQL IN clause
            if len(cluster_ids) == 1:
                ids_tuple = f"({cluster_ids[0]})"
            else:
                ids_tuple = tuple(cluster_ids)
                
            upd_sql = text(f"""
                UPDATE public.raw_author_record
                SET master_author_id = :mid,
                    processing_status = 'linked',
                    processed_at = NOW()
                WHERE id IN {ids_tuple}
            """)
            conn.execute(upd_sql, {"mid": master_id})
            
    # print(f"Processed {len(clusters)} clusters/authors.")

if __name__ == "__main__":
    run_disambiguation()