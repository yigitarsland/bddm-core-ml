import jellyfish # You still need this: pip install jellyfish

# --- 1. MOCK DATABASE (Simulating your scraped tables) ---

# Your "LastName" table example
DB_NAMES = [
    {"author_id": "o_001", "source": "orcid", "last_name": "Smith", "first_name": "John", "currently_used": True},
    {"author_id": "o_002", "source": "orcid", "last_name": "Doe", "first_name": "Jane", "currently_used": True},
    {"author_id": "o_003", "source": "orcid", "last_name": "Madjarov", "first_name": "Gjorgji", "currently_used": True},
    {"author_id": "d_A", "source": "dblp", "last_name": "Smith", "first_name": "J.", "currently_used": True},
    {"author_id": "d_B", "source": "dblp", "last_name": "Smith", "first_name": "John A.", "currently_used": True},
    {"author_id": "d_C", "source": "dblp", "last_name": "Doe", "first_name": "John", "currently_used": True},
]

# Your "PastNames" table
DB_PAST_NAMES = [
    {"author_id": "o_002", "past_name": "Jane Williams"},
]

# Your "Affiliation" table (Universities)
DB_AFFILIATIONS = [
    {"author_id": "o_001", "affiliation": "University of California"},
    {"author_id": "d_A", "affiliation": "Univ. of California"},
    {"author_id": "d_B", "affiliation": "Stanford University"},
    {"author_id": "o_002", "affiliation": "MIT"},
    {"author_id": "d_C", "affiliation": "MIT"},
]

# Your "Publications" table (References)
DB_PUBLICATIONS = [
    {"author_id": "o_001", "title": "A Study on Data"},
    {"author_id": "o_001", "title": "Paper Two"},
    {"author_id": "d_A", "title": "A Study on Data"},
    {"author_id": "d_A", "title": "Paper Three"},
    {"author_id": "o_002", "title": "Intro to AI"},
    {"author_id": "d_C", "title": "Intro to AI"},
]

# (Mock data for Co-Authors, Identifiers, Keywords would follow the same structure)

# --- 2. THE ALGORITHM'S DATABASE-PARSING FUNCTIONS ---

def build_profile_from_db(author_id, source):
    """
    STAGE 2: TRANSFORMATION
    This function queries all the mock DB tables to build 
    a single, clean AuthorProfile object.
    """
    profile = {
        "full_name": "", "past_names": [], "identifiers": {"orcid": None, "dblp": None},
        "affiliations": [], "co_authors": [], "publication_titles": [], "keywords": []
    }
    
    # Set ID
    if source == 'orcid':
        profile["identifiers"]["orcid"] = author_id
    else:
        profile["identifiers"]["dblp"] = author_id

    # 1. Get current name
    for row in DB_NAMES:
        if row["author_id"] == author_id:
            profile["full_name"] = f"{row['first_name']} {row['last_name']}"
            break
            
    # 2. Get past names
    for row in DB_PAST_NAMES:
        if row["author_id"] == author_id:
            profile["past_names"].append(row["past_name"])
            
    # 3. Get affiliations
    for row in DB_AFFILIATIONS:
        if row["author_id"] == author_id:
            profile["affiliations"].append(row["affiliation"])
            
    # 4. Get publication titles
    for row in DB_PUBLICATIONS:
        if row["author_id"] == author_id:
            profile["publication_titles"].append(row["title"])

    # (Add queries for co-authors, keywords, etc.)
    return profile


def process_disambig_batches(orcid_batch_ids, dblp_batch_ids):
    """
    STAGE 1: BLOCKING
    This is the main function that parses the batches and
    manages the entire pipeline.
    """
    
    print("--- Starting Stage 1: Blocking ---")
    
    # 1. Create blocking indexes from the LastName table
    # A real blocking key might be "lastname_firstinitial"
    orcid_blocks = {}
    dblp_blocks = {}
    
    for row in DB_NAMES:
        if row["author_id"] in orcid_batch_ids:
            key = row["last_name"].lower() # Simple block on last name
            orcid_blocks.setdefault(key, []).append(row["author_id"])
            
        if row["author_id"] in dblp_batch_ids:
            key = row["last_name"].lower()
            dblp_blocks.setdefault(key, []).append(row["author_id"])

    # 2. Generate candidate pairs
    candidate_pairs = []
    common_blocks = set(orcid_blocks.keys()).intersection(dblp_blocks.keys())
    
    for key in common_blocks:
        for orcid_id in orcid_blocks[key]:
            for dblp_id in dblp_blocks[key]:
                candidate_pairs.append((orcid_id, dblp_id))
                
    total_comparisons = len(orcid_batch_ids) * len(dblp_batch_ids)
    print(f"Blocking complete.")
    print(f"Reduced {total_comparisons} possible comparisons to {len(candidate_pairs)} candidate pairs.")
    print("--- Starting Stages 2 & 3: Transformation & Scoring ---")

    # 3. Process candidate pairs (Stages 2 and 3)
    results = []
    for orcid_id, dblp_id in candidate_pairs:
        
        # STAGE 2: Transformation
        profile_a = build_profile_from_db(orcid_id, 'orcid')
        profile_b = build_profile_from_db(dblp_id, 'dblp')
        
        # STAGE 3: Scoring
        score = calculate_match_score(profile_a, profile_b)
        
        decision = "NO_MATCH"
        if score > 0.7: # Our decision threshold
            decision = "MATCH"
            
        results.append((orcid_id, dblp_id, score, decision))
        
    return results

# --- 3. STAGE 3: THE SCORING ALGORITHM (FROM PREVIOUS STEP) ---

# (Helper functions for confidence, similarity, etc. go here)
# ... (get_name_confidence, get_affiliation_confidence)
# ... (calculate_name_similarity, calculate_jaccard_similarity, calculate_id_similarity)

# Mock confidence functions
def get_name_confidence(name): return 0.5
def get_affiliation_confidence(affil): return 0.5

# Similarity helpers
def calculate_name_similarity(profile_a, profile_b):
    names_a = {str(profile_a["full_name"]).lower()} | {str(n).lower() for n in profile_a["past_names"]}
    names_b = {str(profile_b["full_name"]).lower()} | {str(n).lower() for n in profile_b["past_names"]}
    best_score = 0.0
    for n_a in names_a:
        for n_b in names_b:
            score = jellyfish.jaro_winkler_similarity(n_a, n_b)
            if score > best_score: best_score = score
    return best_score

def calculate_jaccard_similarity(list_a, list_b):
    set_a = {str(item).lower() for item in list_a if item}
    set_b = {str(item).lower() for item in list_b if item}
    if not set_a and not set_b: return 1.0
    if not set_a or not set_b: return 0.0
    return len(set_a.intersection(set_b)) / len(set_a.union(set_b))

def calculate_id_similarity(profile_a, profile_b):
    orcid_a = profile_a.get("identifiers", {}).get("orcid")
    orcid_b = profile_b.get("identifiers", {}).get("orcid")
    if orcid_a and orcid_b and orcid_a == orcid_b: return 1.0
    return 0.0

def calculate_match_score(profile_a, profile_b):
    """
    STAGE 3: SCORING
    This is the dynamic-weight algorithm.
    """
    W_BASE = {"ID": 0.40, "PUBS": 0.25, "NAME": 0.15, "COAUTHORS": 0.10, "AFFILIATION": 0.05, "KEYWORDS": 0.05}
    S = {} # Scores
    S["ID"] = calculate_id_similarity(profile_a, profile_b)
    S["NAME"] = calculate_name_similarity(profile_a, profile_b)
    S["PUBS"] = calculate_jaccard_similarity(profile_a["publication_titles"], profile_b["publication_titles"])
    S["COAUTHORS"] = calculate_jaccard_similarity(profile_a["co_authors"], profile_b["co_authors"])
    S["AFFILIATION"] = calculate_jaccard_similarity(profile_a["affiliations"], profile_b["affiliations"])
    S["KEYWORDS"] = calculate_jaccard_similarity(profile_a["keywords"], profile_b["keywords"])
    
    C = {} # Confidence
    C["ID"] = 1.0
    C["PUBS"] = 0.9
    C["COAUTHORS"] = 0.8
    C["KEYWORDS"] = 0.7
    C["NAME"] = (get_name_confidence(profile_a["full_name"]) + get_name_confidence(profile_b["full_name"])) / 2.0
    avg_conf_a = sum(get_affiliation_confidence(a) for a in profile_a["affiliations"]) / (len(profile_a["affiliations"]) or 1)
    avg_conf_b = sum(get_affiliation_confidence(a) for a in profile_b["affiliations"]) / (len(profile_b["affiliations"]) or 1)
    C["AFFILIATION"] = (avg_conf_a + avg_conf_b) / 2.0
    
    # Calculate Dynamic Weights
    RAW_W = {}
    for key in W_BASE:
        if S[key] > 0.1: RAW_W[key] = W_BASE[key] * C[key]
        else: RAW_W[key] = W_BASE[key] * 0.1
            
    total_raw_weight = sum(RAW_W.values())
    DYN_W = {}
    if total_raw_weight == 0: return 0.0
    for key in RAW_W: DYN_W[key] = RAW_W[key] / total_raw_weight

    # Calculate Final Score
    final_score = 0.0
    for key in W_BASE:
        final_score += S[key] * DYN_W[key]
        
    print(f"\nComparing {profile_a['full_name']} ({profile_a['identifiers']['orcid']}) vs. {profile_b['full_name']} ({profile_b['identifiers']['dblp']})")
    print(f"FINAL SCORE: {final_score:.4f}")
    return final_score


# --- 4. RUN THE ENTIRE PIPELINE ---

if __name__ == "__main__":
    
    # Simulate getting batches of IDs
    all_orcid_ids = ["o_001", "o_002", "o_003"]
    all_dblp_ids = ["d_A", "d_B", "d_C"]
    
    # Run the main processing pipeline
    final_results = process_disambig_batches(all_orcid_ids, all_dblp_ids)
    
    print("\n--- FINAL DISAMBIGUATION RESULTS ---")
    for orcid_id, dblp_id, score, decision in final_results:
        print(f"Pair: ({orcid_id}, {dblp_id}) | Score: {score:.2f} | Decision: {decision}")