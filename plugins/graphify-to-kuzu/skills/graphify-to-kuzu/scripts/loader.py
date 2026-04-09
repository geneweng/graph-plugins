"""Convert graphify-out/graph.json into a typed KuzuDB database.

Node types:
  File       - source files (.py, .md, ...)
  Class      - classes / types
  Function   - top-level functions
  Method     - class methods (label starts with '.')
  Document   - prose docs (markdown, READMEs)
  Rationale  - extracted docstrings / WHY comments

Rel types (one table per semantic relation):
  CONTAINS              - parent contains child
  CALLS                 - Function/Method calls Function/Method
  INHERITS              - Class/type inheritance
  IMPORTS               - File imports File/symbol
  HAS_METHOD            - Class has Method
  USES                  - generic structural use (broad catch-all)
  RATIONALE_FOR         - Rationale explains code node (either direction)
  REFERENCES            - Document/Rationale references anything
  SIMILAR_TO            - semantic similarity (cross-cutting)
"""
import json
import re
import shutil
from collections import Counter
from pathlib import Path

import kuzu
import pandas as pd

ROOT = Path(__file__).parent
GRAPH_JSON = ROOT / "graph.json"
DB_PATH = ROOT / "graph.kuzu"

ALL_TYPES = {"File", "Class", "Function", "Method", "Document", "Rationale"}

# --- Reset DB ---
if DB_PATH.exists():
    if DB_PATH.is_dir():
        shutil.rmtree(DB_PATH)
    else:
        DB_PATH.unlink()
for wal in ROOT.glob("graph.kuzu*"):
    if wal != DB_PATH:
        wal.unlink()

g = json.loads(GRAPH_JSON.read_text(encoding="utf-8"))
raw_nodes = g["nodes"]
raw_edges = g["links"]

# --- Classify nodes into types ---
FILE_RE = re.compile(r"\.[A-Za-z0-9]{1,5}$")
CLASS_RE = re.compile(r"^[A-Z][A-Za-z0-9_]*$")


def classify(n: dict) -> str:
    ft = n.get("file_type", "")
    label = n.get("label", "") or ""
    if ft == "document":
        return "Document"
    if ft == "rationale":
        return "Rationale"
    # code
    if label.startswith(".") and label.endswith("()"):
        return "Method"
    if label.endswith("()"):
        return "Function"
    if FILE_RE.search(label):
        return "File"
    if CLASS_RE.match(label):
        return "Class"
    # fallback: treat as Function-ish symbol
    return "Function"


# id -> type
node_type = {n["id"]: classify(n) for n in raw_nodes}

print("Node type counts:", Counter(node_type.values()))


def base_row(n):
    return {
        "id": n["id"],
        "label": n.get("label") or "",
        "source_file": n.get("source_file") or "",
        "source_location": n.get("source_location") or "",
        "community": int(n.get("community", -1)),
    }


buckets: dict[str, list] = {
    "File": [], "Class": [], "Function": [],
    "Method": [], "Document": [], "Rationale": [],
}
for n in raw_nodes:
    buckets[node_type[n["id"]]].append(base_row(n))

# --- Classify edges into rel types ---
# Each rule: (set of graphify relation names, allowed from types, allowed to types)
# Using ALL_TYPES where the relation is cross-cutting.
REL_RULES = {
    "CONTAINS":      ({"contains", "includes"},        ALL_TYPES, ALL_TYPES),
    "CALLS":         ({"calls"},                       ALL_TYPES, ALL_TYPES),
    "INHERITS":      ({"inherits", "extends", "implements"}, ALL_TYPES, ALL_TYPES),
    "IMPORTS":       ({"imports", "imports_from"},      ALL_TYPES, ALL_TYPES),
    "HAS_METHOD":    ({"method"},                      ALL_TYPES, ALL_TYPES),
    "USES":          ({"uses", "defines", "case_of", "depends_on",
                       "tracks", "flows_to", "orchestrates",
                       "writes_corrections_to", "produces",
                       "technology_used_in", "replaces",
                       "defines_dependencies_for", "represents",
                       "defines_deployment_strategy_for", "feeds_data_to",
                       "writes_lineage_to", "reused_in", "migrates_to",
                       "routes_through", "assembles_prompts_for", "feeds",
                       "embeds_to", "triggers", "updates",
                       "provides_context_to"},          ALL_TYPES, ALL_TYPES),
    "RATIONALE_FOR": ({"rationale_for"},               ALL_TYPES, ALL_TYPES),
    "REFERENCES":    ({"references", "conceptually_related_to"}, ALL_TYPES, ALL_TYPES),
    "SIMILAR_TO":    ({"semantically_similar_to"},     ALL_TYPES, ALL_TYPES),
}

edge_buckets: dict[str, list] = {k: [] for k in REL_RULES}
unmatched = 0
unmatched_rels = Counter()
for e in raw_edges:
    src, tgt = e["source"], e["target"]
    if src not in node_type or tgt not in node_type:
        continue
    st, tt = node_type[src], node_type[tgt]
    rel = e.get("relation", "")
    placed = False
    for tbl, (rels, from_set, to_set) in REL_RULES.items():
        if rel in rels and st in from_set and tt in to_set:
            edge_buckets[tbl].append({
                "from_id": src,
                "to_id": tgt,
                "from_type": st,
                "to_type": tt,
                "relation": rel,
                "confidence": e.get("confidence") or "",
                "confidence_score": float(e.get("confidence_score", 0.0)),
                "weight": float(e.get("weight", 1.0)),
                "source_file": e.get("source_file") or "",
                "source_location": e.get("source_location") or "",
            })
            placed = True
            break
    if not placed:
        unmatched += 1
        unmatched_rels[rel] += 1

print("Edge counts:", {k: len(v) for k, v in edge_buckets.items()}, "unmatched:", unmatched)
if unmatched_rels:
    print("Unmatched relations:", dict(unmatched_rels.most_common(10)))

# --- Create DB ---
db = kuzu.Database(str(DB_PATH))
conn = kuzu.Connection(db)

NODE_DDL = """
CREATE NODE TABLE {name}(
    id STRING PRIMARY KEY,
    label STRING,
    source_file STRING,
    source_location STRING,
    community INT64
)
"""
for nt in buckets:
    conn.execute(NODE_DDL.format(name=nt))


# Build all FROM-TO pairs actually present per rel table
def distinct_pairs(rows):
    return sorted({(r["from_type"], r["to_type"]) for r in rows})


def kuzu_path(p: Path) -> str:
    """Return a forward-slash path string safe for Kuzu COPY statements."""
    return str(p).replace("\\", "/")


for tbl, rows in edge_buckets.items():
    if not rows:
        continue
    pairs = distinct_pairs(rows)
    from_to_clauses = ",\n    ".join(f"FROM {a} TO {b}" for a, b in pairs)
    ddl = f"""
CREATE REL TABLE {tbl}(
    {from_to_clauses},
    relation STRING,
    confidence STRING,
    confidence_score DOUBLE,
    weight DOUBLE,
    source_file STRING,
    source_location STRING
)
"""
    conn.execute(ddl)

# --- Bulk load nodes via CSV (pipe-delimited to avoid comma-in-data issues) ---
DELIM = "|"


def write_csv(df: pd.DataFrame, path: Path):
    for c in df.select_dtypes(include="object").columns:
        # Replace pipe chars in data to avoid delimiter collisions
        df[c] = df[c].fillna("").astype(str).str.replace("|", "/", regex=False)
    df.to_csv(path, index=False, sep=DELIM)


COPY_OPTS = f'(header=true, delim="{DELIM}")'

for nt, rows in buckets.items():
    if not rows:
        continue
    df = pd.DataFrame(rows).drop_duplicates(subset=["id"])
    csv = ROOT / f"_n_{nt}.csv"
    write_csv(df, csv)
    conn.execute(f'COPY {nt} FROM "{kuzu_path(csv)}" {COPY_OPTS}')
    csv.unlink()

# --- Bulk load edges, one COPY per (from_type, to_type) pair ---
EDGE_COLS = ["from_id", "to_id", "relation", "confidence", "confidence_score",
             "weight", "source_file", "source_location"]
for tbl, rows in edge_buckets.items():
    if not rows:
        continue
    df_all = pd.DataFrame(rows)
    for (ft, tt), sub in df_all.groupby(["from_type", "to_type"]):
        out = sub[EDGE_COLS].copy()
        csv = ROOT / f"_e_{tbl}_{ft}_{tt}.csv"
        write_csv(out, csv)
        conn.execute(f'COPY {tbl} FROM "{kuzu_path(csv)}" (from="{ft}", to="{tt}", header=true, delim="{DELIM}")')
        csv.unlink()

# --- Verify ---
print("\n=== Loaded ===")
for nt in buckets:
    r = conn.execute(f"MATCH (n:{nt}) RETURN count(*) AS n").get_as_df()
    print(f"  {nt:10} {int(r.iloc[0]['n']):>6}")
print()
for tbl in edge_buckets:
    try:
        r = conn.execute(f"MATCH ()-[r:{tbl}]->() RETURN count(*) AS n").get_as_df()
        print(f"  {tbl:14} {int(r.iloc[0]['n']):>6}")
    except Exception as ex:
        print(f"  {tbl}: {ex}")

print("\nTop classes by method count:")
print(conn.execute("""
MATCH (c:Class)-[:HAS_METHOD]->(m)
RETURN c.label AS class, count(m) AS methods
ORDER BY methods DESC LIMIT 5
""").get_as_df().to_string(index=False))
