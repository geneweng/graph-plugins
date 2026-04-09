---
name: graphify-to-kuzu
description: >
  Convert a graphify graph.json (NetworkX node-link format) into a typed KuzuDB
  graph database with classified node types (File, Class, Function, Method,
  Document, Rationale) and semantic relationship tables (CONTAINS, CALLS,
  INHERITS, IMPORTS, HAS_METHOD, USES, RATIONALE_FOR, REFERENCES, SIMILAR_TO).
  Use this skill whenever the user mentions converting graphify output to Kuzu,
  loading graph.json into a graph database, creating a typed KuzuDB from code
  analysis, or anything involving graphify-to-kuzu, kuzu graph loading, or
  building a queryable code graph. Also trigger when the user has a graph.json
  from graphify and wants to run Cypher queries, explore code relationships, or
  build a structured graph DB from it.
---

# graphify-to-kuzu

Convert `graph.json` (NetworkX node-link format produced by graphify) into a
typed KuzuDB graph database that can be queried with Cypher.

## What it produces

### Node tables

Nodes are classified from graphify's flat node list into typed tables based on
`file_type` and label patterns:

| Table | Detection rule |
|---|---|
| `File` | Label contains a file extension (e.g., `.py`, `.md`, `.ts`) |
| `Class` | CamelCase identifier matching `^[A-Z][A-Za-z0-9_]*$` |
| `Function` | Label ends with `()` |
| `Method` | Label starts with `.` and ends with `()` |
| `Document` | `file_type == "document"` |
| `Rationale` | `file_type == "rationale"` |

All node tables share columns:
`id STRING PK, label STRING, source_file STRING, source_location STRING, community INT64`

### Relationship tables

Each relationship table is multi-FROM-TO, covering all (source_type, target_type)
pairs actually observed in the data:

| Table | Graphify relations | Typical direction |
|---|---|---|
| `CONTAINS` | `contains`, `includes` | * -> * |
| `CALLS` | `calls` | Function/Method -> Function/Method/Class |
| `INHERITS` | `inherits`, `extends`, `implements` | * -> * |
| `IMPORTS` | `imports`, `imports_from` | File -> File/Class/Function/Method |
| `HAS_METHOD` | `method` | Class -> Method/Function |
| `USES` | `uses`, `defines`, `case_of`, `depends_on`, `tracks`, `flows_to`, + misc | * -> * |
| `RATIONALE_FOR` | `rationale_for` | Code node -> Rationale |
| `REFERENCES` | `references`, `conceptually_related_to` | Document/Rationale -> * |
| `SIMILAR_TO` | `semantically_similar_to` | * -> * |

All relationship tables share columns:
`relation STRING, confidence STRING, confidence_score DOUBLE, weight DOUBLE, source_file STRING, source_location STRING`

## Workflow

### 1. Locate graph.json

Default: `graphify-out/graph.json` in the current directory. If the user provides
a different path, use that. If it doesn't exist, tell the user to run graphify
first.

### 2. Install dependencies

```bash
python3 -c "import kuzu, pandas" 2>/dev/null || pip install kuzu pandas -q
```

If the project has a `.venv/`, prefer `.venv/bin/python` and `uv pip install kuzu pandas`.

### 3. Write and run the loader script

Copy `scripts/loader.py` from this skill to `<graph_dir>/load_kuzu.py`.

Before running, update the `GRAPH_JSON` path in the script if the user's
graph.json is not in the same directory. Then run it:

```bash
python3 <graph_dir>/load_kuzu.py
```

The script handles everything:
- Resets any existing `graph.kuzu` database for a clean load
- Reads and classifies all nodes into the 6 typed tables
- Buckets edges into the 9 relationship tables using `REL_RULES`
- Creates Kuzu DDL with multi-FROM-TO declarations for observed type pairs
- Bulk-loads via intermediate CSV files (avoids Kuzu's `KU_UNREACHABLE` bug with pandas object dtypes)
- Prints node/edge counts and a sample Cypher query

### 4. Show summary and example query

After the loader completes, show the node and edge counts from its output.
Then suggest an example Cypher query so the user can start exploring:

```python
import kuzu
conn = kuzu.Connection(kuzu.Database("<graph_dir>/graph.kuzu"))
conn.execute("""
  MATCH (c:Class)-[:HAS_METHOD]->(m:Method)
  RETURN c.label, count(m) AS methods
  ORDER BY methods DESC LIMIT 5
""").get_as_df()
```

## Gotchas

- **Multi-FROM-TO COPY**: Kuzu requires `from='Type', to='Type'` hints when a
  rel table has multiple FROM-TO pairs. The loader groups edges by (from_type,
  to_type) and runs one COPY per group.

- **CSV intermediary**: Direct pandas DataFrame loading can hit `KU_UNREACHABLE`
  on string columns with object dtype. The loader writes intermediate CSVs
  instead, which is more reliable. CSVs are cleaned up after loading.

- **Empty tables skipped**: If zero edges of a given relation kind are found, that
  rel table is not created (Kuzu refuses empty multi-FROM-TO declarations).

- **Unmatched edges**: Edges whose (relation, from_type, to_type) don't match any
  rule are counted as `unmatched` and skipped. Inspect `REL_RULES` in the loader
  and broaden if needed.

- **Re-runnable**: The script wipes `graph.kuzu` at the top, so re-runs are clean.
