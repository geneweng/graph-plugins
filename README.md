# graph-plugins

A collection of Claude Code plugins for working with graph databases.

## Plugins

### graphify-to-kuzu

Convert a [graphify](https://github.com/xyz/graphify) `graph.json` (NetworkX node-link format) into a typed [KuzuDB](https://kuzudb.com/) graph database, queryable with Cypher.

**Node types:** File, Class, Function, Method, Document, Rationale

**Relationship tables:** CONTAINS, CALLS, INHERITS, IMPORTS, HAS_METHOD, USES, RATIONALE_FOR, REFERENCES, SIMILAR_TO

#### Installation

```bash
claude install xyz/graph-plugins
```

#### Usage

Once installed, the skill triggers when you ask Claude Code to convert graphify output to Kuzu, load a `graph.json` into a graph database, or build a queryable code graph.

The skill will:

1. Locate `graphify-out/graph.json` (or a user-specified path)
2. Install dependencies (`kuzu`, `pandas`)
3. Run the loader script to classify nodes and edges into typed tables
4. Output node/edge counts and a sample Cypher query

#### Example query

```python
import kuzu

conn = kuzu.Connection(kuzu.Database("graphify-out/graph.kuzu"))
conn.execute("""
  MATCH (c:Class)-[:HAS_METHOD]->(m:Method)
  RETURN c.label, count(m) AS methods
  ORDER BY methods DESC LIMIT 5
""").get_as_df()
```

## License

MIT
