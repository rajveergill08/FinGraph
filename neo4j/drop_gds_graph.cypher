// failIfMissing=false makes repeated community-detection runs idempotent.
CALL gds.graph.drop($graph_name, false)
YIELD graphName
RETURN graphName;
