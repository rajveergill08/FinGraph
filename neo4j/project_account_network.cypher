// Project transfers as an undirected weighted account network. Parallel
// transfers are collapsed and their amounts summed into transaction_volume.
CALL gds.graph.project(
  $graph_name,
  'Account',
  {
    TRANSFERRED_TO: {
      orientation: 'UNDIRECTED',
      properties: {
        transaction_volume: {
          property: 'amount',
          defaultValue: 1.0,
          aggregation: 'SUM'
        }
      }
    }
  }
)
YIELD graphName, nodeCount, relationshipCount, projectMillis
RETURN graphName, nodeCount, relationshipCount, projectMillis;
