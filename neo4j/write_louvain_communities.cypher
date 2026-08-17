// Persist a community identifier on every projected Account node.
CALL gds.louvain.write(
  $graph_name,
  {
    relationshipWeightProperty: 'transaction_volume',
    writeProperty: $write_property,
    maxLevels: $max_levels,
    maxIterations: $max_iterations,
    concurrency: $concurrency
  }
)
YIELD communityCount,
      modularity,
      modularities,
      ranLevels,
      nodePropertiesWritten,
      computeMillis,
      writeMillis
RETURN communityCount,
       modularity,
       modularities,
       ranLevels,
       nodePropertiesWritten,
       computeMillis,
       writeMillis;
