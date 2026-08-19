// Persist a normalized weighted PageRank score on every projected Account.
CALL gds.pageRank.write(
  $graph_name,
  {
    relationshipWeightProperty: 'transaction_volume',
    writeProperty: $write_property,
    scaler: 'MinMax',
    maxIterations: $max_iterations,
    dampingFactor: $damping_factor,
    tolerance: $tolerance,
    concurrency: $concurrency
  }
)
YIELD nodePropertiesWritten,
      ranIterations,
      didConverge,
      centralityDistribution,
      preProcessingMillis,
      computeMillis,
      postProcessingMillis,
      writeMillis
RETURN nodePropertiesWritten,
       ranIterations,
       didConverge,
       centralityDistribution,
       preProcessingMillis,
       computeMillis,
       postProcessingMillis,
       writeMillis;
