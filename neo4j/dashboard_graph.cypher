// Return a bounded, read-only account-transfer snapshot for visualization.
MATCH (source:Account)-[transfer:TRANSFERRED_TO]->(target:Account)
WHERE (
        $community_id IS NULL
        OR source.louvain_community_id = $community_id
        OR target.louvain_community_id = $community_id
      )
  AND (
        (
          coalesce(source.graph_risk_score, 0.0) >= $minimum_risk_score
          AND coalesce(source.pagerank_score, 0.0) >= $minimum_pagerank_score
        )
        OR (
          coalesce(target.graph_risk_score, 0.0) >= $minimum_risk_score
          AND coalesce(target.pagerank_score, 0.0) >= $minimum_pagerank_score
        )
      )
WITH source, target, transfer
ORDER BY transfer.occurred_at DESC, transfer.transaction_id
LIMIT $edge_limit
RETURN source {
         id: source.account_id,
         label: source.account_id,
         .country,
         .account_type,
         .risk_tier,
         graph_risk_score: coalesce(source.graph_risk_score, 0.0),
         pagerank_score: coalesce(source.pagerank_score, 0.0),
         community_id: source.louvain_community_id
       } AS source,
       target {
         id: target.account_id,
         label: target.account_id,
         .country,
         .account_type,
         .risk_tier,
         graph_risk_score: coalesce(target.graph_risk_score, 0.0),
         pagerank_score: coalesce(target.pagerank_score, 0.0),
         community_id: target.louvain_community_id
       } AS target,
       transfer {
         id: transfer.transaction_id,
         source: source.account_id,
         target: target.account_id,
         amount: coalesce(transfer.amount, 0.0),
         .currency,
         occurred_at: toString(transfer.occurred_at),
         .channel,
         .syndicate_id,
         risk_indicators: coalesce(transfer.risk_indicators, [])
       } AS edge;
