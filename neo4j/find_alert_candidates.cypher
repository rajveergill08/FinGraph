// Return bounded, read-only accounts that meet the alert threshold.
MATCH (account:Account)
WHERE coalesce(account.graph_risk_score, 0.0) >= $minimum_risk_score
OPTIONAL MATCH (account)-[transfer:TRANSFERRED_TO]-(counterparty:Account)
WITH account,
     count(DISTINCT transfer.transaction_id) AS transaction_count,
     count(DISTINCT counterparty.account_id) AS counterparty_count,
     max(transfer.occurred_at) AS latest_transfer_at
RETURN account.account_id AS account_id,
       coalesce(account.graph_risk_score, 0.0) AS graph_risk_score,
       account.risk_tier AS risk_tier,
       account.country AS country,
       coalesce(account.pagerank_score, 0.0) AS pagerank_score,
       account.louvain_community_id AS community_id,
       transaction_count,
       counterparty_count,
       CASE
         WHEN latest_transfer_at IS NULL THEN NULL
         ELSE toString(latest_transfer_at)
       END AS latest_transfer_at
ORDER BY graph_risk_score DESC, pagerank_score DESC, account_id
LIMIT $limit;
