MATCH (account:Account)
WHERE account[$write_property] IS NOT NULL
OPTIONAL MATCH ()-[incoming:TRANSFERRED_TO]->(account)
WITH account,
     count(incoming) AS inbound_transfer_count,
     sum(coalesce(incoming.amount, 0.0)) AS inbound_volume
OPTIONAL MATCH (account)-[outgoing:TRANSFERRED_TO]->()
RETURN account.account_id AS account_id,
       account.country AS country,
       account.louvain_community_id AS louvain_community_id,
       round(coalesce(account.graph_risk_score, 0.0), 2) AS graph_risk_score,
       round(account[$write_property], 6) AS pagerank_score,
       inbound_transfer_count,
       round(inbound_volume, 2) AS inbound_volume,
       count(outgoing) AS outbound_transfer_count,
       round(sum(coalesce(outgoing.amount, 0.0)), 2) AS outbound_volume
ORDER BY pagerank_score DESC, graph_risk_score DESC, account_id
LIMIT $account_limit;
