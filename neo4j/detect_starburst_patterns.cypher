// Parameters: $lookback_hours, $minimum_source_accounts,
// $minimum_intermediaries, and $limit.
// Detect a multi-hop funnel without relying on simulator syndicate labels:
// many sources -> multiple intermediaries -> one destination account.
WITH datetime() - duration({hours: $lookback_hours}) AS cutoff
MATCH (source:Account)-[inbound:TRANSFERRED_TO]->(intermediary:Account)
      -[outbound:TRANSFERRED_TO]->(sink:Account)
WHERE source <> intermediary
  AND intermediary <> sink
  AND source <> sink
  AND inbound.occurred_at >= cutoff
  AND outbound.occurred_at >= cutoff
  AND inbound.occurred_at <= outbound.occurred_at
WITH sink,
     collect(DISTINCT source.account_id) AS source_account_ids,
     collect(DISTINCT intermediary.account_id) AS intermediary_account_ids,
     collect(DISTINCT inbound) AS inbound_transfers,
     collect(DISTINCT outbound) AS outbound_transfers,
     max(outbound.occurred_at) AS latest_transfer_at
WHERE size(source_account_ids) >= $minimum_source_accounts
  AND size(intermediary_account_ids) >= $minimum_intermediaries
RETURN 'starburst:' + sink.account_id AS id,
       sink.account_id AS sink_account_id,
       source_account_ids,
       intermediary_account_ids,
       size(source_account_ids) AS source_count,
       size(intermediary_account_ids) AS intermediary_count,
       size(inbound_transfers) AS inbound_transfer_count,
       size(outbound_transfers) AS outbound_transfer_count,
       toString(latest_transfer_at) AS latest_transfer_at
ORDER BY source_count DESC, intermediary_count DESC, latest_transfer_at DESC
LIMIT $limit;
