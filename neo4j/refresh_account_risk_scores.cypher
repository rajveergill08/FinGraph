// Parameters: $lookback_hours, $high_risk_countries, and $volume_unit.
// The score is explainable and capped at 100; Week 3 GDS will enrich it.
WITH datetime() - duration({hours: $lookback_hours}) AS cutoff
MATCH (account:Account)
OPTIONAL MATCH (account)-[transfer:TRANSFERRED_TO]-(counterparty:Account)
WHERE transfer.occurred_at >= cutoff
WITH account,
     count(DISTINCT transfer.transaction_id) AS transaction_count,
     count(DISTINCT counterparty.account_id) AS counterparty_count,
     coalesce(sum(transfer.amount), 0.0) AS total_volume,
     reduce(indicator_count = 0, relationship IN collect(DISTINCT transfer) |
       indicator_count + size(coalesce(relationship.risk_indicators, []))) AS indicator_count
WITH account, transaction_count, counterparty_count, total_volume, indicator_count,
     CASE account.risk_tier
       WHEN 'critical' THEN 30.0
       WHEN 'high' THEN 22.0
       WHEN 'medium' THEN 12.0
       ELSE 0.0
     END AS tier_points,
     CASE WHEN account.country IN $high_risk_countries THEN 15.0 ELSE 0.0 END AS country_points,
     CASE WHEN counterparty_count * 2.0 > 20.0 THEN 20.0
          ELSE counterparty_count * 2.0 END AS counterparty_points,
     CASE WHEN total_volume / $volume_unit > 20.0 THEN 20.0
          ELSE total_volume / $volume_unit END AS volume_points,
     CASE WHEN indicator_count * 3.0 > 15.0 THEN 15.0
          ELSE indicator_count * 3.0 END AS indicator_points
WITH account, transaction_count, counterparty_count, total_volume,
     tier_points, country_points, counterparty_points, volume_points, indicator_points,
     tier_points + country_points + counterparty_points + volume_points
       + indicator_points AS raw_score
WITH account, transaction_count, counterparty_count, total_volume,
     tier_points, country_points, counterparty_points, volume_points, indicator_points,
     CASE WHEN raw_score > 100.0 THEN 100.0 ELSE round(raw_score, 2) END AS risk_score
SET account.graph_risk_score = risk_score,
    account.graph_risk_updated_at = datetime()
RETURN account.account_id AS account_id,
       risk_score,
       transaction_count,
       counterparty_count,
       round(total_volume, 2) AS total_volume,
       {
         tier: tier_points,
         country: country_points,
         counterparties: counterparty_points,
         volume: round(volume_points, 2),
         indicators: indicator_points
       } AS score_components
ORDER BY risk_score DESC, account_id;
