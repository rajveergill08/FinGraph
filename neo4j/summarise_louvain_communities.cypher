MATCH (account:Account)
WHERE account[$write_property] IS NOT NULL
WITH account
ORDER BY account.account_id
WITH account[$write_property] AS community_id,
     collect(account.account_id) AS account_ids,
     count(*) AS member_count,
     max(coalesce(account.graph_risk_score, 0.0)) AS highest_risk_score,
     avg(coalesce(account.graph_risk_score, 0.0)) AS average_risk_score
RETURN community_id,
       member_count,
       account_ids[..$member_sample_size] AS sample_account_ids,
       round(highest_risk_score, 2) AS highest_risk_score,
       round(average_risk_score, 2) AS average_risk_score
ORDER BY highest_risk_score DESC, member_count DESC, community_id
LIMIT $community_limit;
