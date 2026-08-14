// Parameters: $lookback_hours, $minimum_amount, and $limit.
// A directed three-account loop is returned once, anchored by its lowest ID.
MATCH path = (a:Account)-[ab:TRANSFERRED_TO]->(b:Account)
             -[bc:TRANSFERRED_TO]->(c:Account)
             -[ca:TRANSFERRED_TO]->(a)
WHERE a <> b
  AND b <> c
  AND c <> a
  AND a.account_id < b.account_id
  AND a.account_id < c.account_id
  AND ab.occurred_at >= datetime() - duration({hours: $lookback_hours})
  AND bc.occurred_at >= datetime() - duration({hours: $lookback_hours})
  AND ca.occurred_at >= datetime() - duration({hours: $lookback_hours})
  AND ab.occurred_at <= bc.occurred_at
  AND bc.occurred_at <= ca.occurred_at
  AND ab.amount >= $minimum_amount
  AND bc.amount >= $minimum_amount
  AND ca.amount >= $minimum_amount
WITH path, a, b, c, relationships(path) AS transfers,
     reduce(total = 0.0, transfer IN relationships(path) |
       total + transfer.amount) AS total_amount
RETURN [account IN nodes(path)[0..3] | account.account_id] AS account_ids,
       [transfer IN transfers | transfer.transaction_id] AS transaction_ids,
       round(total_amount, 2) AS total_amount,
       transfers[0].occurred_at AS started_at,
       transfers[2].occurred_at AS completed_at
ORDER BY completed_at DESC
LIMIT $limit;
