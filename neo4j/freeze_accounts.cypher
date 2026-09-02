// Create an auditable containment case only when every requested account exists.
MATCH (account:Account)
WHERE account.account_id IN $account_ids
WITH collect(DISTINCT account) AS accounts
WHERE size(accounts) = size($account_ids)
CREATE (containment_case:ContainmentCase {
  case_id: $case_id,
  status: 'frozen',
  reason: $reason,
  pattern_id: $pattern_id,
  created_at: datetime($frozen_at)
})
FOREACH (account IN accounts |
  SET account.account_status = 'frozen',
      account.frozen_at = datetime($frozen_at),
      account.freeze_case_id = $case_id
)
WITH containment_case, accounts
UNWIND accounts AS account
MERGE (account)-[:FROZEN_IN]->(containment_case)
WITH containment_case, account
ORDER BY account.account_id
WITH containment_case, collect(account.account_id) AS account_ids
RETURN containment_case.case_id AS case_id,
       containment_case.status AS status,
       containment_case.reason AS reason,
       containment_case.pattern_id AS pattern_id,
       toString(containment_case.created_at) AS frozen_at,
       account_ids;
