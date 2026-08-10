// Parameters: $event, produced by src/fingraph/simulator.py.
// This query is intentionally idempotent so a Flink job can safely retry it.
WITH $event AS event
MERGE (source_bank:Bank {bank_id: event.source_account.bank_id})
  SET source_bank.name = event.source_account.bank_name,
      source_bank.country = event.source_account.bank_country
MERGE (source_person:Person {person_id: event.source_account.person_id})
  SET source_person.name = event.source_account.person_name,
      source_person.country = event.source_account.person_country,
      source_person.entity_type = event.source_account.person_type
MERGE (source_account:Account {account_id: event.source_account.account_id})
  SET source_account.country = event.source_account.country,
      source_account.account_type = event.source_account.account_type,
      source_account.risk_tier = event.source_account.risk_tier
MERGE (source_person)-[:OWNS]->(source_account)
MERGE (source_bank)-[:SERVICES]->(source_account)
MERGE (destination_bank:Bank {bank_id: event.destination_account.bank_id})
  SET destination_bank.name = event.destination_account.bank_name,
      destination_bank.country = event.destination_account.bank_country
MERGE (destination_person:Person {person_id: event.destination_account.person_id})
  SET destination_person.name = event.destination_account.person_name,
      destination_person.country = event.destination_account.person_country,
      destination_person.entity_type = event.destination_account.person_type
MERGE (destination_account:Account {account_id: event.destination_account.account_id})
  SET destination_account.country = event.destination_account.country,
      destination_account.account_type = event.destination_account.account_type,
      destination_account.risk_tier = event.destination_account.risk_tier
MERGE (destination_person)-[:OWNS]->(destination_account)
MERGE (destination_bank)-[:SERVICES]->(destination_account)
MERGE (source_account)-[transfer:TRANSFERRED_TO {transaction_id: event.transaction.transaction_id}]->(destination_account)
  SET transfer.occurred_at = datetime(event.transaction.occurred_at),
      transfer.amount = toFloat(event.transaction.amount),
      transfer.currency = event.transaction.currency,
      transfer.origin_ip = event.transaction.origin_ip,
      transfer.channel = event.transaction.channel,
      transfer.syndicate_id = event.transaction.syndicate_id,
      transfer.risk_indicators = event.transaction.risk_indicators;
