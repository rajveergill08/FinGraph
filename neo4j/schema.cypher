// FinGraph's core graph shape: people own accounts, banks service accounts,
// and transaction edges connect accounts.
CREATE CONSTRAINT person_id_unique IF NOT EXISTS
FOR (person:Person) REQUIRE person.person_id IS UNIQUE;

CREATE CONSTRAINT account_id_unique IF NOT EXISTS
FOR (account:Account) REQUIRE account.account_id IS UNIQUE;

CREATE CONSTRAINT bank_id_unique IF NOT EXISTS
FOR (bank:Bank) REQUIRE bank.bank_id IS UNIQUE;

CREATE CONSTRAINT transaction_id_unique IF NOT EXISTS
FOR ()-[transfer:TRANSFERRED_TO]-() REQUIRE transfer.transaction_id IS UNIQUE;

CREATE INDEX account_country IF NOT EXISTS
FOR (account:Account) ON (account.country);

CREATE INDEX account_risk_tier IF NOT EXISTS
FOR (account:Account) ON (account.risk_tier);

CREATE INDEX transfer_occurred_at IF NOT EXISTS
FOR ()-[transfer:TRANSFERRED_TO]-() ON (transfer.occurred_at);
