// Runs once, the first time the MongoDB data volume is initialised.
const appDb = db.getSiblingDB("currency_db");

appDb.createCollection("exchange_rates");

// One document per (base, target, date) -> re-running the DAG upserts, never duplicates.
appDb.exchange_rates.createIndex(
  { base_currency: 1, target_currency: 1, rate_date: 1 },
  { unique: true, name: "uniq_pair_per_day" }
);

print("currency_db.exchange_rates initialised");
