const appDb = db.getSiblingDB("currency_db");

appDb.createCollection("exchange_rates");


appDb.exchange_rates.createIndex(
  { base_currency: 1, target_currency: 1, rate_date: 1 },
  { unique: true, name: "uniq_pair_per_day" }
);

print("currency_db.exchange_rates initialised");
