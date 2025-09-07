CREATE DATABASE IF NOT EXISTS hades;

CREATE TABLE IF NOT EXISTS hades.prepared_data_json
(
  authId     String,
  dc         String,
  eventTime  DateTime64(3),
  amount     Float64,
  currency   String,
  merchantId String,
  country    String,
  status     String,
  riskScore  Int32,
  cardBin    String,
  deviceType Nullable(String)
)
ENGINE = MergeTree
ORDER BY (authId, eventTime);
