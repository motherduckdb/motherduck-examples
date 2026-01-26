MODEL (
  name interim.stock_options,
  kind SCD_TYPE_2_BY_TIME (
    unique_key contract_symbol,
    updated_at_name _dlt_load_time
  ),
  grain (contract_symbol, strike, type, expiration, symbol),
  audits (
    NOT_NULL(columns = (
      contract_symbol
    ))
  ),
  cron '@daily'
);

SELECT
  contract_symbol::TEXT AS contract_symbol,
  last_trade_date::TIMESTAMP AS last_trade_date,
  strike::DOUBLE AS strike,
  last_price::DOUBLE AS last_price,
  bid::DOUBLE AS bid,
  ask::DOUBLE AS ask,
  change::DOUBLE AS change,
  percent_change::DOUBLE AS percent_change,
  open_interest::BIGINT AS open_interest,
  implied_volatility::DOUBLE AS implied_volatility,
  in_the_money::BOOLEAN AS in_the_money,
  contract_size::TEXT AS contract_size,
  currency::TEXT AS currency,
  type::TEXT AS type,
  expiration::TEXT AS expiration,
  symbol::TEXT AS symbol,
  _dlt_load_id::TEXT AS _dlt_load_id,
  _dlt_id::TEXT AS _dlt_id,
  volume::DOUBLE AS volume,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_load_time
FROM stock_data.stock_options