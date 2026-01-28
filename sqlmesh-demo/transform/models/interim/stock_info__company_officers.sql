MODEL (
  name interim.stock_info__company_officers,
  kind SCD_TYPE_2_BY_TIME (
    unique_key _dlt_id,
    updated_at_name _dlt_load_time
  ),
  grain (symbol, name),
  audits (
    NOT_NULL(columns = (name, symbol))
  ),
  cron '@daily'
);

SELECT
  CO.max_age::BIGINT AS max_age,
  CO.name::TEXT AS name,
  CO.age::BIGINT AS age,
  CO.title::TEXT AS title,
  CO.year_born::BIGINT AS year_born,
  CO.fiscal_year::BIGINT AS fiscal_year,
  CO.total_pay::BIGINT AS total_pay,
  CO.exercised_value::BIGINT AS exercised_value,
  CO.unexercised_value::BIGINT AS unexercised_value,
  SI.symbol::TEXT AS symbol,
  CO._dlt_id::TEXT AS _dlt_id,
  TO_TIMESTAMP(SI._dlt_load_id::DOUBLE) AS _dlt_load_time
FROM stock_data.stock_info__company_officers AS CO
LEFT JOIN stock_data.stock_info AS SI
  ON CO._dlt_parent_id = SI._dlt_id