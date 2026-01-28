MODEL (
  name mart.stock_price_by_day,
  kind VIEW,
  grain (trade_date, stock_symbol),
  audits (
    UNIQUE_COMBINATION_OF_COLUMNS(columns := (trade_date, stock_symbol)),
    NOT_NULL(columns := (trade_date, stock_symbol))
  )
);

SELECT
  c.symbol AS stock_symbol,
  c.shares_outstanding,
  sp.close,
  sp.trade_date,
  ROUND(c.shares_outstanding::REAL * sp.close::REAL, 0) AS market_cap
FROM conformed.company_info AS c
LEFT JOIN conformed.price_history AS sp
  ON c.symbol = sp.symbol
ORDER BY
  c.symbol,
  sp.trade_date