MODEL (
  name conformed.price_history,
  kind VIEW,
  cron '@daily'
);

SELECT
  *
FROM interim.stock_history