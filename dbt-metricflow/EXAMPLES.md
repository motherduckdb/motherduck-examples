# MetricFlow Query Examples

This guide shows common patterns for querying metrics with MetricFlow.

## Prerequisites

```bash
# Activate virtual environment
source metricflow-example/venv/bin/activate

# Navigate to project
cd metricflow-example/ecommerce_metrics

# Make sure data is loaded and models are built
DBT_PROFILES_DIR=.. dbt seed
DBT_PROFILES_DIR=.. dbt run
```

## Basic Queries

### Single Metric

```bash
# Total revenue
DBT_PROFILES_DIR=.. mf query --metrics revenue

# Output:
# revenue
# ---------
# 4584.02
```

### Multiple Metrics

```bash
# Revenue, orders, and customers
DBT_PROFILES_DIR=.. mf query --metrics revenue,orders,customers

# Output:
# revenue    orders    customers
# ---------  --------  -----------
# 4584.02    20        12
```

## Time-Based Queries

### Group by Month

```bash
DBT_PROFILES_DIR=.. mf query --metrics revenue,orders --group-by metric_time__month

# Output:
# metric_time__month      revenue    orders
# --------------------  ---------  --------
# 2024-01-01T00:00:00     1962.29        10
# 2024-02-01T00:00:00     2621.73        10
```

### Group by Day

```bash
DBT_PROFILES_DIR=.. mf query --metrics revenue --group-by metric_time__day --limit 5

# Output:
# metric_time__day        revenue
# --------------------  ---------
# 2024-01-15T00:00:00     150.00
# 2024-01-16T00:00:00     250.50
# 2024-01-17T00:00:00      75.25
# 2024-01-18T00:00:00     100.00
# 2024-01-19T00:00:00     320.00
```

## Filtering

### By Status

```bash
DBT_PROFILES_DIR=.. mf query \
  --metrics revenue,orders \
  --group-by order_id__status

# Output:
# order_id__status    revenue    orders
# ----------------  ---------  --------
# completed           4434.02        18
# cancelled            150.00         2
```

### Time Range

```bash
DBT_PROFILES_DIR=.. mf query \
  --metrics revenue \
  --group-by metric_time__month \
  --start-time 2024-01-01 \
  --end-time 2024-01-31

# Output: Only January data
# metric_time__month      revenue
# --------------------  ---------
# 2024-01-01T00:00:00     1962.29
```

## Derived Metrics

### Revenue per Customer

```bash
DBT_PROFILES_DIR=.. mf query \
  --metrics revenue_per_customer \
  --group-by metric_time__month

# Output:
# metric_time__month      revenue_per_customer
# --------------------  ----------------------
# 2024-01-01T00:00:00                  327.048
# 2024-02-01T00:00:00                  262.173
```

## Advanced Queries

### Multiple Dimensions

```bash
DBT_PROFILES_DIR=.. mf query \
  --metrics revenue,orders \
  --group-by metric_time__month,order_id__status

# Output:
# metric_time__month    order_id__status    revenue    orders
# --------------------  ----------------  ---------  --------
# 2024-01-01T00:00:00   completed           1862.29         9
# 2024-01-01T00:00:00   cancelled            100.00         1
# 2024-02-01T00:00:00   completed           2571.73         9
# 2024-02-01T00:00:00   cancelled             50.00         1
```

### Average Order Value

```bash
DBT_PROFILES_DIR=.. mf query \
  --metrics avg_order_value \
  --group-by metric_time__month

# Output:
# metric_time__month      avg_order_value
# --------------------  -----------------
# 2024-01-01T00:00:00             196.229
# 2024-02-01T00:00:00             262.173
```

## SQL Inspection

### View Generated SQL

```bash
DBT_PROFILES_DIR=.. mf query \
  --metrics revenue \
  --group-by order_id__status \
  --explain

# Output:
# 🔎 SQL:
#
# SELECT
#   status AS order_id__status
#   , SUM(amount) AS revenue
# FROM "ecommerce_local"."main"."fct_orders" orders_src_10000
# GROUP BY
#   status
```

## Listing Available Metrics

### Show All Metrics

```bash
DBT_PROFILES_DIR=.. mf list metrics

# Output:
# ✔ 🌱 We've found 5 metrics.
# • avg_order_value: metric_time, order_id__order_date, order_id__status
# • customers: metric_time, order_id__order_date, order_id__status
# • orders: metric_time, order_id__order_date, order_id__status
# • revenue: metric_time, order_id__order_date, order_id__status
# • revenue_per_customer: metric_time, order_id__order_date, order_id__status
```

### Show All Dimensions

```bash
DBT_PROFILES_DIR=.. mf list dimensions --metrics revenue

# Output lists all dimensions available for the revenue metric
```

## MotherDuck Queries

All the same queries work with MotherDuck by setting the target:

```bash
# Any query from above, just add DBT_TARGET=motherduck
DBT_PROFILES_DIR=.. DBT_TARGET=motherduck mf query \
  --metrics revenue,orders,customers \
  --group-by metric_time__month
```

The SQL generated is identical. MotherDuck's hybrid execution automatically optimizes where computation happens.

## Custom Metrics

Want to add your own metrics? Edit `models/semantic_models.yml`:

```yaml
# Add a new measure
measures:
  - name: cancelled_orders
    agg: sum
    expr: "CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END"

# Add a new metric
metrics:
  - name: cancellation_rate
    type: derived
    label: Order Cancellation Rate
    type_params:
      expr: cancelled_orders / orders
      metrics:
        - cancelled_orders
        - orders
```

Then rebuild and query:

```bash
DBT_PROFILES_DIR=.. dbt run
DBT_PROFILES_DIR=.. mf query --metrics cancellation_rate
```

## Performance Tips

1. **Start small**: Query a subset with `--limit` when exploring
2. **Use time filters**: `--start-time` and `--end-time` reduce data scanned
3. **Check SQL**: Use `--explain` to see what queries are generated
4. **Local first**: Test queries locally before running on MotherDuck

## Common Patterns

### Weekly Reports

```bash
# Revenue and orders by week
DBT_PROFILES_DIR=.. mf query \
  --metrics revenue,orders,customers \
  --group-by metric_time__week
```

### Status Breakdown

```bash
# See performance by order status
DBT_PROFILES_DIR=.. mf query \
  --metrics revenue,orders,avg_order_value \
  --group-by order_id__status
```

### Trend Analysis

```bash
# Monthly trends with all key metrics
DBT_PROFILES_DIR=.. mf query \
  --metrics revenue,orders,customers,avg_order_value,revenue_per_customer \
  --group-by metric_time__month \
  --order-by metric_time__month
```

## Exporting Results

Save query results to a file:

```bash
DBT_PROFILES_DIR=.. mf query \
  --metrics revenue,orders \
  --group-by metric_time__month > results.txt
```

Or pipe to other tools:

```bash
DBT_PROFILES_DIR=.. mf query \
  --metrics revenue \
  --group-by metric_time__day | grep "2024-01"
```

## Need Help?

- Run `mf query --help` for all options
- Run `mf --help` for other commands
- See [dbt docs](https://docs.getdbt.com/docs/build/metricflow-commands) for full MetricFlow CLI reference
