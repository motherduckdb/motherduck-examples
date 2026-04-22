# churn prediction with Python ML

This example shows a simple churn workflow in the order most teams actually use it:

1. create a dataset
2. train a model
3. predict churn for the current customer population

The repo includes dbt models to shape customer history into features and labels, and a Python script to train and evaluate the model. By default, the Python script uses the IBM Telco churn dataset so you can run it immediately. If you later replace the sample data with your own larger historical dataset, you can train on your own dbt-built tables instead.

## before you start

Write down what churn means in your business. For example, you might define churn as a cancellation in the next 30 days, or no return activity in the next 60 days. That definition becomes the target your model will learn.

For churn modeling, you usually need four kinds of source data:

- a customer table with one row per customer
- a subscription or contract table
- an activity or usage table
- a payment or billing table

You do not need a perfect schema to start. You need a clean one.

## step 1: install the project

Install the dependencies:

```sh
uv sync
```

This installs dbt, DuckDB, pandas, scikit-learn, lifelines, and the rest of the packages used in the example.

## step 2: create the dataset

The first job is not training. It is building a dataset the model can learn from.

In this repo, the dataset is built in dbt. The sample data is loaded with seeds, then dbt creates staged tables, historical labels, historical features, and a current-day scoring table.

Load the sample data:

```sh
uv run dbt seed --profiles-dir . --full-refresh
```

Build the dataset:

```sh
uv run dbt build --profiles-dir . --exclude resource_type:seed
```

After this step, the most important tables are:

- `fct_customer_features_historical`
- `fct_customer_churn_labels`
- `fct_customer_churn_scores_daily`

Think of them like this:

- `fct_customer_features_historical` is the training input
- `fct_customer_churn_labels` is the training target
- `fct_customer_churn_scores_daily` is the current prediction dataset

If you want to inspect the current score table:

```sh
uv run dbt show --profiles-dir . --select fct_customer_churn_scores_daily
```

### what the training dataset means

Each row in the historical feature table represents one customer at one snapshot date. The columns describe what was known about that customer on that date. Those columns are the features.

The label table tells you whether that same customer churned after the snapshot date. That is the target.

This time split matters. Features come from the snapshot date. Labels come from the future. That is what makes the training setup valid.

### what features usually look like

Common churn features include:

- days since last activity
- number of events in the last 30, 60, or 90 days
- spend in recent windows
- failed payments
- complaints
- customer tenure
- renewal status

In this repo, those fields are already built for you in the dbt models.

## step 3: train the model

Once the dataset exists, you can train the model.

Start with logistic regression. It is the right first model for churn because churn is usually a binary target, and logistic regression gives you a probability rather than only a yes or no answer.

Run the training script on the IBM Telco dataset:

```sh
uv run python scripts/train_python_churn_models.py --source ibm_telco
```

The script does the following:

1. loads the dataset
2. prepares the target column
3. splits the data into train, validation, and test sets
4. preprocesses numeric and categorical columns
5. trains logistic regression and a few comparison models
6. picks the best model on validation data
7. calibrates the selected model
8. evaluates it on the test set

The outputs are written under:

```text
artifacts/python_models/
```

The main files are:

- `model_metrics.csv`
- `test_predictions.csv`
- `top_feature_importance.csv`
- `run_summary.json`
- `best_model.joblib`

### what the metrics mean

The most useful fields are:

- `roc_auc`, which tells you how well the model ranks churners above non-churners
- `average_precision`, which is useful when churn is imbalanced
- `brier_score`, which tells you whether the probabilities are calibrated

The model also produces a churn probability for each test row. That probability is usually more useful than a hard yes or no prediction because it lets you rank customers by risk.

## step 4: train on your own data

After you understand the IBM benchmark, switch to your own history.

To do that, replace the sample source data with your own customer, subscription, activity, and payment data, then rebuild dbt:

```sh
uv run dbt build --profiles-dir . --exclude resource_type:seed
```

When your historical dataset is large enough, train on the dbt-built feature matrix:

```sh
uv run python scripts/train_python_churn_models.py --source dbt --database local.db
```

The script refuses to train on the tiny bundled sample through this path because the sample is too small for useful machine learning. That check is intentional.

## step 5: predict churn

Once you have a trained model, the final step is prediction.

In practice, prediction means taking the current customer feature table and assigning each row a churn probability. In this repo, the current-day feature and score output is centered on:

```text
fct_customer_churn_scores_daily
```

That table is the current prediction layer for the eligible customer population.

If you want Python model outputs written back into DuckDB as tables, run:

```sh
uv run python scripts/train_python_churn_models.py --source ibm_telco --database local.db --write-schema science
```

That creates prediction and metric tables in the selected schema.

If you want to predict against MotherDuck instead of local DuckDB, set up `.env` first:

```sh
cp .env.example .env
```

Then set:

```sh
MOTHERDUCK_TOKEN=your_token_here
MOTHERDUCK_DATABASE=subscription_churn
```

Build in MotherDuck:

```sh
uv run dbt seed --profiles-dir . --target prod --full-refresh
uv run dbt build --profiles-dir . --target prod --select tag:churn_daily+ --exclude resource_type:seed
```

Then write prediction outputs back to MotherDuck:

```sh
uv run python scripts/train_python_churn_models.py \
  --source ibm_telco \
  --database "md:${MOTHERDUCK_DATABASE}" \
  --write-schema science
```

## if you are using your own data

Use this order:

1. define churn
2. clean the raw customer history
3. build historical features
4. build historical labels
5. train the model
6. review the metrics
7. score the current customer population

Do not skip straight to training. If the dataset is wrong, the model will be wrong too.

## the shortest possible path

If you just want the fastest end-to-end run:

Create the dataset:

```sh
uv sync
uv run dbt seed --profiles-dir . --full-refresh
uv run dbt build --profiles-dir . --exclude resource_type:seed
```

Train the model:

```sh
uv run python scripts/train_python_churn_models.py --source ibm_telco
```

Inspect the current predictions:

```sh
uv run dbt show --profiles-dir . --select fct_customer_churn_scores_daily
```

That is the core workflow this example is meant to teach.
