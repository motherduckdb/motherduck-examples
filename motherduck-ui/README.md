# MotherDuck UI

This is a folder containing a sample script of some data transformation with MotherDuck in the UI. It should be ran one query at a time from the UI.

The sample data file can be found in this folder as `sample_data.csv`.  

## Exercise

Provide the difference between the highest rated, least expensive bottle and the lowest rated, most expensive bottle for bottles in a vintage of 1990 or later.

To calculate highest and lowest rated, use `coalesce(coalesce("WA score","Vinous score"),-1)`, and use `offer_price / qty as price_per_bottle` to calculate price per bottle.