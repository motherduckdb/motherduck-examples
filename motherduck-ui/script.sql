-- import sample data set
CREATE OR REPLACE TABLE winelist AS SELECT * FROM read_csv_auto(['sample_data.csv']);

-- load a summary of the data
SUMMARIZE winelist;

/* begin EDA */
-- remove a long column so its easier to see the data
SELECT * EXCLUDE("Wine Name")
FROM winelist;

-- remove obvious bad data
SELECT * EXCLUDE("Wine Name")
FROM winelist
WHERE vintage > 1000;

-- split the unit size into qty and volume
SELECT * EXCLUDE("Unit size"),
  cast(substr("Unit size", 1, instr("Unit size", 'x') - 1) as integer) as qty,
      cast(
        substr(
            "Unit size",
            instr("Unit size", 'x') + 1,
            length("Unit size") - instr("Unit size", 'x') - 2
        ) as integer
    ) as volume_cl,
FROM winelist
WHERE vintage > 1000;

-- format the price as a decimal + add price per bottle
SELECT * EXCLUDE("Unit size"),
  cast(substr("Unit size", 1, instr("Unit size", 'x') - 1) as integer) as qty,
      cast(
        substr(
            "Unit size",
            instr("Unit size", 'x') + 1,
            length("Unit size") - instr("Unit size", 'x') - 2
        ) as integer
    ) as volume_cl,
      cast(
        replace(replace("Offer price", '$', ''), ',', '') as decimal(10, 2)
    ) as offer_price,
  offer_price /(qty*volume_cl) * 75 as price_per_75cl,
  offer_price / qty as price_per_bottle
FROM winelist
WHERE vintage > 1000;

-- exercise
with cte_cheap_but_good as (select * from winelist_clean
where Vintage >= 1990 AND ("WA score" IS NOT NULL OR "Vinous score" IS NOT NULL)
  order by coalesce(coalesce("WA score","Vinous score"),-1) desc, price_per_bottle
limit 1),
  cte_expensive_and_bad as (
select * from winelist_clean
where Vintage >= 1990 and ("WA score" IS NOT NULL OR "Vinous score" IS NOT NULL)
  order by coalesce(coalesce("WA score","Vinous score"),-1) asc, price_per_bottle desc
limit 1)
select a.price_per_bottle - b.price_per_bottle as difference from cte_cheap_but_good a
join cte_expensive_and_bad b on true
