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
  offer_price /(qty*volume_cl) * 75 as price_per_bottle
FROM winelist
WHERE vintage > 1000;