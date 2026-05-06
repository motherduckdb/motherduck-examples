select household_id, 'purchase_frequency_z' as feature_name, purchase_dates::double as raw_value from {{ ref('fct_household_features') }}
union all
select household_id, 'promo_targeting_z' as feature_name, pct_amount_campaign_targeted::double as raw_value from {{ ref('fct_household_features') }}
union all
select household_id, 'campaign_coupon_z' as feature_name, pct_amount_campaign_coupon::double as raw_value from {{ ref('fct_household_features') }}
union all
select household_id, 'manufacturer_coupon_z' as feature_name, pct_amount_manufacturer_coupon::double as raw_value from {{ ref('fct_household_features') }}
union all
select household_id, 'private_label_z' as feature_name, pct_amount_private_label::double as raw_value from {{ ref('fct_household_features') }}
union all
select household_id, 'instore_discount_z' as feature_name, pct_amount_instore_discount::double as raw_value from {{ ref('fct_household_features') }}
union all
select household_id, 'basket_value_z' as feature_name, avg_basket_value::double as raw_value from {{ ref('fct_household_features') }}
union all
select household_id, 'discount_depth_z' as feature_name, discount_depth::double as raw_value from {{ ref('fct_household_features') }}
