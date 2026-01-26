/*
    This dbt model processes review data from the toys_and_games table.
    It extracts structured attributes from the reviews such as sentiment, 
    product features, and customer service interactions from the review text.

    You can configure this model directly within this SQL file, which will
    override configurations stated in dbt_project.yml.
*/
{{ config(materialized="table") }}


select parent_asin, prompt_struct_response.*
from
    (
        select
            parent_asin,
            prompt(
                'You are a very helpful assistant. You are given a product review title and test.\n'
                || 'You are required to extract information from the review.\n'
                || 'Here is the title of the review:'
                || '```'
                || title
                || '```'
                || 'Here is the review text:'
                || '```'
                || text
                || '```',
                struct := {
                    -- Sentiment
                    sentiment:'VARCHAR',
                    -- Feature mentions
                    product_features:'VARCHAR[]',
                    pros:'VARCHAR[]',
                    cons:'VARCHAR[]',
                    -- Quality indicators
                    has_size_info:'BOOLEAN',
                    mentions_price:'BOOLEAN',
                    mentions_shipping:'BOOLEAN',
                    mentions_packaging:'BOOLEAN',
                    -- Comparative analysis
                    competitor_mentions:'VARCHAR[]',
                    previous_version_comparison:'BOOLEAN',
                    -- Usage context
                    use_case:'VARCHAR[]',
                    purchase_reason:'VARCHAR[]',
                    time_owned:'VARCHAR',
                    -- Issues and concerns
                    reported_issues:'VARCHAR[]',
                    quality_concerns:'VARCHAR[]',
                    -- Customer service interaction
                    customer_service_interaction:'BOOLEAN',
                    customer_service_sentiment:'VARCHAR'
                },
                struct_descr := {
                    sentiment:'the sentiment of the review, can only take values `positive`, `neutral` or `negative`',
                    product_features:'a list of features mentioned in the review, if none mentioned return empty array',
                    pros:'a list of pros or positive aspects mentioned in the review, if none mentioned return empty array',
                    cons:'a list of cons or negative aspects mentioned in the review, if none mentioned return empty array',
                    has_size_info:'indicates if the review mentions size information',
                    mentions_price:'indicates if the review mentions price information',
                    mentions_shipping:'indicates if the review mentions shipping information',
                    mentions_packaging:'indicates if the review mentions packaging information',
                    competitor_mentions:'a list of competitors mentioned in the review, if none mentioned return empty array',
                    previous_version_comparison:'indicates if the review compares the product to a previous version',
                    use_case:'a list of use cases mentioned in the review, if none return empty array',
                    purchase_reason:'a list of purchase reasons mentioned in the review, if none return empty array',
                    time_owned:'the time the reviewer has owned the product, if mentioned return the time what ever was written in text, if not mentioned return empty string',
                    reported_issues:'a list of issues reported in the review, if none return empty array',
                    quality_concerns:'a list of quality concerns mentioned in the review, if none return empty array',
                    customer_service_interaction:'indicates if the review mentions customer service interaction',
                    customer_service_sentiment:'the sentiment of the customer service interaction, can only take values `positive`, `neutral` or `negative`'
                }
            ) as prompt_struct_response
        from reviews_raw
    )
