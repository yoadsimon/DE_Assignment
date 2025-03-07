# Section 2: Marketing Data Modeling Challenge 
## 1. Data Model Overview
A single fact table (fact_ad_performance) captures the metrics (cost, impressions, clicks) and frequently changing fields (bid, label). All hierarchical details—Account, Sub Account, Portfolio, Campaign, Ad Group, Ad—are combined into one dimension table (dim_ad_hierarchy). Three additional dimensions handle date (dim_date), device (dim_device), and geo (dim_geo). This star schema reduces complexity and simplifies reporting queries.

## 2. Hierarchy & SCD Type 2
dim_ad_hierarchy includes valid_from and valid_to columns for slowly changing attributes. When a dimension value (e.g., campaign_name) changes, the older record is closed by setting valid_to, and a new record is inserted. This approach (SCD Type 2) retains full historical context of the hierarchical attributes without impacting existing fact records.

## 3. Frequently Changing Fields
bid and label can change hourly. Storing them in fact_ad_performance ensures that each data load reflects the current values without overwriting dimension rows. This separation accommodates the frequent nature of these fields.

## 4. Additional Dimensions
• dim_date stores time-based fields (year, quarter, month, etc.) to support standard time aggregations.  
• dim_device references device details and can be extended if new attributes become relevant.  
• dim_geo stores geographic info, allowing deeper regional analytics and expansions (cities, regions, etc.) without altering the fact table schema.

## 5. Business Requirements Coverage
• Multiple Platforms: Both fact and dimension structures are platform-agnostic, so Google Ads, Facebook, Bing, or others map easily into the same schema.  
• Historical + Real-Time: SCD Type 2 preserves past data for analysis, while hourly or daily fact updates support near real-time insights.  
• Hierarchical Structure: Combining all levels in one dimension table avoids multiple joins.  
• Data Quality: Required fields (cost, impressions, clicks, bid, device, geo) are NOT NULL, while optional fields (click_date, view_date) can be NULL.

## 6. Trade-Offs
• Star vs. Snowflake: The star schema keeps queries straightforward at the cost of some denormalization.  
• Overwriting vs. SCD Type 2: Overwriting loses historical detail; SCD Type 2 provides a complete history.  
• Device & Geo as Separate Dimensions: Isolating these attributes keeps the fact table slim and allows flexible attribute expansions later on.

## 7. Data Quality & Indexing
• Foreign keys maintain consistency between fact_ad_performance and dimension tables.  
• The unique constraint on (ad_id, valid_from) avoids duplicate dimension records for the same ad version.  
• Indexes on foreign keys (ad_hierarchy_key, date_key, device_key, geo_key) speed up filtering and group-by queries.  
• Partitioning fact_ad_performance by date_key could further improve performance at high data volumes.

## 8. Conclusion
This star schema efficiently stores hierarchical marketing data, supports frequent updates, and retains historical dimension details. Placing rapidly changing fields (bid, label) in the fact table eliminates unnecessary dimension updates, while splitting date, device, and geo into separate dimensions provides flexibility and clarity for analytics.