# 2025-10-18 Update

## Summary

### review_sync.py
- Implemented a structured ingestion pipeline to fetch user reviews and app metadata from the ChatGPT Android app via the Google Play API.
- Designed two Snowflake tables:
  - **reviews** — user-submitted reviews with rating, version, and timestamp  
  - **app_metadata** — app-level metrics like score, installs, IAP, etc.
- Removed non-essential fields (`user_image`, `thumbs_up`) to keep the schema clean and focused.

### review_update.py

- Handles automated incremental updates to fetch only new reviews monthly via GitHub Actions.
- Queries Snowflake to determine the latest `created_at` timestamp and avoids re-ingesting existing reviews.
  
### analysis.py
- Updated to read directly from Snowflake for consistent integration

## Note
- The Google Play API only exposes metadata for the latest app version, so historical version-level info is not available. 
- To address this, an automated pipeline now captures and archives metadata snapshots into Snowflake.




# 2025-10-15 Update

## Key Findings
- **Data Quality:** Core fields (`content`, `score`, `at`) are highly complete; developer reply fields are mostly missing.  
- **Rating Patterns:** Ratings are heavily right-skewed, dominated by 5-star reviews.  
- **Text Length:** Reviews are extremely short (average 6.3 words, median 3), while negative reviews tend to be longer.  
- **Sentiment Score:** The Pearson correlation between sentiment and rating is **0.398**, and the correlation fluctuates over time, partly due to sarcastic comments.  
- **Sarcasm Detection:** Around **10.8k** reviews show mismatched sentiment–rating pairs, indicating sarcasm or misclassification.  
- **Temporal and Version Trends:** Review volume grew sharply in 2024–2025, with a mild dip in ratings mid-2025. Later versions show stabilized sentiment and rating trends as the user base expanded.  
- **User Clustering:** Four user clusters identified — *concise-positive*, *brief-negative*, *consistently positive*, and *expressive-mixed*. Over time, the user base shifted from quick positive reviewers to a more diverse and critical audience.  

## Note
- There is potential representation bias: although most reviews are 5-star, the majority of linguistically rich comments come from negative reviews.  
- It is advisable to apply stratified sampling or length-based weighting to prevent overrepresentation of either positive or negative reviews.

