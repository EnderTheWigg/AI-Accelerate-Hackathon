from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
# Necessary import for real News API calls
from newsapi import NewsApiClient 
import json
from datetime import datetime, timedelta # ADDED for incremental sync logic

# --- Constants ---
TABLE_NAME = "NEWS_ARTICLES"
MAX_ARTICLES_PER_REQUEST = 100  # Max page size for News API
MAX_PAGES_TO_FETCH = 5          # Limit total pages to fetch for the combined query

# ----------------------------------------------------
# 1. Schema Definition
# ----------------------------------------------------

def schema(config):
    """Defines the structure of the data that will be replicated.
    FIX: Using the list-of-objects format, which is often more reliable
    for Fivetran SDK validation than the dictionary format."""
    return [
        {
            "table": TABLE_NAME,  # Explicitly defined table name
            "primary_key": ["url", "publishedAt"], 
            "columns": {
                "source_id": "STRING",
                "source_name": "STRING",
                "author": "STRING",
                "title": "STRING",
                "description": "STRING",
                "url": "STRING",
                "urlToImage": "STRING",
                "publishedAt": "TIMESTAMP",
                "content": "STRING",
                # Field to indicate the search term used (the entire query_term string)
                "search_keyword": "STRING" 
            }
        }
    ]

# ----------------------------------------------------
# 2. Update (Data Extraction) Logic - Using /v2/everything
# ----------------------------------------------------

def update(config, state):
    """
    Fetches data using the /v2/everything endpoint, treating the entire query_term 
    as a single large 'OR' search across a limited number of pages.
    Implements incremental sync based on the 'publishedAt' cursor.
    """
    log.warning("Starting News API sync using single broad query.")
    
    # --- Configuration Retrieval ---
    api_key = config.get("api_key")
    if not api_key:
        raise Exception("News API Key not found in configuration.")
        
    query_terms_str = config.get("query_term", "general news") 
    full_query = query_terms_str
    
    # Instantiate the News API Client
    try:
        newsapi = NewsApiClient(api_key=api_key) 
    except NameError:
        log.error("The 'newsapi' Python package (specifically NewsApiClient) must be installed/available.")
        raise

    # ------------------ Incremental Sync Logic ------------------
    # Determines the date range for the News API call
    last_sync_time_str = state.get("articles_cursor")
    
    if last_sync_time_str:
        # Use the time from the previous sync state
        from_date = last_sync_time_str
        log.info(f"Starting incremental sync from: {from_date}")
    else:
        # Initial sync: pull data from the last 30 days (NewsAPI free tier limit)
        thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).isoformat() + 'Z'
        from_date = thirty_days_ago
        log.info(f"Starting initial sync from last 30 days: {from_date}")
        
    # The new cursor value is the time this sync is starting
    new_cursor_value = datetime.utcnow().isoformat() + 'Z' 
    # ------------------------------------------------------------

    # ------------------ Core Sync Logic (Single Query) ------------------
    
    titles_seen_this_run = set()
    articles_to_upsert = []
    
    page = 1
    total_results_fetched = 0
    
    # We loop up to MAX_PAGES_TO_FETCH for the single, broad query
    while page <= MAX_PAGES_TO_FETCH:
        log.info(f"Fetching page {page} for full query.")

        try:
            # 🎯 REAL NEWS API CALL using the get_everything endpoint
            results = newsapi.get_everything(
                q=full_query,
                from_param=from_date, # ADDED: Use cursor for incremental sync
                language='en',
                sort_by='publishedAt',
                page_size=MAX_ARTICLES_PER_REQUEST,
                page=page
            )
            
        except Exception as e:
            # Catch API errors (rate limits, invalid key, etc.) and stop the sync gracefully
            log.error(f"News API request failed for page {page}: {e}. Stopping sync.")
            break 

        articles = results.get('articles', [])
        
        if not articles:
            log.info(f"No more articles found after page {page}.")
            break
        
        # 3. Process articles for the current page
        for article in articles:
            article_title = article.get("title")
            clean_title = article_title.lower().strip() if article_title else None
            
            # Simple Deduplication by Title (within this run)
            if clean_title and clean_title in titles_seen_this_run:
                continue
            if clean_title:
                titles_seen_this_run.add(clean_title)

            # Flatten and Cleanse Data
            cleaned_article = {
                "source_id": article.get('source', {}).get('id'),
                "source_name": article.get('source', {}).get('name'),
                "author": article.get('author'),
                "title": article.get('title'),
                "description": article.get('description'),
                "url": article.get('url'),
                "urlToImage": article.get('urlToImage'),
                "publishedAt": article.get('publishedAt'),
                # Ensure content is stringified, as the schema dictates
                "content": str(article.get('content', '')),
                "search_keyword": full_query  # Record the full search query used
            }
            
            articles_to_upsert.append(cleaned_article)

        total_results_fetched += len(articles)
        page += 1
        
        # Break if the results returned are less than the page size, indicating the end of results
        if len(articles) < MAX_ARTICLES_PER_REQUEST:
            break


    log.info(f"Total unique articles processed this run: {len(articles_to_upsert)}")

    # 4. Send data to Fivetran
    # Upsert prevents duplicates in the warehouse based on the primary key (url, publishedAt)
    yield op.upsert(table=TABLE_NAME, data=articles_to_upsert)
    
    # 5. Checkpoint: Saves a successful state. 
    yield op.checkpoint(state={"articles_cursor": new_cursor_value}) # UPDATED: Save the new cursor
    log.info(f"Checkpoint saved: Next sync will search from {new_cursor_value}")


# ----------------------------------------------------
# 3. Initialize the Connector
# ----------------------------------------------------

connector = Connector(schema=schema, update=update)


if __name__ == "__main__":
    try:
        # Load configuration from the expected file path
        with open("config.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        log.error("config.json not found. Using empty configuration.")
        configuration = {}
    
    # --- Custom JSON Output Logic for Local Debugging ---
    try:
        data_to_output = []
        
        # Manually call the update function to capture yielded operations
        # We start with an empty state ({}) for local testing
        for operation in update(configuration, {}):
            # Check if the operation is an Upsert and targets the correct table
            if isinstance(operation, op.Upsert) and operation.table == TABLE_NAME:
                data_to_output.extend(operation.data)
        
        # Write the captured data to output.json
        if data_to_output:
            with open("output.json", "w") as outfile:
                json.dump(data_to_output, outfile, indent=4)
            log.warning("Successfully wrote extracted articles to output.json for local inspection.")
        else:
            log.warning("No articles were successfully upserted to write to output.json.")

    except Exception as e:
        log.error(f"Error during local execution and JSON output: {e}")
    # ----------------------------------------------------
    
    # Execute the connector logic in debug mode (This will print operations to stdout)
    connector.debug(configuration=configuration)