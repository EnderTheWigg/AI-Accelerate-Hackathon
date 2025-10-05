

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
# Necessary import for real News API calls
from newsapi import NewsApiClient 
import json

# --- Constants ---
TABLE_NAME = "NEWS_ARTICLES"
MAX_ARTICLES_PER_REQUEST = 100  # Max page size for News API
MAX_PAGES_TO_FETCH = 5          # Limit total pages to fetch for the combined query

# ----------------------------------------------------
# 1. Schema Definition
# ----------------------------------------------------

def schema(config):
    """Defines the structure of the data that will be replicated."""
    return [
        {
            "table": TABLE_NAME,
            "primary_key": ["url"], # MODIFIED: Simplified primary key
            "columns": {
                "source_id": "STRING",
                "source_name": "STRING",
                "author": "STRING",
                "title": "STRING",
                "description": "STRING",
                "url": "STRING",
                "urlToImage": "STRING",
                "publishedAt": "STRING",
                "content": "STRING",
                "search_keyword": "STRING" 
            }
        }
    ]

# ----------------------------------------------------
# 2. Update (Data Extraction) Logic
# ----------------------------------------------------

def update(configuration, state = None, local_debug=False):
    """
    Fetches data using the /v2/everything endpoint based on keyword relevance.
    This function performs a full sync on every run.
    If local_debug is True, it also writes the fetched records to output.json.
    """
    log.warning("Starting News API sync based on keywords.")
    
    # --- Configuration Retrieval ---
    api_key = configuration.get("api_key")
    if not api_key:
        raise Exception("News API Key not found in configuration.")
        
    query_terms_str = configuration.get("query_term", "general news")
    full_query = query_terms_str
    
    try:
        newsapi = NewsApiClient(api_key=api_key) 
    except NameError:
        log.severe("The 'newsapi' Python package must be installed/available.")
        raise

    # --- Core Sync Logic (Single Query) ---
    titles_seen_this_run = set()
    articles_to_upsert = []
    
    page = 1
    while page <= MAX_PAGES_TO_FETCH:
        log.info(f"Fetching page {page} for full query.")

        try:
            # MODIFIED: Removed 'from_param' and changed 'sort_by' to 'relevancy'
            results = newsapi.get_everything(
                q=full_query,
                language='en',
                sort_by='relevancy', 
                page_size=MAX_ARTICLES_PER_REQUEST,
                page=page
            )
        except Exception as e:
            log.severe(f"News API request failed for page {page}: {e}. Stopping sync.")
            break 

        articles = results.get('articles', [])
        
        if not articles:
            log.info(f"No more articles found after page {page}.")
            break
        
        for article in articles:
            article_title = article.get("title")
            clean_title = article_title.lower().strip() if article_title else None
            
            if clean_title and clean_title in titles_seen_this_run:
                continue
            if clean_title:
                titles_seen_this_run.add(clean_title)

            cleaned_article = {
                "source_id": article.get('source', {}).get('id'),
                "source_name": article.get('source', {}).get('name'),
                "author": article.get('author'),
                "title": article.get('title'),
                "description": article.get('description'),
                "url": article.get('url'),
                "urlToImage": article.get('urlToImage'),
                "publishedAt": article.get('publishedAt'),
                "content": str(article.get('content', '')),
                "search_keyword": full_query
            }
            
            articles_to_upsert.append(cleaned_article)

        if len(articles) < MAX_ARTICLES_PER_REQUEST:
            break
        page += 1

    log.info(f"Total unique articles processed this run: {len(articles_to_upsert)}")

    # --- Local JSON output logic ---
    if local_debug:
        log.warning("Local debug mode enabled: Attempting to write output to output.json.")
        if articles_to_upsert:
            try:
                with open("output.json", "w") as outfile:
                    json.dump(articles_to_upsert, outfile, indent=4)
                log.warning(f"Successfully wrote {len(articles_to_upsert)} articles to output.json.")
            except Exception as e:
                log.severe(f"Error writing to output.json: {e}")
        else:
            log.warning("No new articles were found to write to output.json.")

    # --- Send data to Fivetran ---
    for article in articles_to_upsert:
        yield op.upsert(table=TABLE_NAME, data=article)
    
    # REMOVED: Checkpoint logic is gone as we are no longer doing incremental syncs.
    log.info("Sync complete.")


# ----------------------------------------------------
# 3. Initialize the Connector
# ----------------------------------------------------

# MODIFIED: update function no longer uses state
connector = Connector(schema=schema, update=update)


if __name__ == "__main__":
    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        log.severe("configuration.json not found. Using empty configuration.")
        configuration = {}
    
    # --- Custom JSON Output Logic for Local Debugging ---
    try:
        log.warning("Starting manual update run to generate output.json...")
        # MODIFIED: Removed 'state' from the call
        for _ in update(configuration=configuration, local_debug=True):
            pass # Consume the generator to run the function
        log.warning("Manual update run for JSON output complete.")

    except Exception as e:
        log.severe(f"Error during local execution and JSON output: {e}")
    # ----------------------------------------------------
    
    # Execute the connector logic in debug mode (This will print operations to stdout)
    log.warning("\nStarting connector.debug() to print operations to console...")
    connector.debug(configuration=configuration)






