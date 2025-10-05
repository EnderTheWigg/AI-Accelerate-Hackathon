import json 
from datetime import datetime
import requests as rq

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op 

# --- Constants ---
API_BASE_URL = "https://newsdata.io/api/1/latest" 
TABLE_NAME = "news_articles"
OUTPUT_DATA_PATH = "articles_output.jsonl" 

# --- Schema Definition (Unchanged) ---
def schema(configuration: dict):
    return [
        {
            "table": TABLE_NAME,
            "primary_key": ["article_id"] 
        }
    ]

def validate_configuration(configuration: dict):
    if "api_key" not in configuration:
        raise ValueError("Missing required configuration value: 'api_key'")


# --- Main Sync Logic (FINAL FIX: Append and Deduplicate by Title) ---
def update(configuration: dict, state: dict): 
    """
    Performs a full sync, prevents duplicate articles based on title during the run,
    and appends data to the output file and database (via UPSERT).
    """
    log.warning("Starting News API sync: Appending data, deduplicating by title.")
    
    validate_configuration(configuration=configuration)

    api_key = configuration.get("api_key")
    query_terms_str = configuration.get("query_term", "fivetran") 
    api_endpoint = configuration.get("api_endpoint", API_BASE_URL)

    keywords = [term.strip() for term in query_terms_str.split(',') if term.strip()]
    
    # Use a dictionary for deduplication based on article_id across multiple queries
    all_unique_articles = {} 
    
    # 🎯 NEW: Use a set to track titles encountered during this run for title-based deduplication
    titles_seen_this_run = set()

    # We must open the file in APPEND mode ('a') before the loop starts
    try:
        # Use 'a' for append mode to keep existing data
        with open(OUTPUT_DATA_PATH, 'a', encoding='utf-8') as f:
            
            for keyword in keywords:
                log.info(f"Fetching data for query: '{keyword}'")

                params = {
                    "apikey": api_key,
                    "q": keyword, 
                    "language": "en" 
                }
                
                # --- API Request ---
                try:
                    response = rq.get(api_endpoint, params=params)
                    response.raise_for_status() 
                    articles = response.json().get("results", [])
                    log.info(f"Retrieved {len(articles)} articles for '{keyword}'.")

                    # --- Deduplication and Writing ---
                    for article in articles:
                        article_id = article.get("article_id")
                        article_title = article.get("title")
                        
                        # Use lowercase, stripped title for robust comparison
                        clean_title = article_title.lower().strip() if article_title else None

                        # Check if article_id or clean_title is already processed in this run
                        if article_id in all_unique_articles:
                            continue
                        if clean_title and clean_title in titles_seen_this_run:
                            log.fine(f"Skipping article with duplicate title: {article_title[:40]}...")
                            continue

                        # Mark as seen
                        all_unique_articles[article_id] = article
                        if clean_title:
                            titles_seen_this_run.add(clean_title)
                            
                        # Data Cleansing 
                        cleaned_article = {}
                        for key, value in article.items():
                            if isinstance(value, list):
                                cleaned_article[key] = ",".join(map(str, value))
                            elif isinstance(value, dict):
                                cleaned_article[key] = json.dumps(value)
                            else:
                                cleaned_article[key] = value

                        # 🎯 1. Append to Database (via SDK/Tester's op.upsert)
                        op.upsert(table=TABLE_NAME, data=cleaned_article) 

                        # 🎯 2. Append to Manual Output File
                        fif_record = {
                            "data": cleaned_article,
                            "table": TABLE_NAME,
                            "operation": "UPSERT" 
                        }
                        f.write(json.dumps(fif_record) + '\n')
                            
                except rq.exceptions.RequestException as e:
                    log.error(f"API request failed for keyword '{keyword}': {e}. Continuing.")
                
            log.info(f"Total unique articles processed and appended: {len(all_unique_articles)}")

    except Exception as e:
        log.severe(f"Fatal error during file write or sync: {e}")
        raise 

    # 5. Checkpoint the new state
    op.checkpoint(state=state) 
    
    log.info("Successfully finished sync. Data appended to database and output file.")


# This creates the connector object that will use the update and schema functions defined.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
if __name__ == "__main__":
    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        log.error("configuration.json not found. Using empty configuration.")
        configuration = {}
        
    connector.debug(configuration=configuration)