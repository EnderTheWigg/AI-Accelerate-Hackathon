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
# NEW CONSTANT: Defines the predictable output file name
OUTPUT_DATA_PATH = "articles_output.jsonl" 


# --- Schema Definition (Unchanged) ---
def schema(configuration: dict):
    return [
        {
            "table": TABLE_NAME,
            "primary_key": ["article_id"] 
        }
    ]

# ... (validate_configuration function is unchanged) ...

def validate_configuration(configuration: dict):
    if "api_key" not in configuration:
        raise ValueError("Missing required configuration value: 'api_key'")


# --- Main Sync Logic (Manual File Write) ---
def update(configuration: dict, state: dict): 
    """
    Performs a full sync by making separate API requests and MANUALLY writing
    the Fivetran Intermediate Format (FIF) data to a predictable file.
    """
    log.warning("Starting News API sync and manually writing to data file.")
    
    validate_configuration(configuration=configuration)

    api_key = configuration.get("api_key")
    query_terms_str = configuration.get("query_term", "fivetran") 
    api_endpoint = configuration.get("api_endpoint", API_BASE_URL)

    keywords = [term.strip() for term in query_terms_str.split(',') if term.strip()]
    all_unique_articles = {} 

    # We must explicitly open the file for writing before the loop starts
    try:
        with open(OUTPUT_DATA_PATH, 'w', encoding='utf-8') as f:
            
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
                        if article_id and article_id not in all_unique_articles:
                            all_unique_articles[article_id] = article
                            
                            # Data Cleansing Logic (moved outside the loop for simplicity in this example)
                            cleaned_article = {}
                            for key, value in article.items():
                                if isinstance(value, list):
                                    cleaned_article[key] = ",".join(map(str, value))
                                elif isinstance(value, dict):
                                    cleaned_article[key] = json.dumps(value)
                                else:
                                    cleaned_article[key] = value

                            # Manual FIF Record Creation
                            fif_record = {
                                "data": cleaned_article,
                                "table": TABLE_NAME,
                                "operation": "UPSERT" 
                            }
                            # Write JSON Line to the file
                            f.write(json.dumps(fif_record) + '\n')
                            
                except rq.exceptions.RequestException as e:
                    log.severe(f"API request failed for keyword '{keyword}': {e}. Continuing.")
                
            log.info(f"Total unique articles processed and written to {OUTPUT_DATA_PATH}.")

    except Exception as e:
        log.severe(f"Fatal error during file write or sync: {e}")
        raise # Re-raise the exception to fail the sync process

    # 5. Checkpoint the new state (Still required by the SDK)
    op.checkpoint(state=state) 
    
    log.info("Successfully finished sync. Check 'articles_output.jsonl' for data.")


# This creates the connector object that will use the update and schema functions defined.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
if __name__ == "__main__":
    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        log.severe("configuration.json not found. Using empty configuration.")
        configuration = {}
        
    # We must call debug() to execute the logic, even though it won't be used to read the FIF file.
    connector.debug(configuration=configuration)