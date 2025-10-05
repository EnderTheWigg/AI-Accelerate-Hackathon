from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import requests to make HTTP calls to API.
import requests

# Import dataclass for creating POJO-style classes
from dataclasses import dataclass

# Import time for implementing exponential backoff
import time


API_URL = "https://newsdata.io/api/1/archive?apikey="
API_KEY = "pub_7f9c85763ec94ffca92758b23bc630f3"
__MAX_RETRIES = 3  # Maximum number of retries for failed requests
__BASE_DELAY = 2  # Base delay for exponential backoff in seconds

# POJO-style response class
@dataclass
class Article:
    """
    A class representing a post from the JSONPlaceholder API.
    """

    articleID: str
    id: str
    title: str
    headers: str
    body : str



def schema(configuration: dict):
  
    return [
        {
            "table": "Article",
            "primary_key": ["id"],
            "columns": {"articleID": "STRING", "title": "STRING", "headers" : "STRING","body": "STRING"},
        }
    ]
def update(configuration: dict, state: dict):
    """
    # Define the update function, which is a required function, and is called by Fivetran during each sync.
    # See the technical reference documentation for more details on the update function
    # https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    # The state dictionary is empty for the first sync or for any full re-sync
    :param configuration: a dictionary that holds the configuration settings for the connector.
    :param state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
    """
    
    log.info("Fetching articles from JSONPlaceholder")

    post_list = []
    for attempt in range(1, __MAX_RETRIES + 1):
        try:
            response = requests.get(__API_URL)
            response.raise_for_status()
            post_list = response.json()
            break  # Exit retry loop on success
        except requests.RequestException as e:
            log.warning(f"Request attempt {attempt} failed: {e}")
            if attempt < __MAX_RETRIES:
                delay = __BASE_DELAY**attempt
                log.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                log.severe("Maximum retry attempts reached. Request aborted.")
                return

    for post_dict in post_list:
        try:
            post = Article(**post_dict)  # Deserialize into a POJO

            # Perform upsert operation, to sync the post data
            op.upsert(
                "posts",
                {"id": post.id, "userId": post.articleID, "title": post.title, "header" : post.header, "body": post.body},
            )

        except Exception as e:
            log.warning(f"Failed to process post: {post_dict.get('id', 'unknown')} - {e}")

    log.info(f"Synced {len(post_list)} posts")