from serpapi import GoogleSearch
import os
import time
from dotenv import load_dotenv

load_dotenv()
print(f"DEBUG: SERPAPI_API_KEY from env (after load_dotenv()): {os.getenv('SERPAPI_API_KEY')}")

SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")
if not SERPAPI_API_KEY:
    raise OSError("SERPAPI_API_KEY environment variable not set.")

params = {
    "engine": "google",
        "q": "Coffee Shops in Austin", # Simple test query
    "api_key": SERPAPI_API_KEY,
    "async": False, # <--- IMPORTANT: Set to False for synchronous
    "no_cache": True
}

search = GoogleSearch(params)
initial_result = search.get_dict()

if 'search_metadata' not in initial_result:
    print("Error: No metadata in initial response")
    exit(1)

search_id = initial_result['search_metadata']['id']
print(f"Synchronous Search ID: {search_id} submitted. Waiting 15 seconds...")
time.sleep(15) # Wait for retrieval

# Corrected Retrieval Parameters - ONLY api_key (search_id is positional argument now)
retrieval_params = {
    "api_key": SERPAPI_API_KEY,
}
archived_search = GoogleSearch(retrieval_params).get_search_archive(search_id) # <--- Pass search_id as POSITIONAL argument

if 'organic_results' in archived_search:
    print("Synchronous Retrieval Successful!")
    print("First few organic results:")
    for result in archived_search['organic_results'][:3]:
        print(f"- {result.get('link')}")
else:
    print("Synchronous Retrieval Failed!")
    print(f"Error: No organic results in archived search. Raw response: {archived_search}")

print("Test script finished.")