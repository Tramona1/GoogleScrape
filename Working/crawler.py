from dotenv import load_dotenv
load_dotenv()
import asyncio
import aiohttp
from serpapi import GoogleSearch
import time
import csv
import logging
import os
from datetime import datetime
import shelve  # Import shelve
import json  # Import json
from supabase import create_client # Import synchronous supabase client
import Working.utils as utils # IMPORTANT - Keep this import for utils module
from prometheus_client import start_http_server, Counter, Gauge

# --- Configuration ---
SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")  # Get API Key from environment variable
if not SERPAPI_API_KEY:
    raise EnvironmentError("SERPAPI_API_KEY environment variable not set.")

TWOCAPTCHA_API_KEY = os.getenv("TWOCAPTCHA_API_KEY")  # Get 2Captcha API Key
if not TWOCAPTCHA_API_KEY:
    logging.Logger.warning("TWOCAPTCHA_API_KEY environment variable not set. CAPTCHA solving will be disabled.")

SUPABASE_URL = os.getenv("SUPABASE_URL")  # Get Supabase URL
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # Get Supabase Key
if not SUPABASE_URL or not SUPABASE_KEY:
    raise EnvironmentError("SUPABASE_URL and SUPABASE_KEY environment variables must be set for database storage.")

CITIES = [  # Reduced list for testing
    "Austin",
    "Orlando",
    "Birmingham" # Using Birmingham, Alabama as a city for testing
]

SEARCH_TERMS = [
    "Vacation Rentals", # Reduced to one term for testing
]
PAGES_PER_QUERY = 2  # Reduced for faster testing
OUTPUT_CSV_FILENAME = "property_managers_data.csv"
CONCURRENT_REQUESTS = 2 # Keep low for testing, or set to 1
LOG_LEVEL = logging.DEBUG

# Proxy Configuration
USE_PROXY = os.getenv("USE_PROXY", "False").lower() == "true"
PROXY_HOST = os.getenv("PROXY_HOST")
PROXY_PORT = os.getenv("PROXY_PORT")
PROXY_USER = os.getenv("PROXY_USER")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

# --- End Configuration ---

# Setup logging
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

# SerpAPI Usage Tracking
serpapi_usage = {
    "total_searches": 0,
    "failed_searches": 0
}

# Error Budget Tracking
error_budget = {
    "total": 0,
    "success": 0,
    "failed": 0
}

crawled_pages_metric = Counter('crawled_pages', 'Total pages crawled')
extraction_success_metric = Counter('extraction_success', 'Successful extractions')
extraction_failure_metric = Counter('extraction_failure', 'Failed extractions')
relevance_score_gauge = Gauge('avg_relevance_score', 'Average Relevance Score')
start_http_server(8000)

# Initialize Supabase client - Synchronous (outside async functions)
supabase_client = create_client( # Synchronous client initialization
    SUPABASE_URL,
    SUPABASE_KEY
)


async def fetch_url(session, url, retry_count=3):
    """Asynchronously fetches a URL with proxy support and retry logic."""
    proxy_url = None
    if USE_PROXY:
        if PROXY_HOST and PROXY_PORT and PROXY_USER and PROXY_PASSWORD:
            proxy_url = f"http://{PROXY_USER}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
        else:
            logger.warning("Proxy settings enabled but incomplete. Not using proxy for this request.")

    for attempt in range(retry_count):
        try:
            headers = {  # Added headers here - more realistic
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
            }
            async with session.get(url, timeout=25, proxy=proxy_url, headers=headers) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError as e:
            logger.warning(f"Async request error for {url} (attempt {attempt + 1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                await asyncio.sleep(2)
            else:
                logger.error(f"Failed to fetch {url} after {retry_count} retries.")
                return None
        except asyncio.TimeoutError:
            logger.warning(f"Async request timeout for {url} (attempt {attempt + 1}/{retry_count})")
            if attempt < retry_count - 1:
                await asyncio.sleep(2)
            else:
                logger.error(f"Timeout fetching {url} after {retry_count} retries.")
                return None


async def crawl_and_extract_async(session, context):
    print("DEBUG: Entering crawl_and_extract_async for url:", context.get('url')) # DEBUG - Entry point
    url = context["url"]
    logger.info(f"Crawling {url} for {context['city']} - {context['term']}")
    crawled_pages_metric.inc()

    try:
        data = await utils.extract_contact_info(session, url, context) # Assuming utils.extract_contact_info
        print("DEBUG: Data from utils.extract_contact_info:", data) # DEBUG - After extract_contact_info
        if data:
            logger.debug(f"Successfully extracted data from: {url}")
            extraction_success_metric.inc()
            relevance_score_gauge.inc(data.get("Relevance Score", 0))
            return data
        else:
            extraction_failure_metric.inc()
            logger.warning(f"Extraction failed for {url} (extract_contact_info returned None).")
            return None
    except Exception as e:
        extraction_failure_metric.inc()
        logger.exception(f"Exception during async crawl/extraction for {url}: {e}")
        print(f"DEBUG: Exception in crawl_and_extract_async: {e}") # DEBUG - Exception caught
        return None


async def process_urls_async(url_contexts):
    print("DEBUG: Entering process_urls_async with url_contexts:", url_contexts) # DEBUG - Entry point

    extracted_data_list = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async def controlled_crawl(context):
        async with semaphore:
            await asyncio.sleep(1.5)
            async with aiohttp.ClientSession() as session: # aiohttp session remains async for crawling
                try: # ADDED try...except block in controlled_crawl (already present)
                    data = await crawl_and_extract_async(session, context)
                    print(f"DEBUG (controlled_crawl): Data extracted from {context['url']}: {data}") # DEBUG - Print data from crawl_and_extract_async
                    if data:
                        db_saved = save_to_supabase(supabase_client, data) # Pass synchronous supabase_client - NO AWAIT
                        if not db_saved:
                            save_to_csv([data], context.get("batch_number", 0))
                        return data
                    print(f"DEBUG (controlled_crawl): Extraction failed for {context['url']}") # DEBUG - Extraction failed
                    return None
                except Exception as e_controlled_crawl: # Catch exceptions in controlled_crawl
                    logger.exception(f"Exception in controlled_crawl for {context.get('url')}: {e_controlled_crawl}") # Log exception
                    return None # Ensure it returns None even on exception

    try: # ADDED try...except block AROUND process_urls_async
        tasks = [controlled_crawl(context) for context in url_contexts]
        results = await asyncio.gather(*tasks, return_exceptions=True) # <-- return_exceptions=True
        print(f"DEBUG (process_urls_async): Results from asyncio.gather: {results}") # DEBUG - Before return
        print("DEBUG: Exiting process_urls_async, returning:", results) # DEBUG - Exit point
        return results # <--- FIXED RETURN STATEMENT HERE - Removed invalid syntax
    except Exception as e_process_urls: # Catch any exceptions in process_urls_async
        logger.exception(f"Exception in process_urls_async: {e_process_urls}") # Log exception in process_urls_async
        return None # Return None if process_urls_async itself fails


async def get_google_search_results(session, city, term):
    """Async Google search with SerpAPI for multiple pages and returns URLs with context."""
    logger.debug(f"Entering get_google_search_results for city={city}, term={term}")
    all_urls = []
    for page_num in range(1, PAGES_PER_QUERY + 1):
        params = {
            "api_key": SERPAPI_API_KEY,
            "engine": "google",
            "q": f"{city} {term}",
            "gl": "us",
            "num": 10,
            "start": (page_num - 1) * 10,
            "async": True
        }
        try:
            search = GoogleSearch(params)
            initial_search_result = await asyncio.to_thread(search.get_dict)

            if initial_search_result["search_metadata"]["status"] == "Processing":
                json_endpoint_url = initial_search_result["search_metadata"]["json_endpoint"]
                logger.debug(f"Async search in progress, fetching final results from: {json_endpoint_url}")

                async with session.get(json_endpoint_url) as response:
                    response.raise_for_status()
                    final_search_result_json = await response.text()
                    final_search_result = json.loads(final_search_result_json)
            else:
                final_search_result = initial_search_result

            if "organic_results" in final_search_result:
                for result in final_search_result["organic_results"]:
                    url = result.get("link")
                    if url and utils.is_valid_url(url): # Assuming utils.is_valid_url
                        all_urls.append(url)
            else:
                logger.warning(f"No organic results in final result for {city} {term} page {page_num}")
                logger.debug(f"Final Search Result Dictionary (No organic_results key found):\n{json.dumps(final_search_result, indent=2)}")
                logger.debug(f"Full SerpAPI Response JSON (when no organic results):\n{json.dumps(final_search_result, indent=2)}")

            await asyncio.sleep(2.5) # Increased delay

        except Exception as e:
            logger.error(f"Search failed for {city} {term} page {page_num}: {e}")
            serpapi_usage["failed_searches"] += 1
            continue
        serpapi_usage["total_searches"] += 1

    return {"city": city, "term": term, "urls": all_urls}


def save_to_supabase(supabase_client, data): # Pass synchronous supabase_client - NO ASYNC DEF
    """Saves extracted data to Supabase database (now synchronous)."""
    try:
        # Convert HttpUrl objects to strings before saving to Supabase
        data_for_supabase = {
            k: [str(v) if isinstance(v, utils.HttpUrl) else item for item in v] if isinstance(v, list) else (str(v) if isinstance(v, utils.HttpUrl) else v)
            for k, v in data.items()
        }

        response = supabase_client.table('property_managers').insert(data_for_supabase).execute() # Removed 'await' - Synchronous call

        print(f"DEBUG (save_to_supabase): Full Supabase response object: {response}") # ADD THIS DEBUG PRINT

        if response.error: # Check response.error FIRST for explicit Supabase errors
            logger.error(f"Supabase insert error for {data.get('website_url')}: {response.error}")
            logger.error(f"Supabase error details: {response.error}")
            return False
        elif not response.data: # THEN check if response.data is empty - more general failure check
            logger.error(f"Supabase insert failed for {data.get('website_url')}: No data returned, possible issue.")
            logger.warning(f"Full Supabase response (for debugging): {response}")
            return False
        else: # If no error and data is present, consider it success
            logger.debug(f"Data saved to Supabase for: {data.get('website_url')}")
            return True

    except Exception as e_supabase:
        logger.exception(f"Error saving to Supabase for {data.get('website_url')}: {e_supabase}")
        return False



def save_to_csv(data_list, batch_number):
    """Saves extracted data to CSV file as a fallback."""
    csv_filename = f"property_managers_data_batch_{batch_number}.csv"
    logger.info(f"Saving batch {batch_number} data to CSV file: {csv_filename} (fallback)")

    fieldnames = ["City", "Search Term", "Company Name", "Website URL", "Email Addresses", "Phone Numbers", "Physical Address", "Relevance Score", "Timestamp", "Social Media", "Estimated Properties", "Service Areas", "Management Software", "License Numbers"]
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data_item in data_list:
            # Convert HttpUrl objects to strings before saving to CSV
            social_media_str = ", ".join([str(url) for url in data_item.get("social_media", [])]) # Convert HttpUrl to str for CSV
            writer.writerow({
                "City": data_item.get("City", "N/A"),
                "Search Term": data_item.get("Search Term", "N/A"),
                "Company Name": data_item.get("Company Name", "N/A"),
                "Website URL": data_item.get("website_url", "N/A"), # Include website_url
                "Email Addresses": ", ".join(data_item.get("Email Addresses", [])),
                "Phone Numbers": ", ".join(data_item.get("Phone Numbers", [])),
                "Physical Address": data_item.get("Physical Address", "N/A"),
                "Relevance Score": data_item.get("Relevance Score", 0.0),
                "Timestamp": datetime.now().isoformat(),
                "Social Media": social_media_str, # Use string version
                "Estimated Properties": data_item.get("estimated_properties", 0),
                "Service Areas": ", ".join(data_item.get("service_areas", [])),
                "Management Software": ", ".join(data_item.get("management_software", [])),
                "License Numbers": ", ".join(data_item.get("license_numbers", [])) # Include license numbers in CSV
            })


async def main():
    """Main async workflow to crawl and extract data, using shelve queue and batch processing."""
    start_time = time.time()
    logger.info("Crawler started...")

    error_budget["total"] = len(CITIES) * len(SEARCH_TERMS) * PAGES_PER_QUERY

    async with aiohttp.ClientSession() as session:
        with shelve.open('url_queue.db') as db:
            if 'url_contexts' not in db:
                db['url_contexts'] = []
            url_contexts_queue = db['url_contexts']

            if not url_contexts_queue: # Populate queue initially
                for city in CITIES: # Iterate through cities sequentially
                    for term in SEARCH_TERMS: # Iterate through search terms sequentially for each city
                        search_results = await get_google_search_results(session, city, term) # Get search results for current city and term
                        if search_results:
                            for url in search_results["urls"]:
                                url_contexts_queue.append({"city": city, "term": term, "url": url}) # Add to queue
                            logger.info(f"Added {len(search_results['urls'])} URLs to queue for City: {city}, Search Term: {term}") # Log URLs added
                        else:
                            logger.warning(f"No search results for City: {city}, Search Term: {term}") # Log no results

                db['url_contexts'] = url_contexts_queue
                logger.info(f"Initial URL queue populated with {len(url_contexts_queue)} URLs and saved to disk.")
            else:
                logger.info(f"Resuming from existing URL queue with {len(url_contexts_queue)} URLs.")

            print("DEBUG: Initial len(url_contexts_queue):", len(url_contexts_queue)) # ADDED DEBUG
            batch_number = 0
            while url_contexts_queue:
                print("DEBUG: len(url_contexts_queue) at loop start:", len(url_contexts_queue)) # ADDED DEBUG
                url_contexts_batch = []
                batch_size_crawl = 20
                for _ in range(min(batch_size_crawl, len(url_contexts_queue))):
                    url_contexts_batch.append(url_contexts_queue.pop(0))

                db['url_contexts'] = url_contexts_queue

                print("DEBUG: url_contexts_batch before process_urls_async:", url_contexts_batch) # ADDED DEBUG
                extracted_data_list = await process_urls_async(url_contexts_batch)
                print(f"DEBUG: Type of extracted_data_list: {type(extracted_data_list)}")
                valid_data = [data for data in extracted_data_list if data] # LINE 319 - Error will occur here

                logger.info(f"Extracted data from {len(valid_data)} websites in batch {batch_number} from queue.")
                error_budget["success"] += len(valid_data)
                error_budget["failed"] += (len(url_contexts_batch) - len(valid_data))

                batch_number += 1

        logger.info("Finished processing all URLs from queue.")
        logger.info(f"SerpAPI Usage Summary: Total Searches: {serpapi_usage['total_searches']}, Failed Searches: {serpapi_usage['failed_searches']}")
        logger.info(f"Error budget summary: {error_budget}")
        logger.info(f"Prometheus metrics available at http://localhost:8000/metrics")


    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Crawler finished. Total time: {duration:.2f} seconds.")


if __name__ == "__main__":
    asyncio.run(main())