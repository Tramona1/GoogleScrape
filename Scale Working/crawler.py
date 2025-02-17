print("Script execution started...")

from dotenv import load_dotenv
load_dotenv()
import os

import asyncio
from urllib.parse import urlparse, urljoin
import aiohttp
from serpapi import GoogleSearch
import time
import csv
import logging
import datetime
import shelve # 12. Redis Queue Integration 📦 - REMOVED shelve import
import json
from supabase import create_client
import utils  # Import utils
from utils import (
    metrics, rotate_proxy, decay_proxy_scores, normalize_url,
    crawl_and_extract_async, analyze_batch, get_session, proxy_health_check, close_playwright, get_playwright_instance, # <-- ENSURE close_playwright and get_playwright_instance ARE HERE
    BAD_PATH_PATTERN, GENERIC_DOMAINS, close_session, update_proxy_score # <-- ENSURE decay_proxy_scores AND update_proxy_score ARE HERE
)
from prometheus_client import start_http_server, Counter, Gauge, REGISTRY # MODIFIED: Import REGISTRY
import random
import socket
import struct
from fake_useragent import UserAgent
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type # ADDED: Import retry, stop_after_attempt, wait_exponential
import urllib.robotparser
from urllib.parse import urljoin
from collections import defaultdict
import traceback
import async_timeout # Import async_timeout - ALREADY IMPORTED IN UTILS, BUT KEEPING HERE AS WELL FOR CLARITY IN CRAWLER
from itertools import cycle  # Add this import at the top with other imports
import redis # 12. Redis Queue Integration 📦 - ADDED redis import

print("Imports completed...")


# --- Configuration ---
SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")
if not SERPAPI_API_KEY:
    raise EnvironmentError("SERPAPI_API_KEY environment variable not set.")

TWOCAPTCHA_API_KEY = os.getenv("TWOCAPTCHA_API_KEY")
if not TWOCAPTCHA_API_KEY:
    logging.Logger.warning("TWOCAPTCHA_API_KEY environment variable not set. CAPTCHA solving will be disabled.")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise EnvironmentError("SUPABASE_URL and SUPABASE_KEY environment variables must be set for database storage.")

CITIES = [
    "Sedona", "Los Angeles", "San Diego", "Seattle", "portland", "Malibu", "Newport Beach"
    # "Houston", "Phoenix", "Philadelphia", "San Antonio",
]

SEARCH_TERMS = [
    "vacation rentals",
    "short term rentals",
    "property managers short term rentals"
]

PAGES_PER_QUERY = 15
OUTPUT_CSV_FILENAME = "property_managers_data.csv"
CONCURRENT_REQUESTS = 75 # Reduced Concurrent Requests - Production Tuning
LOG_LEVEL = logging.DEBUG

USE_PROXY = os.getenv("USE_PROXY", "False").lower() == "true"
PROXY_HOST = os.getenv("PROXY_HOST")
PROXY_PORT = os.getenv("PROXY_PORT")
PROXY_USER = os.getenv("PROXY_USER")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

MAX_SEARCHES_PER_IP = 8
BATCH_SIZE = 25 # Increased Batch Size - Production Tuning

SEARCH_CONCURRENCY = 15  # Reduced from 20 - Production Tuning - Proportional to CONCURRENT_REQUESTS
CRAWL_CONCURRENCY = 75  # Reduced from 100 - Production Tuning - Set to CONCURRENT_REQUESTS
LLM_CONCURRENCY = 8     # Reduced from 10 - Production Tuning - Proportional to CONCURRENT_REQUESTS


# --- End Configuration ---

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

serpapi_usage = {
    "total_searches": 0,
    "failed_searches": 0
}

error_budget = {
    "total": 0,
    "success": 0,
    "failed": 0
}


start_http_server(8002)

supabase_client = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)


search_count = defaultdict(int)

proxy_pool_to_use = None
current_proxy_index = 0

# Define Semaphores - Define semaphores at the top level, before main()
SEARCH_SEM = asyncio.Semaphore(SEARCH_CONCURRENCY)
CRAWL_SEM = asyncio.Semaphore(CRAWL_CONCURRENCY)
LLM_SEM = asyncio.Semaphore(LLM_CONCURRENCY)


def rotate_proxy_crawler():
    global proxy_pool_to_use
    return utils.rotate_proxy(proxy_pool_to_use) # Use imported rotate_proxy


async def process_urls_async(url_contexts, proxy_pool, crawl_semaphore): # REMOVED: session parameter
    logger.debug(f"Entering process_urls_async with {len(url_contexts)} URLs") # DEBUG LOG - ENTRY POINT
    async def controlled_crawl(context, proxy_pool, crawl_semaphore): # REMOVED: session parameter
        logger.debug(f"Entering controlled_crawl for URL: {context.get('url')}, City: {context.get('city')}") # DEBUG LOG - ENTRY POINT
        session = await get_session() # Get session from pool - SESSION POOLING - IMPORT FROM UTILS
        try:
            async with crawl_semaphore:
                url = context['url']
                if not utils.validate_request(url):
                    metrics['crawl_errors'].labels(type='invalid_url', proxy=proxy_pool[0] if proxy_pool else 'no_proxy').inc() # Using first proxy for labeling if pool exists
                    return None

                logger.debug(f"Starting crawl_and_extract_async for {url}")
                data = await utils.crawl_and_extract_async(session, context, proxy_pool=proxy_pool) # Use imported function and pass session
                logger.debug(f"Finished crawl_and_extract_async for {context['url']}, Data: {data}") # DEBUG LOG - DATA AFTER CRAWL_AND_EXTRACT
                print(f"DEBUG (controlled_crawl): Data extracted from {context['url']}: {data}")
                if data and data.url: # Line 379 - Correct Indentation - CORRECTED .get() to attribute access
                    db_saved = save_to_supabase(supabase_client, data) # Line 380 - Correct Indentation
                    if not db_saved: # Line 381 - Correct Indentation
                        save_to_csv( [data], context.get("batch_number", 0)) # Line 382 - Correct Indentation
                    return data # Line 383 - Correct Indentation
                print(f"DEBUG (controlled_crawl): Extraction failed for {context['url']}") # Line 384 - Correct Indentation
                return None # Line 385 - Correct Indentation
        except Exception as e_controlled_crawl:
            if "Rate limit" in str(e_controlled_crawl):
                logger.warning(f"Rate limit encountered in controlled_crawl for {context.get('url')}: {e_controlled_crawl}")
            logger.exception(f"Exception in controlled_crawl for {context.get('url')}: {e_controlled_crawl}")
            return None


    extracted_data_list = []

    async def controlled_crawl_wrapper(context, proxy_pool, crawl_semaphore): # REMOVED: session parameter
        return await controlled_crawl(context, proxy_pool, crawl_semaphore) # REMOVED: session parameter

    try:
        tasks = [controlled_crawl_wrapper(context, proxy_pool, crawl_semaphore) for context in url_contexts] # REMOVED: session parameter
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.debug(f"DEBUG: (process_urls_async): Results from asyncio.gather: {results}") # DEBUG LOG - ASYNCIO.GATHER RESULTS
        print("DEBUG: (process_urls_async): Results from asyncio.gather:", results)
        extracted_data_list = [result for result in results if result is not None]
        logger.debug(f"DEBUG: Exiting process_urls_async, returning: {extracted_data_list}") # DEBUG LOG - EXITING FUNCTION
        print("DEBUG: Exiting process_urls_async, returning:", extracted_data_list)
        return extracted_data_list
    except Exception as e_process_urls:
        logger.exception(f"Exception in process_urls_async: {e_process_urls}")
        return None


@retry(
    stop=stop_after_attempt(3), # Retry up to 3 times
    wait=wait_exponential(multiplier=1, min=4, max=30), # Exponential backoff (4s, 8s, 16s, up to 30s)
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)), # Retry on these exceptions
    reraise=True # If all retries fail, raise the last exception
)
async def get_google_search_results(city, term, proxy_pool): # REMOVED: session parameter
    global search_count, proxy_pool_to_use, current_proxy_index
    logger.debug(f"Entering get_google_search_results for city={city}, term={term}") # DEBUG LOG - ENTRY POINT
    all_urls = []
    params = { # params is defined here, locally within the function
        "engine": "google",
        "q": f"{term} in {city}", # Corrected query format: "term in city"
        "gl": "us",
        "hl": "en",
            "num": 10,
            "api_key": SERPAPI_API_KEY,
            "async": True,
            "no_cache": True
        }
    archived_params = params
    current_proxy = None
    proxy_key = 'no_proxy'

    if USE_PROXY and proxy_pool and len(proxy_pool) > 0:
        # 3. Proxy Rotation Fix 🔄 - USE utils.rotate_proxy
        current_proxy = utils.rotate_proxy(proxy_pool)  # Use health-based rotation
        proxy_key = current_proxy

        if search_count[proxy_key] >= MAX_SEARCHES_PER_IP:
            logger.warning(f"Search limit reached for proxy: {current_proxy}. Rotating...")
            current_proxy = rotate_proxy_crawler()
            if current_proxy:
                proxy_key = current_proxy # proxy_key is assigned here if current_proxy is not None
            else:
                logger.warning("No proxies available after rotation, skipping search for this city/term.")
                return {"city": city, "term": term, "urls": []}

    try:
        if USE_PROXY and current_proxy:
            GoogleSearch.SERPAPI_HTTP_PROXY = f"https://{current_proxy}"
            logger.debug(f"SerpAPI search using proxy: {current_proxy}")
        else:
            GoogleSearch.SERPAPI_HTTP_PROXY = None
            logger.debug("SerpAPI search without proxy.")

        logger.debug(f"DEBUG: API Key being used for SerpAPI search: {params['api_key']}")
        # Apply SEARCH_SEM here with timeout
        try:
            async with async_timeout.timeout(45):  # Increased timeout to 45 seconds - Production Tuning
                async with SEARCH_SEM: # SEARCH_SEM is defined globally
                    search = GoogleSearch(params) # params is defined above
                    initial_result = await asyncio.to_thread(search.get_dict)
                    logger.debug(f"DEBUG: Initial SerpAPI result for {city} {term}: {initial_result.get('search_metadata')}") # DEBUG LOG - INITIAL RESULT METADATA
        except asyncio.TimeoutError:
            logger.error(f"Timeout acquiring SEARCH_SEM for {city} {term}. Skipping search.")
            metrics['serpapi_errors'].inc() # Use metrics dict
            serpapi_usage["failed_searches"] += 1
            return {"city": city, "term": term, "urls": []}


        if 'search_metadata' not in initial_result:
            logger.error(f"SerpAPI search failed for {city} {term}: No metadata in response")
            serpapi_usage["failed_searches"] += 1
            if USE_PROXY and current_proxy:
                utils.update_proxy_score(proxy_key, False) # Penalize proxy on SerpAPI error
            return {"city": city, "term": term, "urls": []}

        if initial_result['search_metadata'].get('status') == ' ক্যাপচা ': # Check for CAPTCHA status
            logger.warning(f"SerpAPI returned CAPTCHA for {city} {term} using proxy: {current_proxy}")
            metrics['captcha_requests'].inc()
            metrics['captcha_failed'].inc()
            if USE_PROXY and current_proxy:
                utils.update_proxy_score(proxy_key, False) # Penalize proxy on CAPTCHA
            return {"city": city, "term": term, "urls": []}


        search_id = initial_result['search_metadata']['id']
        logger.debug(f"DEBUG: Search ID after submission: {search_id}")
        logger.info(f"Search ID {search_id} submitted. Waiting 10 seconds for results...")
        await asyncio.sleep(10)

        max_retries = 3
        for attempt in range(max_retries):
            retrieval_url = f"https://serpapi.com/searches/{search_id}.json"
            retrieval_params = {
                "api_key": SERPAPI_API_KEY,
            }
            param_string = "&".join([f"{key}={value}" for key, value in retrieval_params.items()])
            full_retrieval_url = f"{retrieval_url}?{param_string}"
            logger.debug(f"DEBUG: Full Retrieval URL: {full_retrieval_url}")
            logger.debug(f"DEBUG: Retrieval Search ID: {search_id}")

            archived_search = GoogleSearch(retrieval_params).get_search_archive(search_id)

            if 'organic_results' in archived_search:
                logger.debug(f"DEBUG: Organic results found for {city} {term} on attempt {attempt+1}") # DEBUG LOG - ORGANIC RESULTS FOUND
                break
            await asyncio.sleep(2 ** attempt)
        else:
            logger.error(f"Max retries reached for {search_id}")
            serpapi_usage["failed_searches"] += 1
            if USE_PROXY and current_proxy:
                utils.update_proxy_score(proxy_key, False) # Penalize proxy on retrieval failure
            return {"city": city, "term": term, "urls": []}


        if 'organic_results' in archived_search:
            logger.debug(f"Raw SerpAPI results: {archived_search['organic_results']}") # DEBUG LOG - RAW RESULTS
            for result in archived_search['organic_results']:
                url = result.get('link')
                if url and utils.is_valid_url(url):
                    all_urls.append(url)
                    logger.debug(f"Accepted URL: {url}")
            logger.info(f"Found {len(all_urls)} valid URLs for {city} {term}") # INFO LOG - URL COUNT
            utils.update_proxy_score(proxy_key, True) # Reward proxy on successful SerpAPI call
        else:
            logger.warning(f"No organic results in archived search for {city} {term}")
            if USE_PROXY and current_proxy:
                utils.update_proxy_score(proxy_key, False) # Penalize proxy on no organic results

        serpapi_usage["total_searches"] += 1
        search_count[proxy_key] += 1
        if 'search_metadata' in initial_result and 'search_information' in initial_result.get('search_metadata', {}):
            if initial_result['search_metadata']['search_information'].get('status_code') in [429, 503]:
                logger.warning(f"SerpAPI capacity issue: {initial_result['search_metadata']['search_information'].get('status_code')} for {city} {term}. Waiting 60s...")
                await asyncio.sleep(60)


    except Exception as e:
        logger.error(f"Inner SerpAPI search call failed for {city} {term}: {traceback.format_exc()}")
        metrics['serpapi_errors'].inc() # Use metrics dict
        serpapi_usage["failed_searches"] += 1
        if USE_PROXY and current_proxy:
            utils.update_proxy_score(proxy_key, False) # Penalize proxy on exception
        return {"city": city, "term": term, "urls": []}
    finally:
        GoogleSearch.SERPAPI_HTTP_PROXY = None
        logger.debug(f"Exiting get_google_search_results for city={city}, term={term}, returning {len(all_urls)} urls") # DEBUG LOG - EXIT POINT

    return {"city": city, "term": term, "urls": all_urls}


def save_to_supabase(supabase_client, data):
    """Saves extracted data to Supabase database (now synchronous)."""
    try:
        # --- Data Validation ---
        required_fields = ['url']  # Require URL as a minimum
        if not data.url:  # Check data.url now
            logger.warning(f"Missing website URL, skipping insert")
            return False
        # --- End Data Validation ---


        existing = supabase_client.table('property_manager_contacts') \
            .select('url') \
            .eq('url', str(data.url)) \
            .execute()

        if existing.data:
            logger.warning(f"Duplicate URL skipped: {data.url}") # Log data.url
            return True

        data_for_supabase = {
            'name': str(data.name or 'N/A'),             # Map data.name
            'email': data.email,                         # Map data.email (already a list/array)
            'phone_number': data.phoneNumber,             # Map data.phoneNumber (already a list/array)
            'city': str(data.city or 'N/A'),             # Map data.city - from CITIES list now!
            'url': str(data.url),                       # Map data.url
            'search_keywords': data.searchKeywords,     # Map searchKeywords
            # 'latLngPoint': None, # Skip latLngPoint for now
            # 'lastEmailSentAt': None, # Skip lastEmailSentAt for now
        }
        logger.debug(f"Supabase Insert Payload: {data_for_supabase}")

        response = supabase_client.table('property_manager_contacts').insert(data_for_supabase).execute() # Correct table name

        logger.debug(f"Supabase response: {str(response)[:200]}...") # Improved debug logging - truncated response

        # --- UPDATED ERROR CHECKING  ---
        if hasattr(response, 'error') and response.error:
            logger.error(f"Supabase insert failed: {response.error.message}")
            logger.error(f"Supabase Error Details: {response.error}") # More details
            logger.error(f"Data Payload causing error: {data_for_supabase}") # Log payload
            return False

        if not response.data:
            logger.error("Supabase insert succeeded but returned no data. Possible issue.")
            logger.debug(f"Data Payload on success with no data: {data_for_supabase}") # Log payload on success with no data
            return False

        logger.debug(f"Data saved successfully to Supabase, id: {response.data[0]['id']}, url: {data.url}") # Improved success log - include id and URL
        return True
        # --- END UPDATED ERROR CHECKING ---

    except Exception as e_supabase:
        logger.exception(f"Supabase error: {e_supabase}")
        return False

def save_to_csv(data_list, batch_number):
    csv_filename = f"property_managers_data_batch_{batch_number}.csv" # You can rename this if you want
    logger.info(f"Saving batch {batch_number} data to CSV file: {csv_filename}")

    fieldnames = [ # Updated fieldnames to match schema
        "name",
        "email",
        "phone_number",
        "city",
        "url",
        "search_keywords",
        # Removed old fields like "company_name", "website_url", etc.
    ]

    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data_item in data_list:
            writer.writerow({
                "name": data_item.name,
                "email": ", ".join(map(str, data_item.email)), # Ensure emails are strings before joining
                "phone_number": ", ".join(data_item.phoneNumber),
                "city": data_item.city,
                "url": str(data_item.url) if data_item.url else "N/A", # Handle potential None for URL
                "search_keywords": ", ".join(data_item.searchKeywords),
            })


async def main():
    start_time = time.time()
    logger.info("Crawler started...")
    print("DEBUG: USE_PROXY from env:", os.getenv("USE_PROXY"))
    print("DEBUG: PROXY_POOL from utils:", utils.get_proxy_pool()) # DEBUG: Print proxy pool
    utils.validate_proxy_config()

    await utils.get_playwright_instance() # Initialize Playwright

    search_semaphore = asyncio.Semaphore(SEARCH_CONCURRENCY)
    crawl_semaphore = asyncio.Semaphore(CRAWL_CONCURRENCY)
    llm_semaphore = asyncio.Semaphore(LLM_CONCURRENCY)

    proxy_pool_to_use = None
    if os.getenv("USE_PROXY", "False").lower() == "true":
        proxy_pool_to_use = utils.get_proxy_pool()
        asyncio.create_task(utils.proxy_health_check(proxy_pool_to_use))

    error_budget["total"] = len(CITIES) * len(SEARCH_TERMS) * PAGES_PER_QUERY


    async def proxy_decay_scheduler():
        while True:
            utils.decay_proxy_scores()
            await asyncio.sleep(3600)

    asyncio.create_task(proxy_decay_scheduler())
    logger.info("Proxy score decay scheduler started.")

    async def connection_pool_monitor():
        while True:
            session = await get_session()
            logger.info(f"Active connections: {len(session._connector._conns)}")
            await asyncio.sleep(30)
    asyncio.create_task(connection_pool_monitor())
    logger.info("Connection pool monitor started.")


    BATCH_SIZES = [10, 25, 50, 100]
    current_batch_index = 0

    r_client = redis.Redis(decode_responses=True)
    url_contexts_queue = r_client.lrange('crawl_queue', 0, -1)
    if not url_contexts_queue:
        url_contexts_queue = []

    if not url_contexts_queue:
        logger.info("URL queue is empty, starting SerpAPI searches...")
        search_tasks = []
        for city in CITIES:
            for term in SEARCH_TERMS:
                search_query = f"{city} {term}"
                logger.info(f"Preparing SerpAPI search for: Query={search_query}, City={city}, Term={term}") # INFO LOG - PREPARING SEARCH
                async with SEARCH_SEM:
                    search_tasks.append(get_google_search_results(city, term, proxy_pool_to_use)) # Pass term separately

        logger.info("Awaiting SerpAPI search tasks...")
        logger.debug(f"DEBUG: Search tasks initiated: {len(search_tasks)}") # DEBUG LOG - SEARCH TASKS COUNT
        search_results_list = await asyncio.gather(*search_tasks)
        logger.info("SerpAPI search tasks completed.")
        logger.debug(f"DEBUG: Full search_results_list: {search_results_list}") # DEBUG LOG - FULL SEARCH RESULTS

        for search_results in search_results_list:
            logger.info(f"Processing search results for: {search_results['city']} {search_results['term']}")
            if search_results:
                logger.info(f"Found {len(search_results['urls'])} URLs in SerpAPI results.")
                logger.debug(f"DEBUG: URLs from SerpAPI: {search_results['urls']}") # DEBUG LOG - URLS FROM SERPAPI
                for url in search_results["urls"]:
                    normalized_url = normalize_url(url)
                    r_client.lpush('crawl_queue', json.dumps({"city": search_results["city"], "term": SEARCH_TERMS[0], "url": utils.normalize_url(url)})) # Using SEARCH_TERMS[0] as term in queue context
                    logger.info(f"Added URL to queue: {normalized_url} for city: {search_results['city']}") # INFO LOG - URL ADDED TO QUEUE WITH CITY
                    logger.debug(f"DEBUG: Full context added to queue: {json.dumps({'city': search_results['city'], 'term': SEARCH_TERMS[0], 'url': utils.normalize_url(url)})}") # DEBUG LOG - FULL CONTEXT ADDED

        url_contexts_queue = r_client.lrange('crawl_queue', 0, -1)
        logger.info(f"Initial URL queue populated with {len(url_contexts_queue)} URLs and saved to Redis.")
        logger.debug(f"DEBUG: Initial URL queue contents (first 10): {url_contexts_queue[:10]}") # DEBUG LOG - QUEUE CONTENTS
    else:
        logger.info(f"Resuming from existing URL queue with {len(url_contexts_queue)} URLs.")
        logger.debug(f"DEBUG: Resuming URL queue contents (first 10): {url_contexts_queue[:10]}") # DEBUG LOG - RESUMING QUEUE CONTENTS


    batch_number = 0
    rate_limiter = utils.RateLimiter(requests_per_second=2)

    while url_contexts_queue:
        current_batch_index = min(int(error_budget["success"]/50), 3)
        batch_size = BATCH_SIZES[current_batch_index]
        url_contexts_batch = []

        if (error_budget["failed"] / error_budget["total"]) > 0.25:
            logger.critical("Error budget exhausted (failure rate > 25%). Exiting crawler.")
            break

        for _ in range(min(batch_size, len(url_contexts_queue))):
            url_context_str = r_client.lpop('crawl_queue')
            if url_context_str:
                url_contexts_batch.append(json.loads(url_context_str))

        logger.debug(f"DEBUG: URL batch to process: {url_contexts_batch}") # DEBUG LOG - URL BATCH

        await rate_limiter.wait()

        extracted_data_list = await process_urls_async(url_contexts_batch, proxy_pool_to_use, CRAWL_SEM)
        logger.debug(f"DEBUG: Extracted data list after process_urls_async: {extracted_data_list}") # DEBUG LOG - EXTRACTED DATA LIST


        if extracted_data_list:
            llm_batch_size = min(LLM_CONCURRENCY, len(extracted_data_list))
            llm_batches = [extracted_data_list[i:i + llm_batch_size] for i in range(0, len(extracted_data_list), llm_batch_size)]

            for llm_batch in llm_batches:
                if llm_batch:
                    try:
                        session = await get_session()
                        async with LLM_SEM:
                            llm_categories_batch = await analyze_batch([item.url for item in llm_batch], session=session) # Changed to item.url

                            for i, data_item in enumerate(llm_batch):
                                if i < len(llm_categories_batch):
                                    data_item.thoughts = llm_categories_batch[i] # Use thoughts to store LLM category
                                else:
                                    logger.warning(f"LLM category missing for URL: {data_item.url}") # Changed to item.url

                            logger.info(f"LLM analysis completed for a batch of {len(llm_batch)} URLs (placeholder).")
                    except Exception as e_llm_batch:
                        logger.error(f"Error during batch LLM analysis: {e_llm_batch}")


        logger.info(f"Extracted data from {len(extracted_data_list)} websites in batch {batch_number} from queue.")
        error_budget["success"] += sum(1 for r in extracted_data_list if r is not None)
        error_budget["failed"] += sum(1 for r in extracted_data_list if r is None)
        metrics['crawled_pages'].inc(len(extracted_data_list))
        metrics['extraction_failure'].inc(sum(1 for r in extracted_data_list if r is None))


        batch_number += 1
        current_batch_index += 1
        url_contexts_queue = r_client.lrange('crawl_queue', 0, -1)
        logger.debug(f"DEBUG: Remaining URL queue length: {len(url_contexts_queue)}") # DEBUG LOG - REMAINING QUEUE LENGTH


    logger.info("Finished processing all URLs from queue.")
    logger.info(f"SerpAPI Usage Summary: Total Searches: {serpapi_usage['total_searches']}, Failed Searches: {serpapi_usage['failed_searches']}")
    logger.info(f"Search Counts per Proxy/No Proxy: {search_count}")
    logger.info(f"Proxy Scores: {utils.PROXY_SCORES}")
    logger.info(f"Error budget summary: {error_budget}")
    logger.info(f"Prometheus metrics available at http://localhost:8002/metrics")
    logger.info(f"Final 2Captcha balance: ${utils.CAPTCHA_BALANCE:.2f}")


    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Crawler finished. Total time: {duration:.2f} seconds.")

    await close_session()
    await close_playwright() # Ensure Playwright is closed in main()


async def test_rotation():
    proxies = ["p1","p2","p3"]
    assert utils.rotate_proxy(proxies) in proxies

async def test_metrics():
    assert metrics['crawl_errors']._value.get() == 0

if __name__ == "__main__":
    print("Before asyncio.run(main())...")
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "Event loop is closed" in str(e):
            logger.error("Event loop closed error during main execution, likely during shutdown.")
        else:
            raise  # Re-raise other RuntimeErrors
    finally:
        print("Finally block executed, ensuring session closure...")
        try:
            asyncio.run(close_session())
            asyncio.run(utils.close_playwright()) # Close Playwright
        except RuntimeError as e_close:
            if "Event loop is closed" in str(e_close):
                logger.error("Event loop already closed during final session/playwright closure.")
            else:
                logger.error(f"Error during final cleanup: {e_close}")

        print("Session and Playwright closed in finally block.")
    print("After asyncio.run(main())...")
    # asyncio.run(test_rotation())
    # asyncio.run(test_metrics())