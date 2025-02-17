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
    BAD_PATH_PATTERN, GENERIC_DOMAINS, close_session, update_proxy_score, RateLimiter # <-- ENSURE decay_scores and update_proxy_score ARE HERE, and RateLimiter
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
    # "Seattle",
    # "San Diego",
    "Arizona", # Note: "Arizona" is likely intended to be a city or region, but it's less specific
    "Bellevue",
]


SEARCH_TERMS = [
    "vacation rentals",
    # "short term rentals",
    "property managers short term rentals"
]

# PAGES_PER_QUERY = 15 # This is not used for pagination anymore, pagination pages are defined in get_google_search_results
OUTPUT_CSV_FILENAME = "property_managers_data.csv"
CONCURRENT_REQUESTS = 32 # Reduced Concurrent Requests - Point 6 - Concurrency Throttling - Reduced from 75 - Production Tuning
LOG_LEVEL = logging.DEBUG

USE_PROXY = os.getenv("USE_PROXY", "False").lower() == "true"
PROXY_HOST = os.getenv("PROXY_HOST")
PROXY_PORT = os.getenv("PROXY_PORT")
PROXY_USER = os.getenv("PROXY_USER")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

MAX_SEARCHES_PER_IP = 8
BATCH_SIZE = 25 # Increased Batch Size - Production Tuning

SEARCH_CONCURRENCY = 15  # Reduced from 20 - Production Tuning - Proportional to CONCURRENT_REQUESTS
CRAWL_CONCURRENCY = CONCURRENT_REQUESTS  # Point 6 - Concurrency Throttling - Set Crawl concurrency to CONCURRENT_REQUESTS
LLM_CONCURRENCY = 8     # Reduced from 10 - Production Tuning - Proportional to CONCURRENT_REQUESTS
JITTER_PROCESS_URLS = (1, 3) # Concurrency Throttling - Point 6 - Jitter range for process_urls_async


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
    jitter_delay = random.uniform(JITTER_PROCESS_URLS[0], JITTER_PROCESS_URLS[1]) # Concurrency Throttling - Point 6 - Jitter Delay
    await asyncio.sleep(jitter_delay) # Concurrency Throttling - Point 6 - Jitter Delay

    async def controlled_crawl(context, proxy_pool, crawl_semaphore): # REMOVED: session parameter
        logger.debug(f"Entering controlled_crawl for URL: {context.get('url')}, City: {context.get('city')}") # DEBUG LOG - ENTRY POINT
        session = await get_session() # Get session from pool - SESSION POOLING - IMPORT FROM UTILS
        try: # Error Recovery Flow - Point 7 - Add try-except block - Controlled Crawl
            async with crawl_semaphore:
                url = context['url']
                if not utils.validate_request(url):
                    metrics['crawl_errors'].labels(type='invalid_url', proxy=proxy_pool[0] if proxy_pool else 'no_proxy').inc() # Using first proxy for labeling if pool exists
                    return None

                logger.debug(f"Starting crawl_and_extract_async for {url}")
                data = await crawl_and_extract_async(session, context, proxy_pool=proxy_pool) # Use imported function and pass session # Timeout Optimization - Point 2 - Wrapped with timeout in utils
                logger.debug(f"Finished crawl_and_extract_async for {context['url']}, Data: {data}") # DEBUG LOG - DATA AFTER CRAWL_AND_EXTRACT
                print(f"DEBUG (controlled_crawl): Data extracted from {context['url']}: {data}")
                if data and data.url: # Line 379 - Correct Indentation - CORRECTED .get() to attribute access
                    # Supabase Insertion Fix - Ensure searchKeywords is populated from context and validate
                    data.searchKeywords = [context['term']] # Ensure searchKeywords is populated from context
                    if not data.url or not data.searchKeywords: # Validate required fields
                        logger.error(f"Skipping invalid data - URL: {data.url}, Keywords: {data.searchKeywords}")
                        return None

                    db_saved = save_to_supabase(supabase_client, data) # Line 380 - Correct Indentation
                    if not db_saved: # Line 381 - Correct Indentation
                        save_to_csv( [data], context.get("batch_number", 0)) # Line 382 - Correct Indentation
                    return data # Line 383 - Correct Indentation
                print(f"DEBUG (controlled_crawl): Extraction failed for {context['url']}") # Line 384 - Correct Indentation
                return None # Line 385 - Correct Indentation
        except Exception as e_controlled_crawl: # Error Recovery Flow - Point 7 - Catch exceptions in controlled_crawl
            logger.error(f"Exception in controlled_crawl for {context.get('url')}: {e_controlled_crawl}") # Error Recovery Flow - Point 7 - Log exception
            metrics['crawl_errors'].labels(type='controlled_crawl_exception', proxy=proxy_pool[0] if proxy_pool else 'no_proxy').inc() # Error Recovery Flow - Point 7 - Metric for controlled_crawl errors
            metrics['failed_urls'].inc() # Error Recovery Flow - Point 7 - Increment failed_urls counter
            return None # Error Recovery Flow - Point 7 - Return None on exception


    extracted_data_list = []

    async def controlled_crawl_wrapper(context, proxy_pool, crawl_semaphore): # REMOVED: session parameter
        return await controlled_crawl(context, proxy_pool, crawl_semaphore) # REMOVED: session parameter

    try: # Error Recovery Flow - Point 7 - Add try-except block - process_urls_async
        tasks = [controlled_crawl_wrapper(context, proxy_pool, crawl_semaphore) for context in url_contexts] # REMOVED: session parameter
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.debug(f"DEBUG: (process_urls_async): Results from asyncio.gather: {results}") # DEBUG LOG - ASYNCIO.GATHER RESULTS
        print("DEBUG: (process_urls_async): Results from asyncio.gather:", results)
        extracted_data_list = [result for result in results if result is not None]
        logger.debug(f"DEBUG: Exiting process_urls_async, returning: {extracted_data_list}") # DEBUG LOG - EXITING FUNCTION
        print("DEBUG: Exiting process_urls_async, returning:", extracted_data_list)
        return extracted_data_list
    except Exception as e_process_urls: # Error Recovery Flow - Point 7 - Catch exceptions in process_urls_async
        logger.error(f"Exception in process_urls_async: {e_process_urls}") # Error Recovery Flow - Point 7 - Log exception
        metrics['crawl_errors'].labels(type='process_urls_exception', proxy=proxy_pool[0] if proxy_pool else 'no_proxy').inc() # Error Recovery Flow - Point 7 - Metric for process_urls_async errors
        metrics['failed_urls'].inc(len(url_contexts)) # Error Recovery Flow - Point 7 - Increment failed_urls counter for all URLs in batch
        return None # Error Recovery Flow - Point 7 - Return None on exception


@retry(
    stop=stop_after_attempt(3), # Retry up to 3 times
    wait=wait_exponential(multiplier=1, min=4, max=30), # Exponential backoff (4s, 8s, 16s, up to 30s)
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)), # Retry on these exceptions
    reraise=True # If all retries fail, raise the last exception
)
async def get_google_search_results(city, term, proxy_pool): # SerpAPI Pagination Fix # Timeout Optimization - Point 2 - Not wrapping SerpAPI calls with timeout - using retry instead
    global search_count, proxy_pool_to_use, current_proxy_index
    logger.debug(f"Entering get_google_search_results for city={city}, term={term}") # DEBUG LOG - ENTRY POINT
    all_urls = []
    next_page_token = None  # SerpAPI Pagination Fix
    page_num = 0 # Initialize page_num for logging
    max_pages = 7  # Scrape 7 pages per search # Pagination Fix - Increased max_pages from 1 to 7
    while True: # SerpAPI Pagination Fix
        if page_num >= max_pages: # --- ADD THIS CHECK ---
            logger.info(f"Pagination: Reached max page limit ({max_pages} pages). Stopping pagination.") # --- NEW LOG ---
            break # Stop pagination if max pages reached # --- ADD THIS BREAK ---

        params = {
            "engine": "google",
            "q": f"{term} in {city}",
            "gl": "us",
            "hl": "en",
            "num": "100",  # Max results per page # SerpAPI Pagination Fix
            "api_key": SERPAPI_API_KEY,
            "async": True,
            "no_cache": True,
            "location": city,
            "google_domain": "google.com", # SerpAPI Parameter Optimization
            "device": "desktop", # SerpAPI Parameter Optimization
            "filter": "0",  # Disable auto-filtering # SerpAPI Parameter Optimization
            "safe": "active"  # For compliance # SerpAPI Parameter Optimization
        }

        if next_page_token: # SerpAPI Pagination Fix
            params["serpapi_pagination"] = next_page_token # SerpAPI Pagination Fix
        else:
            params["start"] = page_num * 100  # Only for initial page # SerpAPI Pagination Fix

        current_proxy = None
        proxy_key = 'no_proxy'

        if USE_PROXY and proxy_pool and len(proxy_pool) > 0:
            current_proxy = utils.rotate_proxy(proxy_pool)
            proxy_key = current_proxy

            if search_count[proxy_key] >= MAX_SEARCHES_PER_IP:
                logger.warning(f"Search limit reached for proxy: {current_proxy}. Rotating...")
                current_proxy = rotate_proxy_crawler()
                if current_proxy:
                    proxy_key = current_proxy
                else:
                    logger.warning("No proxies available after rotation, skipping search for this city/term.")
                    return {"city": city, "term": term, "urls": []}

        try:
            if USE_PROXY and current_proxy:
                GoogleSearch.SERPAPI_HTTP_PROXY = f"https://{current_proxy}"
                logger.debug(f"SerpAPI search using proxy: {current_proxy}, Page {page_num + 1}") # Added page number to log
            else:
                GoogleSearch.SERPAPI_HTTP_PROXY = None
                logger.debug(f"SerpAPI search without proxy, Page {page_num + 1}") # Added page number to log

            logger.debug(f"DEBUG: API Key being used for SerpAPI search (Page {page_num + 1}): {params['api_key']}") # Keep existing log
            logger.debug(f"DEBUG: SerpAPI Query (Page {page_num + 1}): {params['q']}") # DEBUG LOG - SERPAPI QUERY - **CONFIRM CITY HERE**
            try:
                async with async_timeout.timeout(45): # Timeout Optimization - Point 2 - Timeout for acquiring SEARCH_SEM - Reduced from utils CLIENT_TIMEOUT
                    async with SEARCH_SEM:
                        search = GoogleSearch(params)
                        initial_result = await asyncio.to_thread(search.get_dict)
                        logger.debug(f"DEBUG: Initial SerpAPI result metadata (Page {page_num + 1}): {initial_result.get('search_metadata')}")

                        # --- ADD THESE DEBUG LOGS ---
                        logger.debug(f"DEBUG: Raw initial_result (Page {page_num + 1}): {initial_result}") # Log the ENTIRE raw initial_result
                        try:
                            logger.debug(f"DEBUG: Pretty initial_result (Page {page_num + 1}):\n{json.dumps(initial_result, indent=2)}") # Pretty print for readability
                        except Exception as e_json:
                            logger.error(f"DEBUG: Error pretty printing initial_result: {e_json}")
                        # --- END ADDED DEBUG LOGS ---

            except asyncio.TimeoutError:
                logger.error(f"Timeout acquiring SEARCH_SEM for {city} {term}, Page {page_num + 1}. Skipping page.")
                metrics['serpapi_errors'].inc()
                serpapi_usage["failed_searches"] += 1
                continue # Skip to the next page if timeout

            if 'search_metadata' not in initial_result:
                logger.error(f"SerpAPI search failed for {city} {term}, Page {page_num + 1}: No metadata in response")
                serpapi_usage["failed_searches"] += 1
                if USE_PROXY and current_proxy:
                    utils.update_proxy_score(proxy_key, False)
                continue # Skip to the next page if no metadata

            if initial_result['search_metadata'].get('status') == ' ক্যাপচা ':
                logger.warning(f"SerpAPI returned CAPTCHA for {city} {term}, Page {page_num + 1} using proxy: {current_proxy}")
                metrics['captcha_requests'].inc()
                metrics['captcha_failed'].inc()
                if USE_PROXY and current_proxy:
                    utils.update_proxy_score(proxy_key, False)
                continue # Skip to the next page if CAPTCHA

            search_id = initial_result['search_metadata']['id']
            logger.debug(f"DEBUG: Search ID after submission (Page {page_num + 1}): {search_id}")
            logger.info(f"Search ID {search_id} (Page {page_num + 1}) submitted. Waiting 10 seconds for results...")
            await asyncio.sleep(10)

            max_retries = 3
            archived_search = None # Initialize outside the loop
            for attempt in range(max_retries):
                retrieval_url = f"https://serpapi.com/searches/{search_id}.json"
                retrieval_params = {
                    "api_key": SERPAPI_API_KEY,
                }
                param_string = "&".join([f"{key}={value}" for key, value in retrieval_params.items()])
                full_retrieval_url = f"{retrieval_url}?{param_string}"
                logger.debug(f"DEBUG: Full Retrieval URL (Page {page_num + 1}, Attempt {attempt+1}): {full_retrieval_url}")
                logger.debug(f"DEBUG: Retrieval Search ID (Page {page_num + 1}, Attempt {attempt+1}): {search_id}")

                archived_search = GoogleSearch(retrieval_params).get_search_archive(search_id)

                if 'organic_results' in archived_search:
                    logger.debug(f"DEBUG: Organic results found for {city} {term}, Page {page_num + 1} on attempt {attempt+1}")
                    break
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error(f"Max retries reached for {search_id}, Page {page_num + 1}")
                serpapi_usage["failed_searches"] += 1
                if USE_PROXY and current_proxy:
                    utils.update_proxy_score(proxy_key, False)
                continue # Skip to the next page if retrieval fails

            if 'organic_results' in archived_search:
                logger.debug(f"Raw SerpAPI results (Page {page_num + 1}): {archived_search['organic_results']}")
                for result in archived_search['organic_results']:
                    url = result.get('link')
                    if url and utils.is_valid_url(url):
                        all_urls.append(url)
                        logger.debug(f"Accepted URL (Page {page_num + 1}): {url}, City: {city}, Term: {term}") # DEBUG LOG - ACCEPTED URL - **CONFIRM CITY HERE**
                logger.info(f"Found {len(archived_search['organic_results'])} URLs on page {page_num + 1} for {city} {term}") # Updated log message
                utils.update_proxy_score(proxy_key, True)
            else:
                logger.warning(f"No organic results in archived search for {city} {term}, Page {page_num + 1}")
                if USE_PROXY and current_proxy:
                    utils.update_proxy_score(proxy_key, False)

            serpapi_usage["total_searches"] += 1
            search_count[proxy_key] += 1

            if "pagination" in initial_result and "next_page_token" in initial_result["pagination"]: # SerpAPI Pagination Fix
                next_page_token = initial_result["pagination"]["next_page_token"] # SerpAPI Pagination Fix
                page_num += 1 # Increment page number # SerpAPI Pagination Fix
                logger.info(f"Pagination: Next page token found, moving to page {page_num + 1}") # SerpAPI Pagination Fix
            else:
                logger.info("Pagination: No next page token found, stopping pagination.") # SerpAPI Pagination Fix
                break # Stop pagination if no next page token # SerpAPI Pagination Fix


            if 'search_metadata' in initial_result and 'search_information' in initial_result.get('search_metadata', {}):
                if initial_result['search_metadata']['search_information'].get('status_code') in [429, 503]:
                    logger.warning(f"SerpAPI capacity issue: {initial_result['search_metadata']['search_information'].get('status_code')} for {city} {term}, Page {page_num + 1}. Waiting 60s...")
                    await asyncio.sleep(60)


        except Exception as e:
            logger.error(f"Inner SerpAPI search call failed for {city} {term}, Page {page_num + 1}: {traceback.format_exc()}")
            metrics['serpapi_errors'].inc()
            serpapi_usage["failed_searches"] += 1
            if USE_PROXY and current_proxy:
                utils.update_proxy_score(proxy_key, False)
            continue # Skip to the next page if error
        finally:
            GoogleSearch.SERPAPI_HTTP_PROXY = None

    logger.info(f"Collected {len(all_urls)} URLs across pages for {city} {term}") # Summary log - Updated log message
    logger.debug(f"Exiting get_google_search_results for city={city}, term={term}, returning {len(all_urls)} urls")
    return {"city": city, "term": term, "urls": all_urls}


def save_to_supabase(supabase_client, data):
    """Saves extracted data to Supabase database (now synchronous)."""
    try:
        # --- Data Validation ---
        if not data.city or data.city not in CITIES: # **VALIDATION FIX - ADDED CITY VALIDATION HERE**
            logger.error(f"Invalid city detected: {data.city}. Skipping insert.") # **VALIDATION FIX**
            return False # **VALIDATION FIX**
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
        logger.debug(f"Supabase Insert Payload: {data_for_supabase}") # DEBUG LOG - SUPABASE PAYLOAD - **CONFIRM CITY HERE**

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

        logger.debug(f"Data saved successfully to Supabase, id: {response.data[0]['id']}, url: {data.url}, city: {data.city}") # Improved success log - include id and URL and CITY - **CONFIRM CITY HERE**
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

    error_budget["total"] = len(CITIES) * len(SEARCH_TERMS) * 5 # PAGES_PER_QUERY is not used, using 10 as default page count for error budget


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

    r_client = redis.Redis(decode_responses=True) # Redis Connection Resilience - defined here
    # Redis Connection Resilience - Add Redis connection verification
    try:
        if not r_client.ping():
            raise redis.ConnectionError("Initial Redis connection failed")
    except redis.RedisError as e:
        logger.critical(f"Redis connection failed: {str(e)}")
        # Implement reconnection logic here
        exit(1)

    # --- ADD THIS CODE TO CLEAR THE QUEUE AT THE START ---
    try:
        queue_len = r_client.llen('crawl_queue')
        if queue_len > 0:
            r_client.delete('crawl_queue') # Clear the queue
            logger.info(f"Cleared existing Redis crawl queue containing {queue_len} URLs.")
        else:
            logger.info("Redis crawl queue was empty, no need to clear.")
    except redis.RedisError as e_clear_queue:
        logger.error(f"Error clearing Redis queue at startup: {e_clear_queue}")
    # --- END QUEUE CLEARING CODE ---


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
                    # --- CRITICAL FIX - USE LAMBDA TO CAPTURE city and term ---
                    search_tasks.append(lambda c=city, t=term: get_google_search_results(c, t, proxy_pool_to_use)) # Pass term separately and capture city/term
                    # --- END CRITICAL FIX ---

        logger.info("Awaiting SerpAPI search tasks...")
        logger.debug(f"DEBUG: Search tasks initiated: {len(search_tasks)}") # DEBUG LOG - SEARCH TASKS COUNT
        search_results_list = await asyncio.gather(*[task() for task in search_tasks]) # Execute the lambda tasks
        logger.info("SerpAPI search tasks completed.")
        # --- ADD THESE DEBUG LOGS HERE ---
        logger.debug(f"DEBUG: Full search_results_list BEFORE REDIS POPULATION: {search_results_list}") # Log the entire list
        if search_results_list: # Check if list is not empty before iterating
            for result_set in search_results_list: # Iterate through each result set
                if result_set: # Check if result_set is not None
                    logger.debug(f"DEBUG: Search Result Set - City: {result_set.get('city')}, Term: {result_set.get('term')}, URL Count: {len(result_set.get('urls', [])) if result_set.get('urls') else 0}") # Log city and term from each result set
        # --- END ADDED DEBUG LOGS ---

        for search_results in search_results_list: # Redis Queue Fixes
            if not search_results or "urls" not in search_results: # Redis Queue Fixes
                logger.debug(f"DEBUG: Skipping invalid search_results: {search_results}") # DEBUG LOG - EMPTY SEARCH RESULTS
                continue # Redis Queue Fixes

            logger.debug(f"DEBUG: Processing search_results for city: {search_results.get('city')}, term: {search_results.get('term')}") # DEBUG LOG - PROCESSING SEARCH RESULTS
            logger.debug(f"DEBUG: Search Results for City: {search_results['city']}, Term: {search_results['term']}, URL Count: {len(search_results['urls'])}") # DEBUG LOG - SEARCH RESULTS SUMMARY - **CONFIRM CITY HERE**
            for url in search_results["urls"]: # Redis Queue Fixes
                context = {
                    "city": search_results["city"],
                    "term": search_results["term"],  # Critical fix # Redis Queue Fixes
                    "url": utils.normalize_url(url)
                }
                # --- ADD THESE DEBUG LOGS RIGHT HERE ---
                logger.debug(f"DEBUG: **BEFORE REDIS PUSH** - City: {context['city']}, Term: {context['term']}, URL: {context['url']}") # DEBUG LOG - REDIS PUSH - **CRITICAL LOG**
                logger.debug(f"DEBUG: Pushing to Redis queue - City: {context['city']}, Term: {context['term']}, URL: {context['url']}") # DEBUG LOG - REDIS PUSH - **CONFIRM CITY HERE**
                # --- END ADDED DEBUG LOGS ---
                try: # Redis Queue Fixes
                    # Verify Redis connection first # Redis Queue Fixes
                    if not r_client.ping(): # Redis Queue Fixes
                        raise redis.ConnectionError("Redis connection lost") # Redis Queue Fixes

                    r_client.lpush('crawl_queue', json.dumps(context)) # Redis Queue Fixes
                    r_client.expire('crawl_queue', 86400)  # 24h TTL # Redis Queue Fixes
                except redis.RedisError as e: # Redis Queue Fixes
                    logger.error(f"Redis error pushing {url}: {str(e)}") # Redis Queue Fixes
                    # Implement retry logic here if needed # Redis Queue Fixes

        # Force Redis persistence and refresh queue
        try:
            r_client.bgsave()
        except redis.RedisError as e:
            logger.error(f"Redis save operation failed: {e}")

        try:
            url_contexts_queue = r_client.lrange('crawl_queue', 0, -1)
            logger.info(f"Queue now contains {len(url_contexts_queue)} URLs")  # Verification
            logger.debug(f"DEBUG: Initial URL queue contents (first 10): {url_contexts_queue[:10]}") # DEBUG LOG - QUEUE CONTENTS
        except redis.RedisError as e:
            logger.error(f"Failed to read queue after save: {e}")
            url_contexts_queue = [] # Ensure queue is empty in case of read failure

    else:
        logger.info(f"Resuming from existing URL queue with {len(url_contexts_queue)} URLs.")
        logger.debug(f"DEBUG: Resuming URL queue contents (first 10): {url_contexts_queue[:10]}") # DEBUG LOG - RESUMING QUEUE CONTENTS


    batch_number = 0
    rate_limiter = RateLimiter(requests_per_second=2) # Concurrency Throttling - Point 6 - Instantiate RateLimiter

    heartbeat_counter = 0 # Monitoring Additions - Point 9 - Initialize heartbeat counter

    while url_contexts_queue:
        current_batch_index = min(int(error_budget["success"]/50), 3)
        batch_size = BATCH_SIZES[current_batch_index]
        url_contexts_batch = []

        if (error_budget["failed"] / error_budget["total"]) > 0.25:
            logger.critical("Error budget exhausted (failure rate > 25%). Exiting crawler.")
            break

        for _ in range(min(batch_size, len(url_contexts_queue))):
            try:
                url_context_str = r_client.lpop('crawl_queue')
                if url_context_str:
                    url_contexts_batch.append(json.loads(url_context_str))
            except redis.RedisError as e:
                logger.error(f"Failed to pop URL from queue: {e}")
                continue # Skip to next iteration if pop fails

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

        heartbeat_counter += len(extracted_data_list) # Monitoring Additions - Point 9 - Increment heartbeat counter
        if heartbeat_counter >= 50: # Monitoring Additions - Point 9 - HEARTBEAT_INTERVAL = 50
            logger.info(f"HEARTBEAT: Processed ~{heartbeat_counter} URLs in total.") # Monitoring Additions - Point 9 - Heartbeat log
            heartbeat_counter = 0 # Monitoring Additions - Point 9 - Reset counter


        batch_number += 1
        current_batch_index += 1
        try:
            url_contexts_queue = r_client.lrange('crawl_queue', 0, -1)
            logger.debug(f"DEBUG: Remaining URL queue length: {len(url_contexts_queue)}") # DEBUG LOG - REMAINING QUEUE LENGTH
        except redis.RedisError as e:
            logger.error(f"Failed to read queue length: {e}")
            url_contexts_queue = [] # Ensure loop terminates if queue cannot be read


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