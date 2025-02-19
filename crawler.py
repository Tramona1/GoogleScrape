# crawler.py - FULL CODE WITH ALL IMPLEMENTED CHANGES
import re
import html # 2. Implement HTML Entity Decoding - IMPORT HTML

# REMOVE THESE FUNCTIONS FROM CRAWLER.PY:
# def normalize_phone(number): # 1. Add Phone Normalization - IMPLEMENT FUNCTION
#     return re.sub(r'\D', '', number).lstrip('1')

# # Implement HTML Entity Decoding
# def decode_html_entities(text): # 2. Implement HTML Entity Decoding - IMPLEMENT FUNCTION
#     return html.unescape(text)

# Add Deduplication Logic - for phone numbers is done in extract_phone_numbers function below, for emails is done already using set in extract_emails

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
import shelve # 12. Redis Queue Integration ðŸ“¦ - REMOVED shelve import
import json
from supabase import create_client
import utils  # Import utils
from utils import (
    metrics, rotate_proxy, decay_proxy_scores, normalize_url,
    crawl_and_extract_async, analyze_batch, get_session, proxy_health_check, close_playwright, get_playwright_instance, # <-- ENSURE close_playwright and get_playwright_instance ARE HERE
    BAD_PATH_PATTERN, GENERIC_DOMAINS, close_session, update_proxy_score, PROXY_SCORE_CONFIG # <-- ENSURE decay_scores AND update_proxy_score ARE HERE and PROXY_SCORE_CONFIG, curl_impersonated_request
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
import redis # 12. Redis Queue Integration ðŸ“¦ - ADDED redis import
import threading  # Add this import

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
     # --- California - High Priority Cities (Larger Cities & Key Smaller Places) ---
    
    "Huntington Beach",
    "Santa Monica",
    "Malibu",
    # "Temecula",
    # "Oceanside",
    # "San Clemente",
    # "Dana Point",
    # "Irvine",
    # "Mission Viejo",
    # "Riverside",
    # "Ontario",
    # "Laguna Beach",
    # "Beverly Hills",

    # "Oakland",
    # "Berkeley",
    # "San Jose",
    # "Palo Alto",
    # "Mountain View",
    # "Napa",
    # "Sonoma",
    # "Monterey",
    # "Carmel",
    # "Santa Cruz",
    # "South Lake Tahoe",
    # "Redding",
]

SEARCH_TERMS = [
    "vacation rentals",
    "short term rentals"
]

PAGES_PER_QUERY = 15
OUTPUT_CSV_FILENAME = "property_managers_data.csv"
CONCURRENT_REQUESTS = 10 # Reduced Concurrent Requests - Production Tuning
LOG_LEVEL = logging.DEBUG
# FIX 3: Content Length Requirements - Adjust length threshold:
MIN_CONTENT_LENGTH = 300  # Allow shorter contact pages # Changed here from 1200 to 300
# FIX 7: Internal Link Handling - Use config constant instead of hardcoded value
MAX_DEPTH = 3 # FIX 4: Missing Internal Link Following - Added MAX_DEPTH here, if not already present


USE_PROXY = os.getenv("USE_PROXY", "False").lower() == "false" # --- PROXY DISABLED ---
PROXY_HOST = os.getenv("PROXY_HOST")
PROXY_PORT = os.getenv("PROXY_PORT")
PROXY_USER = os.getenv("PROXY_USER")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

MAX_SEARCHES_PER_IP = 8
BATCH_SIZE = 25 # Increased Batch Size - Production Tuning

SEARCH_CONCURRENCY = 3  # Reduced from 20 - Production Tuning - Proportional to CONCURRENT_REQUESTS
CRAWL_CONCURRENCY = 10  # Reduced from 100 - Production Tuning - Set to CONCURRENT_REQUESTS
LLM_CONCURRENCY = 1     # Reduced from 10 - Production Tuning - Proportional to CONCURRENT_REQUESTS



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

proxy_pool_to_use = None # --- PROXY DISABLED ---
current_proxy_index = 0

# Define Semaphores - Define semaphores at the top level, before main()
SEARCH_SEM = asyncio.Semaphore(SEARCH_CONCURRENCY)
CRAWL_SEM = asyncio.Semaphore(CRAWL_CONCURRENCY)
LLM_SEM = asyncio.Semaphore(LLM_CONCURRENCY)


def rotate_proxy_crawler(proxy_pool_to_use): # Modified to accept proxy_pool_to_use
    return utils.rotate_proxy(proxy_pool_to_use) # Use imported rotate_proxy

def should_use_proxies(proxy_pool_to_use): # Added should_use_proxies
    return utils.should_use_proxies(proxy_pool_to_use) # Use imported should_use_proxies


async def process_urls_async(url_contexts, proxy_pool, crawl_semaphore): # REMOVED: session parameter
    logger.debug(f"Entering process_urls_async with {len(url_contexts)} URLs") # DEBUG LOG - ENTRY POINT
    async def controlled_crawl(context, proxy_pool, crawl_semaphore): # REMOVED: session parameter
        logger.debug(f"Entering controlled_crawl for URL: {context.get('url')}, City: {context.get('city')}") # DEBUG LOG - ENTRY POINT
        session = await get_session() # Get session from pool - SESSION POOLING - IMPORT FROM UTILS
        current_proxy = None
        proxy_key = 'no_proxy'
        use_proxy_for_crawl = False # --- PROXY DISABLED --- #should_use_proxies(proxy_pool) # Determine if proxies should be used for this crawl

        if use_proxy_for_crawl and proxy_pool:
            current_proxy = rotate_proxy_crawler(proxy_pool) # Use proxy rotation for crawling
            proxy_key = current_proxy
        else:
            current_proxy = None # Explicitly set to None for direct crawl


        try:
            async with crawl_semaphore:
                url = context['url']
                if not utils.validate_request(url):
                    metrics['crawl_errors'].labels(type='invalid_url', proxy=proxy_key).inc()
                    return None
                await asyncio.sleep(random.uniform(0.5, 1.5))
                logger.debug(f"Starting crawl_and_extract_async for {url} with proxy: {proxy_key if proxy_key != 'no_proxy' else 'Direct'}")
                data = await utils.crawl_and_extract_async(session, context, proxy_pool=None, current_proxy=None, depth=context.get('depth', 0))
                if data is None:
                    logger.warning(f"Extraction failed for URL: {url}")
                    metrics['extraction_failure'].inc()
                logger.debug(f"Finished crawl_and_extract_async for {context['url']}, Data: {data}")
                print(f"DEBUG (controlled_crawl): Data extracted from {context['url']}: {data}")
                if data and data.url:
                    db_saved = save_to_supabase(supabase_client, data)
                    if not db_saved:
                        save_to_csv([data], context.get("batch_number", 0))
                    return data
                print(f"DEBUG (controlled_crawl): Extraction failed for {context['url']}")
                return None
        except Exception as e_controlled_crawl:
            if "Rate limit" in str(e_controlled_crawl):
                logger.warning(f"Rate limit encountered in controlled_crawl for {context.get('url')}: {e_controlled_crawl}")
                metrics['crawl_errors'].labels(type='rate_limit', proxy=proxy_key).inc()
            if isinstance(e_controlled_crawl, aiohttp.ClientProxyConnectionError):
                logger.warning(f"Proxy connection error in controlled_crawl for {context.get('url')}: {e_controlled_crawl}. Continuing without proxy if direct scrape fails.")
            logger.exception(f"Exception in controlled_crawl for {context.get('url')}: {e_controlled_crawl}")
            metrics['crawl_errors'].labels(type='exception', proxy=proxy_key).inc()
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


async def get_google_search_results(city, term, proxy_pool):
    global search_count, proxy_pool_to_use, current_proxy_index
    logger.debug(f"Entering get_google_search_results for city={city}, term={term}")
    all_urls = []
    next_page_url = None  # Initialize next_page_url
    page_num = 1 # Track page number for logging

    params = {
        "engine": "google",
        "q": f'"{term}" "{city}"',  # Add quotes for exact phrase matching
        "gl": "us",
        "hl": "en",
        "num": 150,  # Reduced num for testing - Reduced num for testing - Reduced num for testing
        "api_key": SERPAPI_API_KEY,
        "async": True,
        "no_cache": True,
        "exclude_domains": ".gov" # Filter out .gov domains
    }
    params['exclude_domains'] = ".gov" # Filter out .gov domains
    current_proxy = None
    proxy_key = 'no_proxy'
    use_proxy_for_serpapi = False # --- PROXY DISABLED --- #should_use_proxies(proxy_pool)


    if USE_PROXY and use_proxy_for_serpapi and proxy_pool and len(proxy_pool) > 0:
        current_proxy = rotate_proxy_crawler(proxy_pool)
        proxy_key = current_proxy

    try:
        GoogleSearch.SERPAPI_HTTP_PROXY = None

        while page_num <= PAGES_PER_QUERY: # Paginate up to PAGES_PER_QUERY (e.g., 15)
            logger.info(f"Fetching SerpAPI page {page_num} for {city} {term}...") # Log page number
            if next_page_url: # Use next_page_url if available (for pagination)
                search = GoogleSearch({"api_key": SERPAPI_API_KEY, "async": True, "no_cache": True, "url": next_page_url}) # Use 'url' param for pagination
            else: # Initial search
                search = GoogleSearch(params)

            try: # Apply SEARCH_SEM with timeout
                async with async_timeout.timeout(45):
                    async with SEARCH_SEM:
                        search = GoogleSearch(params)
                        time.sleep(10) # <--- ADD THIS DELAY - Delay before get_dict
                        results = await asyncio.to_thread(search.get_dict)
                        logger.debug(f"Raw SerpAPI response: {json.dumps(results, indent=2)}") # <--- ADDED DEBUG LOGGING HERE - Raw response logging
                        if 'error' in results: # Debug and Fix SerpAPI Queries - Add detailed logging for SerpAPI errors
                            logger.error(f"SerpAPI error for query '{city} {term}', page {page_num}: {results['error']}") # Debug and Fix SerpAPI Queries - Add detailed logging for SerpAPI errors
                            metrics['serpapi_errors'].inc() # Monitor Rate-Limiting and CAPTCHA Challenges - metrics to track rate-limiting and CAPTCHA challenges
                            break # Stop pagination on error


                        # Wait for async results to complete
                        if results.get("search_metadata", {}).get("status") == "Processing":
                            json_endpoint = results["search_metadata"]["json_endpoint"]
                            retries = 0
                            while retries < 5 and results.get("search_metadata", {}).get("status") == "Processing":
                                await asyncio.sleep(2)
                                results = await fetch_serpapi_json(json_endpoint)
                                retries += 1


            except asyncio.TimeoutError:
                logger.error(f"Timeout in SEARCH_SEM for {city} {term}, page {page_num}. Skipping page.")
                metrics['serpapi_errors'].inc()
                serpapi_usage["failed_searches"] += 1
                break # Skip to next city/term

            if 'search_metadata' not in results or results['search_metadata'].get('status') == ' à¦•à§à¦¯à¦¾à¦ªà¦šà¦¾ ':
                logger.warning(f"SerpAPI CAPTCHA or error on page {page_num} for {city} {term}. Stopping pagination.")
                metrics['captcha_requests'].inc()
                metrics['captcha_failed'].inc()
                if USE_PROXY and current_proxy and use_proxy_for_serpapi:
                    utils.update_proxy_score(proxy_key, False) # Debug and Fix SerpAPI Queries - Ensure proper handling of async results
                break # Stop pagination

            original_url_count = 0
            filtered_urls = []
            removed_domains = set()

            # Process results only when status is "Success"
            if results.get("search_metadata", {}).get("status") == "Success" and 'organic_results' in results:
                organic_results = results['organic_results']
                original_url_count = len(organic_results)

                for result in organic_results:
                    url = result.get('link')
                    if url and utils.is_valid_url(url):
                        domain = urlparse(url).netloc.lower().replace('www.', '')
                        if domain in GENERIC_DOMAINS:
                            removed_domains.add(domain)
                            logger.debug(f"SerpAPI Filter - Generic Domain: {url} - Domain: {domain}") # DEBUG LOG - GENERIC DOMAIN FILTER
                            continue
                        if utils.BAD_PATH_PATTERN.search(urlparse(url).path):
                            continue
                        filtered_urls.append(url)

                logger.info(f"Page {page_num} URLs after filter: {len(filtered_urls)}/{original_url_count}")
                logger.debug(f"Page {page_num} Removed domains: {removed_domains}")
                all_urls.extend(filtered_urls)

                # --- UPDATED PAGINATION CHECK HERE ---
                if results.get('serpapi_pagination', {}).get('next'): # Use 'serpapi_pagination' and .get('next') # Pagination fix
                    next_page_url = results['pagination']['next_link'] # Keep extracting 'next_link'
                    logger.debug(f"Pagination link found for page {page_num}: {next_page_url}")
                    page_num += 1
                else:
                    logger.info(f"No more pagination links after page {page_num} for {city} {term}.")
                    break

                if USE_PROXY and current_proxy and use_proxy_for_serpapi:
                    utils.update_proxy_score(proxy_key, True) # Debug and Fix SerpAPI Queries - Ensure proper handling of async results
            else:
                logger.warning(f"No organic results on page {page_num} for {city} {term}.")
                if USE_PROXY and current_proxy and use_proxy_for_serpapi:
                    utils.update_proxy_score(proxy_key, False) # Debug and Fix SerpAPI Queries - Ensure proper handling of async results


            serpapi_usage["total_searches"] += 1
            search_count[proxy_key] += 1
            if 'search_metadata' in results and results['search_metadata'].get('search_information', {}).get('status_code') in [429, 503]:
                logger.warning(f"SerpAPI capacity issue on page {page_num} for {city} {term}. Waiting 60s...")
                await asyncio.sleep(60)
                metrics['rate_limit_errors'].inc() # Monitor Rate-Limiting and CAPTCHA Challenges - metrics to track rate-limiting and CAPTCHA challenges


        # --- PAGINATION CHECK MOVED INSIDE 'organic_results' block --- (already done above)

    except Exception as e:
        logger.error(f"Error in get_google_search_results for {city} {term}: {traceback.format_exc()}")
        metrics['serpapi_errors'].inc()
        serpapi_usage["failed_searches"] += 1
        if USE_PROXY and current_proxy and use_proxy_for_serpapi:
            utils.update_proxy_score(proxy_key, False) # Debug and Fix SerpAPI Queries - Ensure proper handling of async results
    finally:
        GoogleSearch.SERPAPI_HTTP_PROXY = None
        logger.debug(f"Exiting get_google_search_results for city={city}, term={term}, returning {len(all_urls)} urls")

    return {"city": city, "term": term, "urls": all_urls}

async def fetch_serpapi_json(endpoint):
    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint) as response: # Added 'as response'
            return await response.json()


async def process_pagination(next_page_url, city, term, original_query): # --- Pagination Fix: Add original_query parameter ---
    """
    Processes pagination URLs from SerpAPI to fetch more results.
    """
    all_urls = []
    page_num = 2 # Start page number at 2 for logging

    while next_page_url and page_num <= PAGES_PER_QUERY: # --- Pagination Fix: Limit pagination pages ---
        logger.info(f"Fetching SerpAPI pagination page {page_num} for {city} {term}: {next_page_url}") # MODIFIED: Include city and term in log
        params = {
            "engine": "google",
            "q": original_query,  # --- Pagination Fix: Use original query ---
            "start": (page_num - 1) * 10, # --- Pagination Fix: Use start parameter ---
            "api_key": SERPAPI_API_KEY,
            "async": True,
            "no_cache": True
        }
        current_proxy = None
        proxy_key = 'no_proxy'
        use_proxy_for_serpapi = False # --- PROXY DISABLED --- #should_use_proxies(proxy_pool)

        # if USE_PROXY and use_proxy_for_serpapi and proxy_pool:
        #     current_proxy = rotate_proxy_crawler(proxy_pool)
        #     proxy_key = current_proxy

        try:
            GoogleSearch.SERPAPI_HTTP_PROXY = None # Ensure SerpAPI uses its own proxy

            async with async_timeout.timeout(45): # Timeout for pagination requests
                async with SEARCH_SEM:
                    search = GoogleSearch(params)
                    results = await asyncio.to_thread(search.get_dict) # --- Pagination Fix: Use get_dict instead of search ---
                    if 'error' in results: # Debug and Fix SerpAPI Queries - Add detailed logging for SerpAPI errors
                        logger.error(f"SerpAPI error for pagination query '{city} {term}', page {page_num}: {results['error']}") # Debug and Fix SerpAPI Queries - Add detailed logging for SerpAPI errors
                        metrics['serpapi_errors'].inc() # Monitor Rate-Limiting and CAPTCHA Challenges - metrics to track rate-limiting and CAPTCHA challenges
                        break # Stop pagination on error


            original_url_count = 0
            filtered_urls = []
            removed_domains = set()

            if 'organic_results' in results:
                organic_results = results['organic_results']
                original_url_count = len(organic_results)

                for result in organic_results:
                    url = result.get('link')
                    if url and utils.is_valid_url(url):
                        domain = urlparse(url).netloc.lower().replace('www.', '')
                        if domain in GENERIC_DOMAINS:
                            removed_domains.add(domain)
                            continue
                        if utils.BAD_PATH_PATTERN.search(urlparse(url).path):
                            continue
                        filtered_urls.append(url)

                logger.info(f"Pagination page {page_num} URLs after filter: {len(filtered_urls)}/{original_url_count}")
                logger.debug(f"Pagination page {page_num} Removed domains: {removed_domains}")
                all_urls.extend(filtered_urls)

                if 'pagination' in results and 'next' in results['pagination']:
                    next_page_url = results['pagination']['next'] # --- Keep next_page_url extraction for now, but might not be needed ---
                    logger.debug(f"Found next pagination link for page {page_num}: {next_page_url}")
                else:
                    next_page_url = None # No more pagination
            else:
                logger.warning(f"No organic results in pagination page {page_num} for {city} {term}")
                next_page_url = None # Stop pagination if no results

        except Exception as e_page:
            logger.error(f"Error fetching pagination page {page_num} for {city} {term}: {e_page}")
            next_page_url = None # Stop pagination on error
        finally:
            page_num += 1
            GoogleSearch.SERPAPI_HTTP_PROXY = None # Reset proxy

    logger.info(f"Fetched {len(all_urls)} URLs from pagination for {city} {term}")
    return all_urls


def save_to_supabase(supabase_client, data):
    """Saves extracted data to Supabase database (now synchronous)."""
    try:
        # --- Data Validation ---
        required_fields = ['url']  # This is good but not enforced
        if not data.url:  # Check data.url now
            logger.warning(f"Missing website URL, skipping insert")
            return False

        # FIX 1: Overly Strict Contact Requirements - Relax contact info requirement
        if not (data.email or data.phoneNumber):
            logger.warning(f"Skipping {data.url} - no contact info")
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
        "status" # MODIFIED: Include status in CSV export
        # Removed old fields like "company_name", "website_url", etc.
    ]

    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data_item in data_list:
            writer.writerow({
                "name": data_item.name,
                "email": ", ".join(map(str, data_item.email)), # Ensure emails are strings before joining
                "phone_number": ", ".join(map(str, data_item.phoneNumber), ),
                "city": data_item.city,
                "url": str(data_item.url) if data_item.url else "N/A", # Handle potential None for URL
                "search_keywords": ", ".join(data_item.searchKeywords),
                "status": data_item.status # MODIFIED: Include status in CSV export
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

    proxy_pool_to_use = None # --- PROXY DISABLED ---
    if os.getenv("USE_PROXY", "False").lower() == "true":
        proxy_pool_to_use = utils.get_proxy_pool()
        asyncio.create_task(utils.proxy_health_check(proxy_pool_to_use)) # Start proxy health check

    error_budget["total"] = len(CITIES) * len(SEARCH_TERMS) * PAGES_PER_QUERY # Recalculate error budget


    def proxy_decay_scheduler(): # Removed async for thread compatibility
        while True:
            utils.decay_proxy_scores() # Call proxy score decay - but it's commented out in utils.py now
            time.sleep(3600) # Use time.sleep for thread-based scheduler

    # threading.Thread(target=proxy_decay_scheduler, daemon=True).start() # Start proxy decay as thread # --- PROXY DISABLED ---
    logger.info("Proxy score decay scheduler started.") # --- PROXY DISABLED ---

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

    # Clear the crawl_queue
    try:
        queue_length_before_clear = r_client.llen('crawl_queue')
        if queue_length_before_clear > 0:
            logger.info("Clearing existing crawl queue in Redis...")
            r_client.delete('crawl_queue')
            logger.info("Crawl queue cleared.")
        else:
            logger.info("Crawl queue is already empty or does not exist.")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Error connecting to Redis to clear queue: {e}")
    except Exception as e:
        logger.error(f"Unexpected error clearing Redis queue: {e}")



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
                    # --- CORRECTED CONTEXT CREATION ---
                    context = {
                        'city': search_results["city"],
                        'term': search_results["term"]  # Use search_results['term'] here!
                    }
                    r_client.lpush('crawl_queue', json.dumps(context | {'url': utils.normalize_url(url)})) # MERGED: Use context dict and add URL
                    logger.info(f"Added URL to queue: {normalized_url} for city: {search_results['city']}") # INFO LOG - URL ADDED TO QUEUE WITH CITY
                    logger.debug(f"DEBUG: Full context added to queue: {json.dumps(context | {'url': utils.normalize_url(url)})}") # DEBUG LOG - FULL CONTEXT ADDED

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
                if llm_batch and False: # LLM Batch Processing - DISABLED - keep disabled for now
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
        metrics['url_attempts'].labels(status='success').inc(sum(1 for r in extracted_data_list if r is not None)) # MODIFIED: Track success


        batch_number += 1
        current_batch_index += 1
        url_contexts_queue = r_client.lrange('crawl_queue', 0, -1)
        logger.debug(f"DEBUG: Remaining URL queue length: {len(url_contexts_queue)}") # DEBUG LOG - REMAINING QUEUE LENGTH

        await asyncio.sleep(random.uniform(1, 5))  # Random delay 1-5s # MODIFIED: Request Jitter - Random delay before next batch

    logger.info("Finished processing all URLs from queue.")
    logger.info(f"SerpAPI Usage Summary: Total Searches: {serpapi_usage['total_searches']}, Failed Searches: {serpapi_usage['failed_searches']}")
    logger.info(f"Search Counts per Proxy/No Proxy: {search_count}")
    logger.info(f"Proxy Scores: {utils.PROXY_SCORES}") # --- PROXY DISABLED ---
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
    # asyncio.run(test_metrics())?