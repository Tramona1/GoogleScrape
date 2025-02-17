# crawler.py - FINALIZED version with critical fixes and robots.txt commented out
print("Script execution started...")

from dotenv import load_dotenv
load_dotenv()
import os
import asyncio
from urllib.parse import urlparse
import aiohttp
from serpapi import GoogleSearch
import time
import logging
from supabase import create_client
from utils import (
    metrics, rotate_proxy, decay_proxy_scores, normalize_url, can_crawl, # Import can_crawl
    crawl_and_extract_async, analyze_batch, get_session, close_session_pool,
    BAD_PATH_PATTERN, GENERIC_DOMAINS
)
from prometheus_client import start_http_server, Counter
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, wait_exponential_jitter # Import tenacity decorators

# Configuration
CITIES = ["New York City", "Los Angeles"]
SEARCH_TERMS = ["vacation rentals"]
CONCURRENT_REQUESTS = 75
SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SERPAPI_API_KEY:
    raise EnvironmentError("SERPAPI_API_KEY environment variable not set.")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise EnvironmentError("SUPABASE_URL and SUPABASE_KEY environment variables must be set for database storage.")


# Logging setup
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
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


# Prometheus metrics initialization - Ensure metrics are initialized after utils import
# METRICS INITIALIZATION REMOVED FROM HERE - NO DUPLICATION - USING METRICS FROM utils.py


# Initialization
start_http_server(8002)
supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def process_urls_async(url_contexts, proxy_pool):
    async def worker(context):
        session = await get_session()
        try:
            data = await crawl_and_extract_async(session, context, proxy_pool)
            if data: # Check if data is not None (crawl and extraction successful)
                metrics['crawled_pages'].inc()
                return data
            else:
                metrics['extraction_failure'].inc() # Increment extraction_failure if crawl_and_extract_async returns None
                return None # Propagate None to indicate failure
        except Exception as e:
            logger.exception(f"Exception in worker for {context.get('url')}: {e}") # Log full exception in worker
            metrics['extraction_failure'].inc()
            return None

    results = await asyncio.gather(*[worker(ctx) for ctx in url_contexts])
    # Filter out None results (failed extractions) before returning
    return [result for result in results if result is not None]


@retry( # Apply tenacity retry to SerpAPI calls
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=2, max=30, jitter=1),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    reraise=True
)
async def get_google_search_results(city, term, proxy_pool):
    params = {
        "engine": "google",
        "q": f"{term} in {city}",
        "gl": "us",
        "hl": "en",
        "num": 10,
        "api_key": SERPAPI_API_KEY,
        "async": True,
        "no_cache": True
    }
    try:
        search = GoogleSearch(params)
        results = await asyncio.to_thread(search.get_dict)
        if 'organic_results' in results:
            urls = [result.get('link') for result in results['organic_results'] if result.get('link')]
            valid_urls = [url for url in urls if url and not BAD_PATH_PATTERN.search(urlparse(url).path) and urlparse(url).netloc not in GENERIC_DOMAINS]
            return valid_urls
        else:
            logger.warning(f"No organic results for {city} {term}")
            metrics['serpapi_errors'].inc()
            serpapi_usage["failed_searches"] += 1
            return []
    except Exception as e:
        logger.error(f"SerpAPI search failed for {city} {term}: {e}")
        metrics['serpapi_errors'].inc()
        serpapi_usage["failed_searches"] += 1
        return []

async def save_to_supabase(supabase_client, data):
    try:
        if not data.get('website_url'):
            logger.warning(f"Missing website URL, skipping insert")
            return False

        existing = supabase_client.table('property_managers') \
                .select('website_url') \
                .eq('website_url', str(data['website_url'])) \
                .execute()

        if existing.data:
            logger.warning(f"Duplicate URL skipped: {data['website_url']}")
            return True

        data_for_supabase = {
            'website_url': str(data.get('website_url')),
            'city': str(data.get('city', 'N/A')),
            'search_term': str(data.get('search_term', 'N/A')),
            'company_name': str(data.get('company_name', 'N/A')),
            'email_addresses': data.get('email_addresses', []),
            'phone_numbers': data.get('phone_numbers', []),
        }

        response = supabase_client.table('property_managers').insert(data_for_supabase).execute()
        if response.error:
            logger.error(f"Supabase insert failed: {response.error.message}")
            return False

        logger.debug(f"Data saved successfully to Supabase, website_url: {data.get('website_url')}")
        return True

    except Exception as e_supabase:
        logger.exception(f"Supabase error: {e_supabase}")
        return False


async def main():
    print("Entering main function...")
    start_time = time.time()
    logger.info("Crawler started...")

    proxy_pool_to_use = []
    if os.getenv("USE_PROXY", "False").lower() == "true":
        proxy_pool_to_use = [os.getenv("PROXY_HOST")] # PROXY_POOL_TO_USE IS NOW A LIST
        print("DEBUG: PROXY_POOL from utils:", proxy_pool_to_use)
    else:
        print("DEBUG: PROXY_POOL from utils: Proxy Usage Disabled")

    error_budget["total"] = len(CITIES) * len(SEARCH_TERMS)

    url_contexts_queue = []
    for city in CITIES:
        for term in SEARCH_TERMS:
            search_query = f"{term} in {city}"
            print(f"  Preparing SerpAPI search for: Query={search_query}")
            urls = await get_google_search_results(city, search_query, proxy_pool_to_use)
            if urls:
                print(f"    Found {len(urls)} URLs in SerpAPI results.")
                for url in urls:
                    normalized_url = normalize_url(url)
                    url_contexts_queue.append({'url': normalized_url, 'city': city, 'term': term})
                    logger.info(f"Added URL to queue: {normalized_url}")
            else:
                print(f"    No search results received from SerpAPI for {city} {term}.")
                logger.warning(f"No search results for City: {city}, Search Term: {term}")

    print(f"  Total URLs added to queue: {len(url_contexts_queue)}")
    logger.info(f"Initial URL queue populated with {len(url_contexts_queue)} URLs.")


    batch_number = 0
    async with get_session() as session: # USE CONTEXT MANAGER FOR SESSION
        while url_contexts_queue:
            batch_size = min(CONCURRENT_REQUESTS, len(url_contexts_queue))
            url_contexts_batch = []
            for _ in range(batch_size):
                url_contexts_batch.append(url_contexts_queue.pop(0))

            extracted_data_list = await process_urls_async(url_contexts_batch, proxy_pool_to_use)

            if extracted_data_list:
                for data in extracted_data_list:
                    if data and data.get('website_url'):
                        db_saved = await save_to_supabase(supabase_client, data)
                        if db_saved:
                            error_budget["success"] += 1
                        else:
                            error_budget["failed"] += 1
                    else:
                        error_budget["failed"] += batch_size
            else:
                error_budget["failed"] += batch_size # Assume all failed if process_urls_async returns None

            logger.info(f"Extracted data from {len(extracted_data_list if extracted_data_list else [])} websites in batch {batch_number} from queue.")
            batch_number += 1
            await asyncio.sleep(1) # Basic rate limiting - 1 second delay between batches

    logger.info("Finished processing all URLs from queue.")
    logger.info(f"SerpAPI Usage Summary: Total Searches: {serpapi_usage['total_searches']}, Failed Searches: {serpapi_usage['failed_searches']}")
    logger.info(f"Error budget summary: {error_budget}")
    logger.info(f"Prometheus metrics available at http://localhost:8002/metrics")

    # NO NEED TO MANUALLY CLOSE SESSION HERE WITH CONTEXT MANAGER
    # await close_session_pool()

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Crawler finished. Total time: {duration:.2f} seconds.")

if __name__ == "__main__":
    print("Before asyncio.run(main())...")
    asyncio.run(main())
    print("After asyncio.run(main())...")