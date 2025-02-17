# crawler.py
from dotenv import load_dotenv
load_dotenv()
import asyncio
from urllib.parse import urlparse, urljoin
import aiohttp
from serpapi import GoogleSearch
import time
import csv
import logging
import os
from datetime import datetime
import shelve
import json
from supabase import create_client # Removed APIResponse import
import utils
from prometheus_client import start_http_server, Counter, Gauge
import random
import socket
import struct
from fake_useragent import UserAgent
from tenacity import retry, wait_exponential, stop_after_attempt
import urllib.robotparser
from urllib.parse import urljoin
import ssl # Import ssl module

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
    "Austin",
    "Orlando",
    "Birmingham"
]

SEARCH_TERMS = [
    "Vacation Rentals",
]
PAGES_PER_QUERY = 2
OUTPUT_CSV_FILENAME = "property_managers_data.csv"
CONCURRENT_REQUESTS = 2
LOG_LEVEL = logging.DEBUG  # ENSURE LOG_LEVEL IS DEBUG FOR DETAILED LOGGING

USE_PROXY = os.getenv("USE_PROXY", "False").lower() == "true"
PROXY_HOST = os.getenv("PROXY_HOST")
PROXY_PORT = os.getenv("PROXY_PORT")
PROXY_USER = os.getenv("PROXY_USER")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

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

crawled_pages_metric = Counter('crawled_pages', 'Total pages crawled')
extraction_success_metric = Counter('extraction_success', 'Successful extractions')
extraction_failure_metric = Counter('extraction_failure', 'Failed extractions')
relevance_score_gauge = Gauge('avg_relevance_score', 'Average Relevance Score')
start_http_server(8001)

supabase_client = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
]


def can_crawl(url, user_agent='PropertyManagerCrawler/1.3'):
    try:
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        robots_url = urljoin(base_url, '/robots.txt')
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch(user_agent, url)
    except Exception as e:
        logger.warning(f"Robots.txt check error for {url}: {e}")
        return True


async def process_urls_async(url_contexts, proxy_pool, ssl_context):
    async def controlled_crawl(session, context): # Add session as argument here
        try: # Added try-except block
            async with semaphore:
                data = await utils.crawl_and_extract_async(session, context, proxy_pool=proxy_pool, ssl_context=ssl_context) # Pass session here
                print(f"DEBUG (controlled_crawl): Data extracted from {context['url']}: {data}")
                if data and data.get('website_url'): # Added website_url check
                    db_saved = save_to_supabase(supabase_client, data)
                    if not db_saved:
                        save_to_csv([data], context.get("batch_number", 0))
                    return data
                print(f"DEBUG (controlled_crawl): Extraction failed for {context['url']}")
                return None
        except aiohttp.ClientError as e:
            logger.warning(f"Client error in controlled_crawl for {context.get('url')}: {str(e)}")
            return None
        except Exception as e_controlled_crawl:
            if "Rate limit" in str(e_controlled_crawl):
                logger.warning(f"Rate limit encountered in controlled_crawl for {context.get('url')}: {e_controlled_crawl}")
            logger.exception(f"Exception in controlled_crawl for {context.get('url')}: {e_controlled_crawl}")
            return None

    extracted_data_list = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async def controlled_crawl_wrapper(session, context): # Modify wrapper to accept session
        async with semaphore:
            return await controlled_crawl(session, context) # Pass session to controlled_crawl

    connector = aiohttp.TCPConnector(ssl=ssl_context) # Create connector here
    async with aiohttp.ClientSession(connector=connector) as session: # Create session here and use it
        try:
            tasks = [controlled_crawl_wrapper(session, context) for context in url_contexts] # Pass session to wrapper
            results = await asyncio.gather(*tasks, return_exceptions=True)
            print("DEBUG: (process_urls_async): Results from asyncio.gather:", results)
            extracted_data_list = [result for result in results if result is not None]
            print("DEBUG: Exiting process_urls_async, returning:", extracted_data_list)
            return extracted_data_list
        except Exception as e_process_urls:
            logger.exception(f"Exception in process_urls_async: {e_process_urls}")
            return None


async def get_google_search_results(session, city, term):
    logger.debug(f"Entering get_google_search_results for city={city}, term={term}")
    all_urls = []
    params = {
        "engine": "google",
        "q": f"{city} {term}",
        "gl": "us",
        "hl": "en",
        "num": 10,
        "api_key": SERPAPI_API_KEY,
        "async": True,
        "no_cache": True
    }
    archived_params = params # Define archived_params here

    try:
        # Initial search submission
        search = GoogleSearch(params)
        initial_result = await asyncio.to_thread(search.get_dict)

        if 'search_metadata' not in initial_result:
            logger.error(f"SerpAPI search failed for {city} {term}: No metadata in response")
            return {"city": city, "term": term, "urls": []}

        search_id = initial_result['search_metadata']['id']
        logger.info(f"Search ID {search_id} submitted. Waiting 10 seconds for results...") # reduced wait time
        await asyncio.sleep(10)  # Reduced wait time to 10 seconds

        # Retry logic with backoff
        max_retries = 3
        for attempt in range(max_retries):
            archived_search = GoogleSearch(archived_params).get_search_archive(search_id) # Use defined archived_params
            if 'organic_results' in archived_search:
                break
            await asyncio.sleep(10 * (attempt + 1))  # 10s, 20s, 30s backoff
        else:
            logger.error(f"Max retries reached for {search_id}")
            return {"city": city, "term": term, "urls": []} # Return empty list on max retries


        if 'organic_results' in archived_search:
            logger.debug(f"Raw SerpAPI results: {archived_search['organic_results']}")  # Add debug logging
            for result in archived_search['organic_results']:
                url = result.get('link')
                if url and utils.is_valid_url(url):
                    all_urls.append(url)
                    logger.debug(f"Accepted URL: {url}")  # Log accepted URLs
            logger.info(f"Found {len(all_urls)} valid URLs for {city} {term}")
        else:
            logger.warning(f"No organic results in archived search for {city} {term}")

    except Exception as e:
        logger.error(f"SerpAPI search failed for {city} {term}: {str(e)}")

    return {"city": city, "term": term, "urls": all_urls}


def save_to_supabase(supabase_client, data):
    """Saves extracted data to Supabase database (now synchronous)."""
    try:
        # --- Data Validation ---
        required_fields = ['website_url']  # Only require URL
        if not data.get('website_url'):
            logger.warning(f"Missing website URL, skipping insert")
            return False
        # --- End Data Validation ---


        existing = supabase_client.table('property_managers') \
            .select('website_url') \
            .eq('website_url', str(data['website_url'])) \
            .execute()

        if existing.data:
            logger.warning(f"Duplicate URL skipped: {data['website_url']}")
            return True

        data_for_supabase = {
            'website_url': str(data.get('website_url')), # website_url is mandatory
            'social_media': data.get('social_media', []),
            'city': str(data.get('city', 'N/A')),
            'search_term': str(data.get('search_term', 'N/A')),
            'company_name': str(data.get('company_name', 'N/A')),
            'email_addresses': data.get('email_addresses', []),
            'phone_numbers': data.get('phone_numbers', []),
            'physical_address': data.get('physical_address', 'N/A'),
            'relevance_score': data.get('relevance_score', 0.0),
            'estimated_properties': data.get('estimated_properties', 0) or 0,
            'service_areas': data.get('service_areas', []),
            'management_software': data.get('management_software', []),
            'license_numbers':  data.get('license_numbers', []),
            'llm_category': data.get('llm_category', None) # LLM Category now always saved
        }
        logger.debug(f"Supabase Insert Payload: {data_for_supabase}")

        response = supabase_client.table('property_managers').insert(data_for_supabase).execute()

        logger.debug(f"Supabase response: {str(response)[:200]}...") # Improved debug logging - truncated response

        # --- UPDATED ERROR CHECKING  ---
        if hasattr(response, 'error') and response.error:
            logger.error(f"Supabase insert failed: {response.error.message}")
            return False

        if not response.data:
            logger.error("Supabase insert succeeded but returned no data")
            return False

        logger.debug(f"Data saved successfully to Supabase, id: {response.data[0]['id']}, website_url: {data.get('website_url')}") # Improved success log - include id and URL
        return True
        # --- END UPDATED ERROR CHECKING ---

    except Exception as e_supabase:
        logger.exception(f"Supabase error: {str(e_supabase)}")
        return False

def save_to_csv(data_list, batch_number):
    csv_filename = f"property_managers_data_batch_{batch_number}.csv"
    logger.info(f"Saving batch {batch_number} data to CSV file: {csv_filename} (fallback)")

    fieldnames = [
        "city", "search term", "company name", "website_url", "email addresses",
        "phone numbers", "physical address", "relevance score", "timestamp",
        "social_media", "estimated_properties", "service_areas",
        "management_software", "license_numbers",
        "llm_category"
    ]
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data_item in data_list:
            social_media_str = ", ".join([str(url) for url in data_item.get("social_media", [])])
            writer.writerow({
                "city": data_item.get("city", "N/A"),
                "search term": data_item.get("search_term", "N/A"),
                "company name": data_item.get("company_name", "N/A"),
                "website_url": data_item.get("website_url", "N/A"),
                "email_addresses": ", ".join(data_item.get("email_addresses", [])),
                "phone_numbers": ", ".join(data_item.get("phone_numbers", [])),
                "physical address": data_item.get("physical_address", "N/A"),
                "relevance_score": data_item.get("relevance_score", 0.0),
                "timestamp": datetime.now().isoformat(),
                "social_media": social_media_str,
                "estimated_properties": data_item.get("estimated_properties", 0),
                "service_areas": ", ".join(data_item.get("service_areas", [])),
                "management_software": ", ".join(data_item.get("management_software", [])),
                "license_numbers": ", ".join(data_item.get("license_numbers", [])),
                "llm_category": data_item.get("llm_category", "N/A")
            })


async def main():
    start_time = time.time()
    logger.info("Crawler started...")

    print("DEBUG: USE_PROXY from env:", os.getenv("USE_PROXY"))
    logger.debug(f"DEBUG: OXLABS_USER from env: {os.getenv('OXLABS_USER')}")
    logger.debug(f"DEBUG: OXLABS_PASS from env: {os.getenv('OXLABS_PASS')}")
    logger.debug(f"DEBUG: SMARTPROXY_USER from env: {os.getenv('SMARTPROXY_USER')}")
    logger.debug(f"DEBUG: SMARTPROXY_PASS from env: {os.getenv('SMARTPROXY_PASS')}")
    logger.debug(f"DEBUG: BRIGHTDATA_USER from env: {os.getenv('BRIGHTDATA_USER')}")
    logger.debug(f"DEBUG: BRIGHTDATA_PASS from env: {os.getenv('BRIGHTDATA_PASS')}")

    proxy_pool_to_use = None

    utils.validate_proxy_config()

    if os.getenv("USE_PROXY", "False").lower() == "true":
        proxy_pool_to_use = utils.get_proxy_pool()
        print("DEBUG: PROXY_POOL from utils:", proxy_pool_to_use)
    else:
        print("DEBUG: PROXY_POOL from utils: Proxy Usage Disabled")

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    ssl_context.set_ciphers('ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256') # Modern ciphers


    error_budget["total"] = len(CITIES) * len(SEARCH_TERMS) * PAGES_PER_QUERY


    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        with shelve.open('url_queue.db') as db:
            if 'url_contexts' not in db:
                db['url_contexts'] = []
            url_contexts_queue = db['url_contexts']

            if not url_contexts_queue:
                for city in CITIES:
                    for term in SEARCH_TERMS:
                        search_results = await get_google_search_results(session, city, term)
                        if search_results:
                            for url in search_results["urls"]:
                                url_contexts_queue.append({"city": city, "term": term, "url": url})
                            logger.info(f"Added {len(search_results['urls'])} URLs to queue for City: {city}, Search Term: {term}")
                        else:
                            logger.warning(f"No search results for City: {city}, Search Term: {term}")

                db['url_contexts'] = url_contexts_queue
                logger.info(f"Initial URL queue populated with {len(url_contexts_queue)} URLs and saved to disk.")
            else:
                logger.info(f"Resuming from existing URL queue with {len(url_contexts_queue)} URLs.")

            print("DEBUG: Initial len(url_contexts_queue):", len(url_contexts_queue))
            batch_number = 0
            while url_contexts_queue:
                print("DEBUG: len(url_contexts_queue) at loop start:", len(url_contexts_queue))
                url_contexts_batch = []
                batch_size_crawl = 10
                for _ in range(min(batch_size_crawl, len(url_contexts_queue))):
                    url_contexts_batch.append(url_contexts_queue.pop(0))

                db['url_contexts'] = url_contexts_queue

                print("DEBUG: url_contexts_batch before process_urls_async:", url_contexts_batch)
                extracted_data_list = await process_urls_async(url_contexts_batch, proxy_pool=proxy_pool_to_use, ssl_context=ssl_context)
                print(f"DEBUG (main): Type of extracted_data_list: {type(extracted_data_list)}")

                logger.info(f"Extracted data from {len(extracted_data_list)} websites in batch {batch_number} from queue.")
                error_budget["success"] += sum(1 for r in extracted_data_list if r is not None)
                error_budget["failed"] += sum(1 for r in extracted_data_list if r is None)

                batch_number += 1

        logger.info("Finished processing all URLs from queue.")
        logger.info(f"SerpAPI Usage Summary: Total Searches: {serpapi_usage['total_searches']}, Failed Searches: {serpapi_usage['failed_searches']}")
        logger.info(f"Error budget summary: {error_budget}")
        logger.info(f"Prometheus metrics available at http://localhost:8001/metrics")

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Crawler finished. Total time: {duration:.2f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())