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
from supabase import create_client
import utils
from prometheus_client import start_http_server, Counter, Gauge
import random
import socket
import struct
from fake_useragent import UserAgent
from tenacity import retry, wait_exponential, stop_after_attempt
import urllib.robotparser
from urllib.parse import urljoin

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
start_http_server(8000)

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


async def crawl_and_extract_async(session, context, proxy_pool): # Modified to accept proxy_pool
    print("DEBUG: Entering crawl_and_extract_async for url:", context.get('url'))
    url = context["url"]
    logger.info(f"Crawling {url} for {context['city']} - {context['term']}")
    crawled_pages_metric.inc()

    # Proxy selection is now handled in fetch_url, just pass the proxy_pool
    try:
        # Pass proxy_pool to extract_contact_info
        data = await utils.extract_contact_info(session, url, context, proxy_pool=proxy_pool)
        print("DEBUG: Data from utils.extract_contact_info:", data)
        if data:
            logger.debug(f"Successfully extracted data from: {url} ") # Proxy info now in fetch_url logs
            extraction_success_metric.inc()
            relevance_score_gauge.inc(data.get("Relevance Score", 0))
            return data
        else:
            extraction_failure_metric.inc()
            logger.warning(f"Extraction failed for {url} (extract_contact_info returned None).") # Proxy info now in fetch_url logs
            return None
    except Exception as e:
        extraction_failure_metric.inc()
        logger.exception(f"Exception during async crawl/extraction for {url}: {e}") # Proxy info now in fetch_url logs
        print(f"DEBUG: Exception in crawl_and_extract_async: {e}")
        return None


async def process_urls_async(url_contexts, proxy_pool): # Modified to accept proxy_pool
    print("DEBUG: Entering process_urls_async with url_contexts:", url_contexts)

    extracted_data_list = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async def controlled_crawl(context):
        async with semaphore:
            await asyncio.sleep(random.uniform(2, 5))
            async with aiohttp.ClientSession() as session:
                try:
                    # Pass proxy_pool to crawl_and_extract_async
                    data = await crawl_and_extract_async(session, context, proxy_pool=proxy_pool)
                    print(f"DEBUG (controlled_crawl): Data extracted from {context['url']}: {data}")
                    if data:
                        db_saved = save_to_supabase(supabase_client, data)
                        if not db_saved:
                            save_to_csv([data], context.get("batch_number", 0))
                        return data
                    print(f"DEBUG (controlled_crawl): Extraction failed for {context['url']}")
                    return None
                except Exception as e_controlled_crawl:
                    logger.exception(f"Exception in controlled_crawl for {context.get('url')}: {e_controlled_crawl}")
                    return None

    try:
        tasks = [controlled_crawl(context) for context in url_contexts]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        print(f"DEBUG (process_urls_async): Results from asyncio.gather: {results}")
        extracted_data_list = [result for result in results if result is not None] # Filter out None results
        print("DEBUG: Exiting process_urls_async, returning:", extracted_data_list)
        return extracted_data_list
    except Exception as e_process_urls:
        logger.exception(f"Exception in process_urls_async: {e_process_urls}")
        return None


async def get_google_search_results(session, city, term):
    logger.debug(f"Entering get_google_search_results for city={city}, term={term}")
    all_urls = []
    for page_num in range(1, PAGES_PER_QUERY + 1):
        params = {
            "api_key": SERPAPI_API_KEY,
            "engine": "google",
            "q": f"{city} {term}",
            "gl": "us",
            "hl": "en", # Added language parameter
            "num": 20, # Increased num to 20
            "start": (page_num - 1) * 10,
            "async": True
        }
        try:
            search = GoogleSearch(params)
            initial_search_result = await asyncio.to_thread(search.get_dict)
            if initial_search_result and 'organic_results' in initial_search_result:
                for result in initial_search_result['organic_results']:
                    url = result.get('link')
                    if url and utils.is_valid_url(url):
                        all_urls.append(url)
            else:
                logger.warning(f"No organic results found in SerpAPI response for {city} {term} page {page_num}")
        except Exception as e:
            logger.error(f"SerpAPI search failed for {city} {term} page {page_num}: {e}. Response: {initial_search_result}") #Improved error log with response
            serpapi_usage["failed_searches"] += 1
            await asyncio.sleep(5)
            continue
        serpapi_usage["total_searches"] += 1

    return {"city": city, "term": term, "urls": all_urls}


def save_to_supabase(supabase_client, data):
    """Saves extracted data to Supabase database (now synchronous)."""
    try:
        # Add rigorous duplicate check
        existing = supabase_client.table('property_managers') \
            .select('website_url') \
            .eq('website_url', str(data['website_url'])) \
            .execute()

        if existing.data:
            logger.warning(f"Duplicate URL skipped: {data['website_url']}")
            return True  # Considered successful skip

        data_for_supabase = {
            'social_media': json.dumps(data.get('social_media', [])), # Convert social_media to JSON string
            'website_url': str(data['website_url']),  # Convert HttpUrl to string
            'city': data.get('City', 'N/A'),
            'search_term': data.get('Search Term', 'N/A'),
            'company_name': data.get('Company Name', 'N/A'),
            'email_addresses': data.get('Email Addresses', []),
            'phone_numbers': data.get('Phone Numbers', []),
            'physical_address': data.get('Physical Address', 'N/A'),
            'relevance_score': data.get('Relevance Score', 0.0),
            'estimated_properties': data.get('estimated_properties', 0),
            'service_areas': data.get('service_areas', []),
            'management_software': data.get('management_software', []),
            'license_numbers': data.get('license_numbers', []),
            'llm_category': data.get('llm_category', None)
        }

        response = supabase_client.table('property_managers').insert(data_for_supabase).execute()

        print(f"DEBUG (save_to_supabase): Full Supabase response object: {response}")

        # --- CORRECTED ERROR CHECKING ---
        if response.get('error'):  # Use response.get('error') - CORRECT WAY
            logger.error(f"Supabase insert error for {data.get('website_url')}: {response.get('error')}")
            if hasattr(response.error, 'message'): # Keep this detailed error logging
                logger.error(f"Supabase error details: {response.error.message}")
            logger.debug(f"Full Supabase response (error): {response.__dict__}")
            return False
        elif not response or not response.data: # Check for empty response.data too
            logger.error(f"Supabase insert failed for {data.get('website_url')}: No data returned, possible issue.")
            logger.warning(f"Full Supabase response (no data): {response}")
            return False
        else:
            logger.debug(f"Data saved to Supabase for: {data.get('website_url')}")
            return True
        # --- END CORRECTED ERROR CHECKING ---

    except Exception as e_supabase:
        logger.exception(f"Error saving to Supabase for {data.get('website_url')}: {e_supabase}")
        return False


def save_to_csv(data_list, batch_number):
    csv_filename = f"property_managers_data_batch_{batch_number}.csv"
    logger.info(f"Saving batch {batch_number} data to CSV file: {csv_filename} (fallback)")

    fieldnames = [
        "City", "Search Term", "Company Name", "website_url", "Email Addresses",
        "Phone Numbers", "Physical Address", "Relevance Score", "Timestamp",
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
                "City": data_item.get("City", "N/A"),
                "Search Term": data_item.get("Search Term", "N/A"),
                "Company Name": data_item.get("Company Name", "N/A"),
                "website_url": data_item.get("website_url", "N/A"),
                "Email Addresses": ", ".join(data_item.get("Email Addresses", [])),
                "Phone Numbers": ", ".join(data_item.get("Phone Numbers", [])),
                "Physical Address": data_item.get("Physical Address", "N/A"),
                "Relevance Score": data_item.get("Relevance Score", 0.0),
                "Timestamp": datetime.now().isoformat(),
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

    error_budget["total"] = len(CITIES) * len(SEARCH_TERMS) * PAGES_PER_QUERY

    # Load PROXY_POOL at the start of main() - assuming it's defined in utils.py
    proxy_pool_to_use = utils.PROXY_POOL if os.getenv("USE_PROXY", "False").lower() == "true" else None


    async with aiohttp.ClientSession() as session:
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
                batch_size_crawl = 20
                for _ in range(min(batch_size_crawl, len(url_contexts_queue))):
                    url_contexts_batch.append(url_contexts_queue.pop(0))

                db['url_contexts'] = url_contexts_queue

                print("DEBUG: url_contexts_batch before process_urls_async:", url_contexts_batch)
                # Pass proxy_pool to process_urls_async
                extracted_data_list = await process_urls_async(url_contexts_batch, proxy_pool=proxy_pool_to_use)
                print(f"DEBUG: Type of extracted_data_list: {type(extracted_data_list)}")

                logger.info(f"Extracted data from {len(extracted_data_list)} websites in batch {batch_number} from queue.")
                error_budget["success"] += len(extracted_data_list) #Correct error budget update
                error_budget["failed"] += len(url_contexts_batch) - len(extracted_data_list) #Correct error budget update


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