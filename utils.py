# utils.py - FULL UPDATED CODE BASED ON SCHEMA propertyManagerContacts - WITH CITY FIX - FULL FILE REPLACEMENT
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import time
import datetime
import logging
import urllib.robotparser
from urllib.parse import urlparse, urljoin, urlunparse
import os
from twocaptcha import TwoCaptcha
from pydantic import ( # Updated imports - field_validator and ValidationInfo added
    BaseModel,
    EmailStr,
    HttpUrl,
    confloat,
    field_validator, # <-- field_validator is imported
    Field,
    ValidationInfo, # <-- ValidationInfo is imported (optional, but good practice)
    conlist,
    # Optional # <-- Optional is still listed here within pydantic imports for clarity, but the import is now from 'typing' # REMOVE Optional from pydantic import
)
from typing import List, Optional # <-- Correct import for Optional - IMPORT FROM TYPING MODULE
import ollama
from html import unescape
import random
import socket
from fake_useragent import UserAgent
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from urllib.parse import urlparse
import struct
from curl_cffi import requests
import threading
from functools import lru_cache
from prometheus_client import Summary, Counter, Gauge, REGISTRY, start_http_server
from curl_cffi.curl import CurlError
from collections import defaultdict
import async_timeout
from random import choices
from itertools import cycle
from aiohttp import ClientSession, TCPConnector, AsyncResolver
from async_lru import alru_cache
import redis
from time import sleep
from playwright.async_api import async_playwright

MAX_CAPTCHA_ATTEMPTS = 2 # Reduced CAPTCHA Retries - Point 9
MAX_PROXY_FAILURES = 2 # Proxy Failure Handling - Point 3
logger = logging.getLogger(__name__)

GENERIC_DOMAINS = GENERIC_DOMAINS = { # Domain Filter Adjustment - Point 5 - Refined Generic Domains
    "orbitz.com", "blogspot.com", "google.com", "facebook.com",
    "youtube.com", "twitter.com", "linkedin.com", "instagram.com", "pinterest.com",
    "yelp.com", "tripadvisor.com", "reddit.com", "wikipedia.org", "mediawiki.org",
    "amazon.com", "ebay.com", "craigslist.org", "indeed.com", "glassdoor.com",
    "zillow.com", "realtor.com", "apartments.com",
    "booking.com", "allpropertymanagement.com", "airdna.co", "airdna.com",
    "vacasa.com", "cozycozy.com", "kayak.com", "expedia.com", "news-outlet.com",
    "social_media.com", "evolve.com",
    "homes-and-villas.marriott.com", "hometogo.com", "www.hometogo.com",
    "whimstay.com", "www.whimstay.com", "avantstay.com", "houfy.com", "www.redawning.com",
    "corporatehousing.com", "www.corporatehousing.com", "https://www.onefinestay.com", "https://www.wimdu.com"
    "https://www.theblueground.com", "https://awning.com", "https://www.airconcierge.net", "fiscalnote.com",
    "news.com", "www.investopedia.com", "www.bankrate.com", "www.newyorkfed.org", "www.bloomberg.com",
    "https://hotpads.com", "www.zumper.com", "www.homes.com", "www.redfin.com", "www.apartmentfinder.com",
    "www.forrent.com", "http://www.airbtics.com", "www.furnishedfinder.com", "http://www.corporatehousingbyowner.com",
    "www.zumperrentals.com", "www.corporatehousingbyowner.com", "https://www.wimdu.com", "www.quora.com", "www.airconcierge.net",
    "www.staysophari.com", "www.windermere.com", "www.zipcar.com", "awning.com", "www.tesla.com","www.geekwire.com",
}


BAD_PATH_PATTERN = re.compile(r'/(category|tag/page)/', re.IGNORECASE)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
 ]

HEADERS_LIST = [ # Enhanced Headers List
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
        'TE': 'Trailers',
    },
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    },
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.3',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    },
    {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'TE': 'Trailers',
    },
]


RELEVANCE_KEYWORDS = [ # More comprehensive keywords for relevance
    "vacation rental", "vacation rentals", "short term rental", "short term rentals",
    "property management", "property manager", "rental management", "rental manager",
    "airbnb management", "vrbo management", "vacation home", "vacation homes",
    "holiday rental", "holiday rentals", "cabin rental", "cabin rentals",
    "condo rental", "condo rentals", "beach rental", "beach rentals",
    "rental property", "rental properties", "book a stay", "find rentals",
    "guest services", "property care", "manage my rental", "maximize rental income",
    "vacation lodging", "holiday accommodation", "short let", "furnished rentals",
    "corporate housing", "executive rentals", "serviced apartments",
    "rental services", "property hosting", "vacation planning", "travel accommodations",
    "holiday homes", "rental homes", "resort rentals", "villa rentals",
    "chalet rentals", "apartment rentals", "condo rentals", "beachfront rentals",
    "luxury vacation rentals", "family vacation rentals", "pet friendly rentals",
    "group vacation rentals", "extended stay rentals", "weekly rentals", "monthly rentals",
    "property marketing for rentals", "rental listing management", "dynamic pricing for rentals",
    "guest communication services", "cleaning services for rentals", "maintenance services for rentals",
    "property management company", "rental management agency", "vacation rental business",
    "short term rental company", "airbnb property management", "vrbo property management",
    "vacation rental management services", "short term rental management services",
    "rental income optimization", "increase rental bookings", "manage vacation rentals",
    "list your rental property", "advertise your vacation rental", "rental website",
    "vacation rental website", "short term rental platform", "book direct vacation rentals"
    # Add even more relevant keywords as you think of them!
]


CLIENT_TIMEOUT = 25 # Timeout Optimization - Point 2 - Reduced from 40

PROXY_HEALTH = {}
CAPTCHA_BALANCE = float(os.getenv("INITIAL_CAPTCHA_BALANCE", "10.0"))
CAPTCHA_COST = {
    'recaptcha_v2': 0.0025,
    'recaptcha_v3': 0.005,
    'hcaptcha': 0.0025 # ADDED hCaptcha cost - same as reCAPTCHA v2 for now
}
BRIGHTDATA_PORTS = [22225, 22226, 33335] # Updated BRIGHTDATA_PORTS - FALLBACK PORT ADDED
BRIGHTDATA_GEO = ["us-east", "us-west"]
BRIGHTDATA_USER = os.getenv("BRIGHTDATA_USER")
BRIGHTDATA_PASS = os.getenv("BRIGHTDATA_PASS") # ADDED BRIGHTDATA_PASS for proxy auth
IMPERSONATIONS = ["chrome120", "chrome119"]
captcha_lock = threading.Lock()

PROXY_SCORE_CONFIG = { # Updated PROXY_SCORE_CONFIG - LESS AGGRESSIVE SCORING
    'success': +30,    # Increased from +25
    'failure': -20,    # Reduced from -40
    'timeout': -15,    # Reduced from -30
    'min_score': 20,   # Increased from 15
    'initial': 70      # Increased from 50
}


def configure_metrics():
    metric_prefixes_to_cleanup = ['extraction_', 'serpapi_', 'captcha_', 'crawled_', 'avg_relevance_', 'http_status_', 'page_extraction_', 'proxy_', 'request_', 'extraction_success_', 'crawl_']
    for name in list(REGISTRY._names_to_collectors):
        if any(name.startswith(prefix) for prefix in metric_prefixes_to_cleanup):
            print(f"DEBUG: Unregistering metric with prefix: {name}")
            REGISTRY.unregister(REGISTRY._names_to_collectors[name])

    return {
        'serpapi_errors': Counter('serpapi_errors', 'SerpAPI Search Errors'),
        'crawled_pages': Counter('crawled_pages', 'Total pages crawled'),
        'extraction_failure': Counter('extraction_failure', 'Failed extractions'),
        'avg_relevance_score': Gauge('avg_relevance_score', 'Average Relevance Score'),
        'http_status_codes': Counter('http_status_codes', 'HTTP Status Codes', ['code']),
        'captcha_requests': Counter('captcha_requests', 'Number of CAPTCHA requests'),
        'captcha_solved': Counter('captcha_solved', 'Number of CAPTCHAs successfully solved'),
        'captcha_failed': Counter('captcha_failed', 'Number of CAPTCHA solving failures'),
        'page_extraction_errors': Counter('page_extraction_errors', 'Page Extraction Errors'),
        'proxy_errors': Counter('proxy_errors', 'Proxy Errors'),
        'request_errors': Counter('request_errors', 'Request Errors'),
        'extraction_success': Counter('extraction_success', 'Successful data extractions'),
        'crawl_errors': Counter('crawl_errors', 'Crawl errors', ['type', 'proxy']),
        'failed_urls': Counter('failed_urls', 'Number of URLs that failed to crawl completely') # Monitoring Additions - Point 9
    }


metrics = configure_metrics()
crawl_latency_summary = Summary('crawl_latency_seconds', 'Latency of page crawls')

PROXY_SCORES = defaultdict(lambda: PROXY_SCORE_CONFIG['initial']) # Updated PROXY_SCORES - INITIAL SCORE NOW FROM CONFIG
PROXY_ROTATION_LOCK = threading.Lock()
_session = None
proxy_cycle = None
PROXY_FAILURES = defaultdict(int) # Proxy Failure Handling - Point 3 - Track proxy failures

SSL_WHITELIST = ["brd.superproxy.io", "gate.smartproxy.com"] # ADDED SSL_WHITELIST

# --- Playwright Shared Instance ---
_playwright_instance = None
_browser = None

async def get_playwright_instance():
    global _playwright_instance
    if _playwright_instance is None:
        _playwright_instance = async_playwright()
        await _playwright_instance.__aenter__()
    return _playwright_instance

async def get_browser_instance(proxy=None):
    async with async_playwright() as p:
        proxy_args = {}
        if proxy: # Configure proxy if provided
            proxy_parts = proxy.split('@')
            if len(proxy_parts) == 2:
                auth_user_pass, proxy_host_port = proxy_parts
                auth = auth_user_pass.split(':')
                if len(auth) == 2:
                    proxy_user, proxy_pass = auth
                    proxy_args = {
                        "proxy": {
                            "server": f"http://{proxy_host_port}",
                            "username": proxy_user,
                            "password": proxy_pass
                        }
                    }
                else:
                     proxy_args = {"proxy": {"server": f"http://{proxy}"}} # No auth
            else:
                proxy_args = {"proxy": {"server": f"http://{proxy}"}} # No auth

        browser = await p.chromium.launch(**proxy_args)
        return browser


async def close_playwright():
    global _browser, _playwright_instance
    try:
        if _browser:
            await _browser.close()
        if _playwright_instance:
            try:
                await _playwright_instance.__aexit__(None, None, None)  # Proper context manager exit
            except AttributeError: # Fallback for environments where __aexit__ might not be directly callable
                await _playwright_instance.stop()
    except RuntimeError as e:
        if "Event loop is closed" in str(e):
            logger.warning("Event loop already closed during Playwright cleanup")
    finally:
        _browser = None
        _playwright_instance = None
        logger.info("Playwright closed.")
# --- End Playwright Shared Instance ---


async def get_session():
    global _session
    if not _session or _session.closed:
        ssl_context = False # Default SSL context - can be adjusted if needed globally
        _session = ClientSession(
            connector=TCPConnector(limit=75, resolver=AsyncResolver(), ssl=ssl_context), # Default SSL Context for Session
            timeout=aiohttp.ClientTimeout(total=30)
        )
        logger.debug("DEBUG: Created a new aiohttp ClientSession.") # ADDED DEBUG LOG
    return _session


async def close_session():
    global _session
    if _session and not _session.closed:
        await _session.close()
        logger.debug("DEBUG: Closed the aiohttp ClientSession.") # ADDED DEBUG LOG
    _session = None


def rotate_proxy(proxy_pool):
    if not proxy_pool:
        return None

    # Get proxies with scores above minimum threshold
    healthy_proxies = [p for p in proxy_pool if PROXY_SCORES.get(p, 0) > PROXY_SCORE_CONFIG['min_score']]

    if healthy_proxies:
        # Select proxy with highest score
        return max(healthy_proxies, key=lambda x: PROXY_SCORES[x])
    else:
        # Fallback to random selection from full pool
        return random.choice(proxy_pool)


def decay_proxy_scores():
    decay_factor = 0.95
    min_score = PROXY_SCORE_CONFIG['min_score'] # Use min_score from config
    for proxy in list(PROXY_SCORES.keys()):
        if PROXY_SCORES[proxy] > min_score:
            PROXY_SCORES[proxy] = int(PROXY_SCORES[proxy] * decay_factor)
            logger.debug(f"Decayed score for proxy {proxy}, new score: {PROXY_SCORES[proxy]}")
        if PROXY_SCORES[proxy] <= 0:
            logger.info(f"Proxy {proxy} score reached 0 or below. Consider removing or further validation.")
        if PROXY_SCORES[proxy] < min_score and PROXY_SCORES[proxy] > 0:
            PROXY_SCORES[proxy] = min_score


def normalize_url(url):
    parsed_url = urlparse(url)
    normalized_path = parsed_url.path.rstrip('/')
    if not normalized_path:
        normalized_path = '/'
    return urlunparse((
        parsed_url.scheme.lower(),
        parsed_url.netloc.lower(),
        normalized_path,
        parsed_url.params,
        parsed_url.query,
        parsed_url.fragment
    ))


async def crawl_and_extract_async(session, context, proxy_pool=None): # Timeout Optimization - Point 2 - Wrapped with timeout
    try:
        async with async_timeout.timeout(CLIENT_TIMEOUT): # Timeout Optimization - Point 2 - Reduced timeout
            url = context['url']
            city = context['city'] # City from context - this is what we want to use
            term = context['term']
            logger.debug(f"Entering crawl_and_extract_async for URL: {url}, City: {city}, Term: {term}") # DEBUG LOG - ENTRY POINT - **CONFIRM CITY HERE**
            extracted_data = PropertyManagerData(
                 city=city, # Assign city from context - DIRECT ASSIGNMENT HERE - **CONFIRM CITY HERE**
                 url=url,
                 searchKeywords=[term],
                 link=url, # Set link to be the same as url for now
                 thoughts=f"Search term: {term}" # Example: Set thoughts to search term info
            )
            start_time = time.time()

            current_proxy = None
            proxy_key = 'no_proxy'
            proxy_retries = 0
            use_proxy = os.getenv("USE_PROXY", "False").lower() == "true"

            if use_proxy and proxy_pool:
                current_proxy = rotate_proxy(proxy_pool)
                proxy_key = current_proxy
                if not current_proxy:
                    logger.warning("No proxy available initially, will try without proxy if proxy attempts fail.")

            parsed_url = urlparse(url)
            try_again_with_new_proxy = True
            e_aiohttp = None
            hcaptcha_element = None

            while try_again_with_new_proxy and use_proxy and current_proxy:
                try_again_with_new_proxy = False
                hcaptcha_element = None
                try:
                    headers = {'User-Agent': random.choice(USER_AGENTS)}
                    headers.update(random.choice(HEADERS_LIST))
                    headers['Referer'] = urlparse(url).netloc

                    ssl_context = False if any(d in current_proxy for d in SSL_WHITELIST) else None

                    if os.getenv("IMPERSONATE_CHROME", "false").lower() == "true":
                        impersonate_str = random.choice(IMPERSONATIONS)
                        logger.debug(f"Impersonating: {impersonate_str}")
                        resp = await session.get(url, headers=headers, proxy=f"http://{current_proxy}" if current_proxy else None, impersonate=impersonate_str, timeout=CLIENT_TIMEOUT, ssl=ssl_context)
                    else:
                        resp = await session.get(url, headers=headers, proxy=f"http://{current_proxy}" if current_proxy else None, timeout=CLIENT_TIMEOUT, ssl=ssl_context)

                    metrics['http_status_codes'].labels(code=resp.status).inc()

                    if resp.status in [403, 404, 429, 500, 503]:
                        logger.warning(f"HTTP error {resp.status} for {url} using proxy: {current_proxy if current_proxy else 'None'}")
                        metrics['crawl_errors'].labels(type=f'http_{resp.status}', proxy=proxy_key).inc()
                        update_proxy_score(proxy_key, False)

                        if resp.status == 403 and proxy_pool and proxy_retries < 2:
                            proxy_retries += 1
                            current_proxy = rotate_proxy(proxy_pool)
                            proxy_key = current_proxy
                            logger.info(f"Retrying {url} with a different proxy due to 403, attempt {proxy_retries + 1}...")
                            try_again_with_new_proxy = True
                            continue
                        else:
                            return None

                    if resp.status == 405:
                        logger.warning(f"HTTP 405 error for {url}, trying with allow_redirects=False")
                        metrics['crawl_errors'].labels(type='http_405', proxy=proxy_key).inc()
                        resp = await session.get(url, headers=headers, proxy=f"http://{current_proxy}" if current_proxy else None, allow_redirects=False, timeout=CLIENT_TIMEOUT, ssl=ssl_context)
                        if resp.status != 200:
                            logger.warning(f"HTTP 405 retry failed, status {resp.status}")
                            update_proxy_score(proxy_key, False)
                            return None

                    if resp.status == 503 and "amazonaws.com/captcha" in str(resp.url):
                        logger.warning(f"AWS CAPTCHA detected at {resp.url} for {url}")
                        metrics['captcha_requests'].inc()
                        metrics['captcha_failed'].inc()
                        update_proxy_score(proxy_key, False)
                        return None

                    html_content = await resp.text()
                    print("----- HTML Content (First 500 chars) - aiohttp -----")
                    print(html_content[:500])
                    print("----- Text Content (First 500 chars) - aiohttp -----")
                    soup = BeautifulSoup(html_content, 'lxml')
                    text_content = soup.get_text(separator=' ', strip=True).lower()
                    print(text_content[:500])

                    hcaptcha_element = soup.find('h-captcha')
                    if hcaptcha_element:
                        logger.warning(f"hCaptcha element initially detected by BeautifulSoup, but might be JS rendered. Proceeding with Playwright check for {url}.")
                    else:
                        logger.debug(f"hCaptcha not found in initial HTML, proceeding with BeautifulSoup parsing for {url}")
                        captcha_sitekey_v2 = re.search(r'data-sitekey="([^"]+)"', html_content)
                        captcha_sitekey_v3 = re.search(r'sitekey: ?"([^"]+)"', html_content)
                        captcha_sitekey_invisible = re.search(r'recaptcha\.render\("([^"]+)", {', html_content)

                        if captcha_sitekey_v2 and not hcaptcha_element:
                            captcha_sitekey = captcha_sitekey_v2.group(1)
                            logger.warning(f"ReCAPTCHA v2 detected on {url}, sitekey: {captcha_sitekey[:20]}...")
                            captcha_response = await solve_captcha(url, captcha_sitekey, 'recaptcha_v2', current_proxy)
                            if captcha_response:
                                metrics['captcha_solved'].inc()
                                logger.info(f"reCAPTCHA v2 solved, proceeding with request...")
                                captcha_params = {'g-recaptcha-response': captcha_response}
                                resp_after_captcha = await session.get(url, params=captcha_params, headers=headers, proxy=f"http://{current_proxy}" if current_proxy else None, timeout=CLIENT_TIMEOUT, ssl=ssl_context)
                                if resp_after_captcha.status == 200:
                                    html_content = await resp_after_captcha.text()
                                    soup = BeautifulSoup(html_content, 'lxml')
                                    text_content = soup.get_text(separator=' ', strip=True).lower()
                                else:
                                    logger.error(f"reCAPTCHA v2 solve failed to get 200 OK, status: {resp_after_captcha.status}")
                                    metrics['crawl_errors'].labels(type='captcha_failed_http', proxy=proxy_key).inc()
                                    update_proxy_score(proxy_key, False)
                                    return None
                            else:
                                logger.error(f"ReCAPTCHA v2 solving failed for {url}")
                                metrics['crawl_errors'].labels(type='captcha_unsolved_v2', proxy=proxy_key).inc()
                                update_proxy_score(proxy_key, False)
                                return None

                        elif captcha_sitekey_v3 and not hcaptcha_element:
                            captcha_sitekey = captcha_sitekey_v3.group(1)
                            logger.warning(f"ReCAPTCHA v3 detected on {url}, sitekey: {captcha_sitekey[:20]}...")
                            captcha_response = await solve_captcha(url, captcha_sitekey, 'recaptcha_v3', current_proxy)
                            if captcha_response:
                                metrics['captcha_solved'].inc()
                                logger.info(f"reCAPTCHA v3 solved (although v3 solving is skipped by default).")
                            else:
                                logger.warning(f"ReCAPTCHA v3 solving skipped or failed for {url}")
                                metrics['crawl_errors'].labels(type='captcha_skipped_v3', proxy=proxy_key).inc()
                                return None

                        elif captcha_sitekey_invisible and not hcaptcha_element:
                            captcha_sitekey = captcha_sitekey_invisible.group(1)
                            logger.warning(f"Invisible reCAPTCHA detected on {url}, element ID: {captcha_sitekey[:20]}...")
                            captcha_response = await solve_captcha(url, captcha_sitekey, 'recaptcha_v2', current_proxy)
                            if captcha_response:
                                metrics['captcha_solved'].inc()
                                logger.info(f"Invisible reCAPTCHA solved, proceeding with request...")
                                captcha_params = {'g-recaptcha-response': captcha_response}
                                resp_after_captcha = await session.get(url, params=captcha_params, headers=headers, proxy=f"http://{current_proxy}" if current_proxy else None, timeout=CLIENT_TIMEOUT, ssl=ssl_context)
                                if resp_after_captcha.status == 200:
                                    html_content = await resp_after_captcha.text()
                                    soup = BeautifulSoup(html_content, 'lxml')
                                    text_content = soup.get_text(separator=' ', strip=True).lower()
                                else:
                                    logger.error(f"Invisible reCAPTCHA solve failed to get 200 OK, status: {resp_after_captcha.status}")
                                    metrics['crawl_errors'].labels(type='captcha_failed_http_invisible', proxy=proxy_key).inc()
                                    update_proxy_score(proxy_key, False)
                                    return None
                            else:
                                logger.error(f"Invisible reCAPTCHA solving failed for {url}")
                                metrics['crawl_errors'].labels(type='captcha_unsolved_invisible', proxy=proxy_key).inc()
                                update_proxy_score(proxy_key, False)
                                return None

                        parsed_domain = urlparse(url).netloc.lower() # Domain Filter Adjustment - Point 5 - Check against ALLOWED_DOMAINS first
                        if (parsed_domain in GENERIC_DOMAINS or any(blocked in parsed_domain for blocked in GENERIC_DOMAINS)): # Domain Filter Adjustment - Point 5 - Refined domain check
                            logger.warning(f"Skipping generic domain URL: {url}")
                            metrics['crawl_errors'].labels(type='generic_domain', proxy=proxy_key).inc()
                            return None


                        if BAD_PATH_PATTERN.search(parsed_url.path):
                            logger.warning(f"Skipping URL due to bad path pattern: {url}")
                            metrics['crawl_errors'].labels(type='bad_path_pattern', proxy=proxy_key).inc()
                            return None



                        domain = urlparse(url).netloc.replace('www.', '')

                        emails = extract_emails(html_content, domain)
                        phones = extract_phone_numbers(text_content)
                        address = extract_address(soup)
                        extracted_name = extract_company_name(url)
                        if not extracted_name or extracted_name == "N/A":
                            extracted_name_element = soup.find("h1") or soup.find("title")
                            if extracted_name_element:
                                extracted_name = extracted_name_element.text.strip()
                            else:
                                extracted_name = "N/A"


                        address_text = extract_address(soup) # Address extraction - KEEP THIS if you still want to extract address info
                        extracted_city = "N/A" # Default if city extraction fails (though we are not using it now for database city) # --- KEEP DEFAULT VALUE ---


                        extracted_data.name = extracted_name
                        extracted_data.email = emails
                        extracted_data.phoneNumber = phones
                        extracted_data.city = city # DIRECTLY ASSIGN CITY FROM CONTEXT - NO MORE OVERWRITING - **CONFIRM CITY HERE**


                        logger.info(f"Data extracted successfully from {url} (direct scrape), name: {extracted_name}, emails: {emails}, phones: {phones}, city: {city}") # Log city from context - **CONFIRM CITY HERE**
                        metrics['extraction_success'].inc()
                        logger.debug(f"Exiting crawl_and_extract_async successfully for {url}, returning data: {extracted_data}") # DEBUG LOG - EXIT SUCCESS - **CONFIRM CITY HERE**

                        return extracted_data

                except aiohttp.ClientError as e:
                    logger.error(f"aiohttp ClientError during direct scraping for {url}: {e}")
                    metrics['request_errors'].inc()
                    metrics['crawl_errors'].labels(type='client_error', proxy='no_proxy').inc()
                    logger.debug(f"Exiting crawl_and_extract_async with aiohttp.ClientError for {url}, returning None") # DEBUG LOG - EXIT FAILURE - **CONFIRM CITY HERE (implicitly None)**
                    return None
                except asyncio.TimeoutError:
                    logger.error(f"Asyncio timeout during direct scraping for {url}")
                    metrics['request_errors'].inc()
                    metrics['crawl_errors'].labels(type='timeout_error', proxy='no_proxy').inc()
                    logger.debug(f"Exiting crawl_and_extract_async with asyncio.TimeoutError for {url}, returning None") # DEBUG LOG - EXIT FAILURE - **CONFIRM CITY HERE (implicitly None)**
                    return None
                except Exception as e_direct_crawl:
                    logger.exception(f"Unexpected error during direct scraping for {url}: {e_direct_crawl}")
                    metrics['page_extraction_errors'].inc()
                    metrics['crawl_errors'].labels(type='extraction_error', proxy='no_proxy').inc()
                    logger.debug(f"Exiting crawl_and_extract_async with unexpected error for {url}, returning None") # DEBUG LOG - EXIT FAILURE - **CONFIRM CITY HERE (implicitly None)**
                    return None

                return None


            # --- Fallback to Direct Scraping (No Proxy) ---
            if use_proxy and proxy_pool and not current_proxy:
                logger.warning(f"All proxies failed or none available. Attempting direct scraping without proxy for {url}")
            elif use_proxy and proxy_pool:
                logger.warning(f"Proxy attempts or Playwright failed for {url}. Falling back to direct scraping without proxy.")

            try: # Direct scraping attempt
                headers = {'User-Agent': random.choice(USER_AGENTS)}
                headers.update(random.choice(HEADERS_LIST))
                headers['Referer'] = urlparse(url).netloc

                resp = await session.get(url, headers=headers, timeout=CLIENT_TIMEOUT) # No proxy here

                metrics['http_status_codes'].labels(code=resp.status).inc()

                if resp.status in [403, 404, 429, 500, 503]: # Still handle HTTP errors even without proxy
                    logger.warning(f"HTTP error {resp.status} for {url} during direct scraping.")
                    metrics['crawl_errors'].labels(type=f'http_{resp.status}', proxy='no_proxy').inc() # Label as no_proxy
                    logger.debug(f"Exiting crawl_and_extract_async with HTTP error during direct scrape for {url}, returning None") # DEBUG LOG - EXIT FAILURE - **CONFIRM CITY HERE (implicitly None)**
                    return None # Stop if direct scraping also fails with these errors

                html_content = await resp.text()
                print("----- HTML Content (First 500 chars) - Direct Scraping -----")  # Debugging print
                print(html_content[:500])  # Print first 500 characters
                print("----- Text Content (First 500 chars) - Direct Scraping -----")  # Debugging print
                soup = BeautifulSoup(html_content, 'lxml')
                text_content = soup.get_text(separator=' ', strip=True).lower()
                print(text_content[:500])  # Print first 500 characters

                parsed_domain = urlparse(url).netloc.lower() # Domain Filter Adjustment - Point 5 - Check against ALLOWED_DOMAINS first - Direct scrape check
                if (parsed_domain in GENERIC_DOMAINS or any(blocked in parsed_domain for blocked in GENERIC_DOMAINS)): # Domain Filter Adjustment - Point 5 - Refined domain check - Direct scrape check
                    logger.warning(f"Skipping generic domain URL (direct scrape): {url}")
                    metrics['crawl_errors'].labels(type='generic_domain', proxy='no_proxy').inc()
                    logger.debug(f"Exiting crawl_and_extract_async due to generic domain (direct scrape) for {url}, returning None") # DEBUG LOG - EXIT FAILURE - **CONFIRM CITY HERE (implicitly None)**
                    return None

                emails = extract_emails(html_content, domain)
                phones = extract_phone_numbers(text_content)
                address = extract_address(soup)
                extracted_name = extract_company_name(url) # Or find name in HTML if possible
                if not extracted_name or extracted_name == "N/A":
                    extracted_name_element = soup.find("h1") or soup.find("title") # Example: Try to get name from h1 or title
                    if extracted_name_element:
                        extracted_name = extracted_name_element.text.strip()
                    else:
                        extracted_name = "N/A" # Default if name not found

                address_text = extract_address(soup) # Address extraction - KEEP THIS if you still want to extract address info

                extracted_city = "N/A"  # Initialize extracted_city to "N/A" - but not used for database city anymore


                extracted_data.name = extracted_name
                extracted_data.email = emails
                extracted_data.phoneNumber = phones
                extracted_data.city = city # DIRECTLY ASSIGN CITY FROM CONTEXT - NO MORE OVERWRITING - **CONFIRM CITY HERE**
                extracted_data.searchKeywords = [term] # Assign searchKeywords here


                logger.info(f"Data extracted successfully from {url} (direct scrape), name: {extracted_name}, emails: {emails}, phones: {phones}, city: {city}") # Log city from context - **CONFIRM CITY HERE**
                metrics['extraction_success'].inc()
                logger.debug(f"Exiting crawl_and_extract_async successfully (direct scrape) for {url}, returning data: {extracted_data}") # DEBUG LOG - EXIT SUCCESS - **CONFIRM CITY HERE**


                return extracted_data

            except aiohttp.ClientError as e:
                logger.error(f"aiohttp ClientError during direct scraping for {url}: {e}")
                metrics['request_errors'].inc()
                metrics['crawl_errors'].labels(type='client_error', proxy='no_proxy').inc() # Label as no_proxy
                logger.debug(f"Exiting crawl_and_extract_async with aiohttp.ClientError during direct scrape for {url}, returning None") # DEBUG LOG - EXIT FAILURE - **CONFIRM CITY HERE (implicitly None)**
                return None
            except asyncio.TimeoutError:
                logger.error(f"Asyncio timeout during direct scraping for {url}")
                metrics['request_errors'].inc()
                metrics['crawl_errors'].labels(type='timeout_error', proxy='no_proxy').inc() # Label as no_proxy
                logger.debug(f"Exiting crawl_and_extract_async with asyncio.TimeoutError during direct scrape for {url}, returning None") # DEBUG LOG - EXIT FAILURE - **CONFIRM CITY HERE (implicitly None)**
                return None
            except Exception as e_direct_crawl:
                logger.exception(f"Unexpected error during direct scraping for {url}: {e_direct_crawl}")
                metrics['page_extraction_errors'].inc()
                metrics['crawl_errors'].labels(type='extraction_error', proxy='no_proxy').inc() # Label as no_proxy
                logger.debug(f"Exiting crawl_and_extract_async with unexpected error during direct scrape for {url}, returning None") # DEBUG LOG - EXIT FAILURE - **CONFIRM CITY HERE (implicitly None)**
                return None
    except asyncio.TimeoutError: # Timeout Optimization - Point 2 - Outer timeout catch
        logger.warning(f"crawl_and_extract_async timed out after {CLIENT_TIMEOUT} seconds: {url}") # Timeout Optimization - Point 2
        metrics['crawl_errors'].labels(type='function_timeout', proxy='N/A').inc() # Timeout Optimization - Point 2
        metrics['failed_urls'].inc() # Error Recovery Flow - Point 7 - Increment failed_urls counter
        return None # Timeout Optimization - Point 2
    # --- End Fallback to Direct Scraping ---

    logger.debug(f"Exiting crawl_and_extract_async (all attempts failed) for {url}, returning None") # DEBUG LOG - EXIT FAILURE - **CONFIRM CITY HERE (implicitly None)**
    metrics['failed_urls'].inc() # Error Recovery Flow - Point 7 - Increment failed_urls counter if all attempts fail
    return None # Return None if both proxy and direct attempts fail



def get_proxy_pool():
    proxies = []
    provider_counts = {
        'oxylabs': 0,
        'smartproxy': 0,
        'brightdata': 0
    }

    # Force include Oxylabs if credentials exist
    if os.getenv('OXLABS_USER') and os.getenv('OXLABS_PASS'):
        proxies.append(f"{os.getenv('OXLABS_USER')}:{os.getenv('OXLABS_PASS')}@pr.oxylabs.io:7777")
        provider_counts['oxylabs'] += 1

    # Force include Smartproxy if credentials exist
    if os.getenv('SMARTPROXY_USER') and os.getenv('SMARTPROXY_PASS'):
        proxies.append(f"{os.getenv('SMARTPROXY_USER')}:{os.getenv('SMARTPROXY_PASS')}@gate.smartproxy.com:7000")
        provider_counts['smartproxy'] += 1

    if os.getenv('BRIGHTDATA_USER') and os.getenv('BRIGHTDATA_PASS'):
        for port in BRIGHTDATA_PORTS:
            proxy_string = f"{os.getenv('BRIGHTDATA_USER')}:{os.getenv('BRIGHTDATA_PASS')}@brd.superproxy.io:{port}"
            if "brd.superproxy.io" in proxy_string: # Apply Bright Data HTTP fix
                proxy_string = proxy_string.replace("https://", "http://") + "?type=http" # Force HTTP for Bright Data
            proxies.append(proxy_string)
            provider_counts['brightdata'] += 1

    logger.info(f"Proxy pool 구성: {provider_counts}")

    if not proxies: # Check if proxies list is empty after configuration
        raise ValueError("No proxy credentials found in environment variables")

    healthy_proxies = [proxy for proxy, score in PROXY_SCORES.items() if score > 20]
    if healthy_proxies:
        logger.debug(f"Using healthy proxies, count: {len(healthy_proxies)}")
        return healthy_proxies
    else:
        logger.warning("No healthy proxies found, using refreshed pool.")
        logger.debug("No healthy proxies found, using fresh pool (limited size).")
        return proxies # Return ALL proxies (not limited/weighted random choices anymore)


PROXY_REGEX = r'^(.+?:.+?@)?[\w\.-]+:\d+(\?.*)?$' # Modified regex to allow query parameters


def validate_proxy_config():
    if os.getenv("USE_PROXY", "False").lower() == "true":
        proxy_pool = get_proxy_pool()
        logger.debug(f"DEBUG: PROXY_POOL inside validate_proxy_config(): {proxy_pool}")
        if not proxy_pool:
            logger.warning("Invalid proxy configuration detected: PROXY_POOL is empty.")
            return False
        invalid_proxies = [proxy for proxy in proxy_pool if not re.match(PROXY_REGEX, proxy)]
        if invalid_proxies:
            logger.warning(f"Invalid proxy configuration detected: Proxies failed regex validation: {invalid_proxies}")
            return False
        for proxy_string in proxy_pool:
            try:
                auth_part, host_part = proxy_string.split('@')
                user_pass = auth_part.split(':')
                host_port = host_part.split(':')
                if len(user_pass) < 2 or len(host_port) < 2:
                    raise ValueError("Invalid proxy format")
            except (ValueError, AttributeError) as e:
                logger.error(f"Invalid proxy format: {proxy_string}. Required: user:pass@host:port")
                return False
        logger.info(f"PROXY_POOL configured with {len(proxy_pool)} proxies.")
        return True
    else:
        logger.info("Proxy usage disabled (USE_PROXY=false).")
        return True

def validate_captcha(solution): # CAPTCHA Handling Fixes - Point 4 - Validate CAPTCHA solutions
    if not re.match(r'^[\w-]{40,100}$', solution): # CAPTCHA Handling Fixes - Point 4 - Adjusted regex based on logs
        logger.error("Invalid CAPTCHA format") # CAPTCHA Handling Fixes - Point 4
        return False # CAPTCHA Handling Fixes - Point 4
    return True # CAPTCHA Handling Fixes - Point 4


async def solve_captcha(page_url, captcha_sitekey, captcha_type, current_proxy=None):
    global CAPTCHA_BALANCE
    if not os.getenv('TWOCAPTCHA_API_KEY'):
        logger.warning("TWOCAPTCHA_API_KEY not set, CAPTCHA solving disabled.")
        return None

    solver = TwoCaptcha(os.getenv('TWOCAPTCHA_API_KEY'))
    try:
        balance = await asyncio.to_thread(solver.balance)
        if balance < 0.5:
            logger.critical(f"2Captcha balance below $0.50 (${balance:.2f}). CAPTCHA solving disabled.")
            return None
        with captcha_lock:
            CAPTCHA_BALANCE = float(balance)
    except Exception as e:
        logger.error(f"2Captcha balance check failed: {e}")
        return None

    if CAPTCHA_BALANCE < 2:
        logger.error(f"2Captcha balance too low: ${CAPTCHA_BALANCE}. CAPTCHA solving disabled.")
        return None

    if CAPTCHA_BALANCE < 5:
        logger.warning(f"2Captcha balance critical: ${CAPTCHA_BALANCE:.2f}")

    if captcha_type == 'recaptcha_v3':
        logger.info("Skipping recaptcha_v3 to conserve credits")
        return None

    if CAPTCHA_BALANCE < CAPTCHA_COST[captcha_type] * 2:
        logger.warning("Low CAPTCHA balance buffer, skipping CAPTCHA solve to conserve credits.")
        return None

    kwargs = {}
    if current_proxy:
        kwargs["default_proxy"] = current_proxy
        logger.debug(f"CAPTCHA solving using proxy: {current_proxy}")

    max_attempts = MAX_CAPTCHA_ATTEMPTS # Reduced CAPTCHA Retries - Point 9 - Use MAX_CAPTCHA_ATTEMPTS

    for attempt in range(max_attempts):
        try:
            if captcha_type == 'recaptcha_v2':
                result = await asyncio.to_thread(
                    solver.recaptcha,
                    apiKey=os.getenv('TWOCAPTCHA_API_KEY'),
                    sitekey=captcha_sitekey,
                    url=page_url,
                    version='v2',
                    attempts=1,
                    **kwargs
                )
            elif captcha_type == 'recaptcha_v3':
                continue
            elif captcha_type == 'hcaptcha': # ADDED hCaptcha solving
                result = await asyncio.to_thread(
                    solver.hcaptcha,
                    apiKey=os.getenv('TWOCAPTCHA_API_KEY'),
                    sitekey=captcha_sitekey,
                    url=page_url,
                    attempts=1,
                    **kwargs
                )
            else:
                logger.warning(f"Unknown CAPTCHA type '{captcha_type}', defaulting to reCAPTCHA v2 solver.")
                result = await asyncio.to_thread(
                    solver.recaptcha,
                    apiKey=os.getenv('TWOCAPTCHA_API_KEY'),
                    sitekey=captcha_sitekey,
                    url=page_url,
                    version='v2',
                    attempts=1,
                    **kwargs
                )

            if result and 'code' in result:
                if validate_captcha(result['code']): # CAPTCHA Handling Fixes - Point 4 - Validate CAPTCHA solution
                    logger.info(f"2Captcha solved successfully for {page_url} (Type: {captcha_type})...")
                    cost = CAPTCHA_COST.get(captcha_type, 0.003) # Default cost if type not found (shouldn't happen)
                    if captcha_type in CAPTCHA_COST:
                        cost = CAPTCHA_COST[captcha_type] # Use specific cost if available
                    with captcha_lock:
                        CAPTCHA_BALANCE -= cost
                        logger.info(f"2Captcha balance updated, cost: ${cost:.4f}, remaining balance: ${CAPTCHA_BALANCE:.2f}")
                    return result['code']
                else: # CAPTCHA Handling Fixes - Point 4 - If invalid solution
                    logger.error(f"Invalid CAPTCHA solution received, attempt {attempt + 1}/{max_attempts}") # CAPTCHA Handling Fixes - Point 4
                    if attempt + 1 >= max_attempts:
                        break # CAPTCHA Handling Fixes - Point 4
            else:
                logger.error(f"2Captcha solve failed, no code returned (Type: {captcha_type}), attempt {attempt + 1}/{max_attempts}")
                if attempt + 1 >= max_attempts:
                    break
        except Exception as e:
            logger.error(f"Error during CAPTCHA solving for {page_url} (attempt {attempt + 1}/{max_attempts}): {e}")
            if attempt + 1 >= max_attempts:
                break

    return None


async def enhanced_data_parsing(soup, text_content):
    # No enhanced data parsing needed for the new schema fields in this basic example
    return {}


class PropertyManagerData(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = "N/A"  # Property Manager Name
    email: List[EmailStr] = Field(default_factory=list)  # List of emails
    phoneNumber: List[str] = Field(default_factory=list)  # List of phone numbers
    city: Optional[str] = "N/A" # City -  Now represents City from CITIES list
    url: Optional[HttpUrl] = None  # Website URL
    searchKeywords: List[str] = Field(default_factory=list) # Search Keywords - from SEARCH_TERMS
    latLngPoint: Optional[str] = None  # latLngPoint - Keep if you intend to use it later
    lastEmailSentAt: Optional[datetime.datetime] = None # lastEmailSentAt - Keep if you intend to use it later
    thoughts: Optional[str] = None # Re-add thoughts field if you intend to use LLM analysis

    @field_validator('phoneNumber')
    def validate_phone_number(cls, values: List[str]) -> List[str]:
        return [phone.strip() for phone in values]


async def analyze_with_ollama(company_name, website_text, model_name='deepseek-r1:latest'):
    # Ollama analysis is not used in this schema-matching version for direct fields
    return None


async def analyze_batch(urls, session):
    # Batch LLM analysis is not used in this schema-matching version for direct fields
    return ["LLM analysis skipped"] * len(urls)


async def fetch_text(url, timeout, session):
    try:
        session = await get_session()
        async with async_timeout.timeout(timeout):
            async with session.get(url, timeout=timeout) as response:
                return await response.text()
    except Exception as e:
        logger.warning(f"Error fetching text content from {url} for batch LLM: {e}")
        return None

def update_proxy_score(proxy, success):
     if proxy: # Check if proxy is not None before accessing PROXY_SCORES
        if success:
            PROXY_SCORES[proxy] = min(100, PROXY_SCORES[proxy] + PROXY_SCORE_CONFIG['success']) # Use success score from config
            PROXY_FAILURES[proxy] = 0 # Proxy Failure Handling - Point 3 - Reset failure count on success
        else:
            PROXY_SCORES[proxy] = max(0, PROXY_SCORES[proxy] + PROXY_SCORE_CONFIG['failure']) # Use failure score from config
            PROXY_FAILURES[proxy] += 1 # Proxy Failure Handling - Point 3 - Increment failure count
            if PROXY_FAILURES[proxy] >= MAX_PROXY_FAILURES: # Proxy Failure Handling - Point 3 - Check if limit reached
                PROXY_SCORES[proxy] = -1000 # Proxy Failure Handling - Point 3 - Mark proxy as bad
                logger.warning(f"Proxy {proxy} marked as bad due to excessive failures.") # Proxy Failure Handling - Point 3

        logger.debug(f"Updated score for proxy {proxy}, new score: {PROXY_SCORES[proxy]}, failures: {PROXY_FAILURES[proxy]}") # Proxy Failure Handling - Point 3


        if PROXY_SCORES[proxy] <= PROXY_SCORE_CONFIG['min_score']: # Use min_score from config
            logger.warning(f"Proxy {proxy} score is low ({PROXY_SCORES[proxy]}). May need replacement.")
        if PROXY_SCORES[proxy] <= 0:
            logger.error(f"Proxy {proxy} score reached 0. Proxy is considered bad.")


def extract_company_name(url):
    try:
        domain = urlparse(url).netloc.replace('www.', '')
        company_name = domain.split('.')[0].capitalize()
        return company_name
    except:
        return None

def decode_emails(text): # (Already in your older code - reuse it)
    text = re.sub(r'\b(\w+)\s*\[?@\(?AT\)?\]?\s*(\w+)\s*\[?\.\(?DOT\)?\]?\s*(\w+)\b',
                r'\1@\2.\3', text, flags=re.IGNORECASE)
    text = re.sub(r'\b(\w+)\s*\(\s*at\s*\)\s*(\w+)\s*\(\s*dot\s*\)\s*(\w+)\b',
                r'\1@\2.\3', text, flags=re.IGNORECASE)
    text = unescape(text)
    return text

def extract_emails(html_content, domain):
    decoded_html_content = decode_emails(html_content)
    email_matches = list(set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})*", decoded_html_content, re.IGNORECASE)))
    print("----- Raw Email Matches (Refined Regex + Decoding) -----")
    print(email_matches)
    logger.debug(f"Extracted email matches (raw): {email_matches}") # DEBUG LOG - RAW EMAILS

    valid_emails = [email for email in email_matches if '@' in email and '.' in email]
    print("----- Validated Email Matches -----")
    print(valid_emails)
    logger.debug(f"Validated email matches: {valid_emails}") # DEBUG LOG - VALIDATED EMAILS

    return valid_emails


def extract_phone_numbers(text_content):
    # Reverted to older, verbose phone regex for testing
    phone_matches = list(set(re.findall(r'''
        (?x)                                  # Enable verbose regex mode
        (?:tel:\+?)?                           # Optional "tel:+ " or "tel:" prefix (for tel links)
        (?P<country_code>\d{1,3})?              # Optional country code (1-3 digits) - capture group 'country_code'
        [-.\s]?                                # Optional separator after country code
        \(?                                    # Optional opening parenthesis
        (?P<area_code>\d{3})                    # Area code (3 digits) - capture group 'area_code'
        \)?                                    # Optional closing parenthesis
        [-.\s]?                                # Separator (hyphen, dot, or space) - optional
        (?P<exchange_code>\d{3})                # Exchange code (3 digits) - capture group 'exchange_code'
        [-.\s]?                                # Separator - optional
        (?P<line_number>\d{4})                 # Line number (4 digits) - capture group 'line_number'
        (?:ext\.?\s*(?P<extension>\d+))?       # Optional extension (e.g., ext. 123) - capture group 'extension'
        \b                                    # Word boundary
        ''', text_content, re.VERBOSE | re.IGNORECASE))) # Use text_content here
    print("----- Raw Phone Matches (Older Verbose Regex) -----") # Debugging print
    print(phone_matches)
    logger.debug(f"Extracted phone matches (raw): {phone_matches}") # DEBUG LOG - RAW PHONES
    # Reconstruct phone numbers as strings
    phone_strings = []
    for match in phone_matches:
        country_code, area_code, exchange_code, line_number, extension = match
        phone_number = ""
        if country_code:
            phone_number += f"+{country_code} "
        phone_number += f"({area_code}) {exchange_code}-{line_number}"
        if extension:
            phone_number += f" ext. {extension}"
        phone_strings.append(phone_number.strip()) # Add reconstructed number as string
    logger.debug(f"Validated phone numbers: {phone_strings}") # DEBUG LOG - VALIDATED PHONES
    return phone_strings


def extract_address(soup):
    address_parts = []
    address_selectors = [
        'address', # Highest priority - <address> tag
        './/div[contains(@class, "address")]',
        './/p[contains(@class, "address")]',
        './/span[contains(@class, "address")]',
        './/div[contains(@id, "contact-info")]', # More specific IDs/Classes
        './/div[contains(@id, "contact-details")]',
        './/section[contains(@class, "contact")]', # More section based selectors
        './/footer', # Fallback to footer if address is commonly in there
        './/div[contains(@class, "contact-info")]', # Broader selectors (already present)
        './/div[contains(@class, "contact-details")]',
        'body' # Even broader fallback if needed - be cautious as body is very broad
    ]

    for selector in address_selectors:
        elements = soup.find_all(selector)
        for element in elements:
            text = element.get_text(separator=' ', strip=True)
            if not text:
                continue

            # Improved filtering for address lines - more address keywords and zip code patterns
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            potential_address_lines = [
                line for line in lines
                if re.search(
                    r'\d{3,}.*(Street|Ave|Road|P\.O\.\s?Box|Place|Square|Drive|Ln|Blvd|Court|Way|Circle)\b', line, re.IGNORECASE
                ) or re.search(
                    r'\b(City|Zip|Postal)\b', line, re.IGNORECASE # Removed State from here
                ) or re.search(
                    r'\b\d{5}(-\d{4})?\b', line # US Zip code pattern (5 digits or 5+4)
                ) or re.search(
                    r'\b[A-Z]{1,2}\d{1,3}[A-Z]{1,2}\s*\d[A-Z]{2}\b', line # UK Postcode Pattern (Example: EC1A 1BB) - Add more patterns if needed for other countries
                )
            ]

            if potential_address_lines:
                combined_address = " ".join(potential_address_lines)
                if len(combined_address) > 20: # Basic length filter to avoid short non-address texts
                    address_parts.append(combined_address)

    if address_parts:
        return " ".join(address_parts[:2]) # Limit to first two likely address strings
    return "N/A"


def calculate_relevance(text_content, term): # Modified calculate_relevance
    # Relevance calculation is not used for the schema-matching version focusing on direct fields
    return 0.0


def extract_license_numbers(text_content):
    license_patterns = [
        r'\b(?:LIC|License|LIC#)\s*[:#]?\s*([A-Za-z0-9-]+)\b', # e.g., LIC-12345, License #ABC-567
        r'\b([A-Za-z]{2,3}-\d{6,8})\b', # e.g., CA-1234567, FL-987654
        r'\b(?:License Number)\s*[:]?\s*([A-Za-z0-9-]+)\b' # e.g License Number: 12345
    ]
    extracted_licenses = []
    for pattern in license_patterns:
        found_licenses = re.findall(pattern, text_content, re.IGNORECASE)
        extracted_licenses.extend(found_licenses)
    return list(set(extracted_licenses))


class RateLimiter:
    def __init__(self, requests_per_second):
        self.rate = requests_per_second
        self.tokens = self.rate * 2  # Allow burst up to 2x capacity # Rate Limiting Adjustment
        self.last_refill = time.monotonic()
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            self.refill_tokens()
            if self.tokens >= 1:
                self.tokens -= 1
            else:
                await asyncio.sleep(self.get_delay())
                self.refill_tokens()
                self.tokens -= 1

    def refill_tokens(self):
        now = time.monotonic()
        time_elapsed = now - self.last_refill
        tokens_to_add = time_elapsed * self.rate
        self.tokens = min(self.rate * 2, self.tokens + tokens_to_add) #  tokens don't exceed rate * 2 # Rate Limiting Adjustment
        self.last_refill = now

    def get_delay(self):
        if self.tokens < 1:
            time_needed = 1 / self.rate # Time to get 1 token
            elapsed_since_refill = time.monotonic() - self.last_refill
            return max(0, time_needed - elapsed_since_refill)
        return 0


def is_valid_url(url_string):
    try:
        result = urlparse(url_string)
        return all([result.scheme, result.netloc]) # Check for scheme and netloc
    except: # ADDED Error handling for URL parsing - Fixes UnboundLocalError
        return False

# --- ADD validate_request FUNCTION HERE ---
def validate_request(url):
    """Basic validation to reject obviously invalid or unwanted URLs."""
    if not is_valid_url(url): # Use is_valid_url here
        logger.warning(f"Rejected as invalid URL: {url}")
        return False
    try: # ADDED try-except block - Error Handling Fix
        parsed_url = urlparse(url) # URL parsing moved inside try block
        if BAD_PATH_PATTERN.search(parsed_url.path):
            logger.warning(f"Rejected bad path: {url}")
            return None
    except Exception as e: # Catch URL parsing errors
        logger.error(f"URL parsing error: {str(e)}")
        return False

    # --- Improved Generic Domain Check ---
    parsed_domain = urlparse(url).netloc.lower() # Domain Filter Adjustment - Point 5 - Use parsed_domain
    if (parsed_domain in GENERIC_DOMAINS or any(blocked in parsed_domain for blocked in GENERIC_DOMAINS)): # Domain Filter Adjustment - Point 5 - Refined domain check
        logger.warning(f"Rejected due to generic domain: {url}")
        return False
    # --- End Improved Generic Domain Check ---

    return True


async def proxy_health_check(proxy_pool):
    """Check proxy health by testing connection to httpbin.org/ip""" # Updated test URL
    while True:
        try:
            test_url = "https://httpbin.org/ip" # Updated test URL
            session = await get_session()
            for proxy in proxy_pool:
                try:
                    async with session.get(test_url, proxy=f"http://{proxy}", timeout=15) as response:
                        if response.status == 200:
                            PROXY_SCORES[proxy] = min(100, PROXY_SCORES.get(proxy, 0) + PROXY_SCORE_CONFIG['success']) # Use success score from config
                            PROXY_FAILURES[proxy] = 0 # Proxy Failure Handling - Point 3 - Reset failure count on health check success
                        else:
                            PROXY_SCORES[proxy] = max(0, PROXY_SCORES.get(proxy, 0) + PROXY_SCORE_CONFIG['failure']) # Use failure score from config
                            PROXY_FAILURES[proxy] += 1 # Proxy Failure Handling - Point 3 - Increment failure count on health check failure
                            if PROXY_FAILURES[proxy] >= MAX_PROXY_FAILURES: # Proxy Failure Handling - Point 3 - Check if limit reached
                                PROXY_SCORES[proxy] = -1000 # Proxy Failure Handling - Point 3 - Mark proxy as bad on health check failure
                                logger.warning(f"Proxy {proxy} marked as bad due to excessive failures during health check.") # Proxy Failure Handling - Point 3
                        # No score update if exception during health check - handled in update_proxy_score
                except Exception as e:
                   logger.warning(f"Proxy health check failed for {proxy}: {e}") # Log individual proxy health check failures
                   update_proxy_score(proxy, False) # Penalize proxy on health check failure - still penalize even on exception
             # await session.close() # Let session pool manage closing - handled by session manager
            await asyncio.sleep(3600)  # Check every 1 hour - reduced frequency for proxy health check
        except Exception as e:
            logger.error(f"Proxy health check loop error: {str(e)}")
            await asyncio.sleep(600) # Reduced frequency of health check loop error logging