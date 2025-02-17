# utils.py - Corrected version (FINAL - ALL CHANGES IMPLEMENTED - LATEST VERSION - ERROR FIXES AND OPTIMIZATIONS - ASYNC SESSION & BATCH FIXES - BATCH LLM & FINAL TUNING - PRODUCTION READY UPDATES - HTTP2 REMOVED & PROXY VALIDATION & ROTATION - AIOHTTP VERSION FIX)
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import time
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
    ValidationInfo # <-- ValidationInfo is imported (optional, but good practice)
)
from typing import List, Optional
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


MAX_CAPTCHA_ATTEMPTS = 3
logger = logging.getLogger(__name__)

GENERIC_DOMAINS = GENERIC_DOMAINS = {
    "example.com", "orbitz.com", "blogspot.com", "google.com", "facebook.com",
    "youtube.com", "twitter.com", "linkedin.com", "instagram.com", "pinterest.com",
    "yelp.com", "tripadvisor.com", "tripadvisor.com", "reddit.com", "wikipedia.org", "mediawiki.org",
    "amazon.com", "ebay.com", "craigslist.org", "indeed.com", "glassdoor.com",
    "zillow.com", "realtor.com", "apartments.com", "airbnb.com", "vrbo.com",
    "booking.com", "allpropertymanagement.com", "airdna.co", "airdna.com",
    "vacasa.com", "cozycozy.com", "kayak.com", "expedia.com", "news-outlet.com",
    "social_media.com", "tripadvisor.com", "evolve.com",
    "homes-and-villas.marriott.com", "hometogo.com", "www.hometogo.com",
    "whimstay.com", "www.whimstay.com", "avantstay.com", "houfy.com", "www.redawning.com",
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


CLIENT_TIMEOUT = 40

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
IMPERSONATIONS = ["chrome120", "chrome119"]
captcha_lock = threading.Lock()

PROXY_SCORE_CONFIG = { # Updated PROXY_SCORE_CONFIG - LESS AGGRESSIVE SCORING
    'success': +25,
    'failure': -40,
    'timeout': -30,
    'min_score': 15,  # Previous: 20 - REDUCED min_score
    'initial': 50     # Previous: 70 - REDUCED initial score
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
    }


metrics = configure_metrics()
crawl_latency_summary = Summary('crawl_latency_seconds', 'Latency of page crawls')

PROXY_SCORES = defaultdict(lambda: PROXY_SCORE_CONFIG['initial']) # Updated PROXY_SCORES - INITIAL SCORE NOW FROM CONFIG
PROXY_ROTATION_LOCK = threading.Lock()
_session = None
proxy_cycle = None

SSL_WHITELIST = ["brd.superproxy.io", "gate.smartproxy.com"] # ADDED SSL_WHITELIST


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


async def crawl_and_extract_async(session, context, proxy_pool=None):
    url = context['url']
    city = context['city']
    term = context['term']
    company_name_from_url = extract_company_name(url)
    extracted_data = PropertyManagerData(
         city=city,
         search_term=term,
         website_url=url,
         company_name=company_name_from_url if company_name_from_url else "N/A"
    )
    start_time = time.time()

    current_proxy = None
    proxy_key = 'no_proxy'
    proxy_retries = 0 # Initialize proxy retry counter
    use_proxy = os.getenv("USE_PROXY", "False").lower() == "true" # Get proxy usage setting

    if use_proxy and proxy_pool:
        current_proxy = rotate_proxy(proxy_pool)
        proxy_key = current_proxy
        if not current_proxy:
            logger.warning("No proxy available initially, will try without proxy if proxy attempts fail.")

    parsed_url = urlparse(url)
    try_again_with_new_proxy = True # Control retry logic

    while try_again_with_new_proxy and use_proxy and current_proxy: # Proxy retry loop (only if proxy is enabled and available)
        try_again_with_new_proxy = False # Reset flag for each attempt
        try:
            headers = {'User-Agent': random.choice(USER_AGENTS)}
            headers.update(random.choice(HEADERS_LIST)) # Add enhanced headers
            headers['Referer'] = urlparse(url).netloc # Add Referer header

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

                if resp.status == 403 and proxy_pool and proxy_retries < 2: # Retry 403 with new proxy
                    proxy_retries += 1
                    current_proxy = rotate_proxy(proxy_pool) # Rotate proxy
                    proxy_key = current_proxy
                    logger.info(f"Retrying {url} with a different proxy due to 403, attempt {proxy_retries + 1}...")
                    try_again_with_new_proxy = True # Set flag to retry
                    continue # Retry from the beginning of the while loop
                else:
                    return None # If not 403 or max retries reached, return None

            if resp.status == 405:
                logger.warning(f"HTTP 405 error for {url}, trying with allow_redirects=False")
                metrics['crawl_errors'].labels(type='http_405', proxy=proxy_key).inc()
                resp = await session.get(url, headers=headers, proxy=f"http://{current_proxy}" if current_proxy else None, allow_redirects=False, timeout=CLIENT_TIMEOUT, ssl=ssl_context) # Pass ssl_context for retry
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
            print("----- HTML Content (First 500 chars) -----")  # Debugging print
            print(html_content[:500])  # Print first 500 characters
            print("----- Text Content (First 500 chars) -----")  # Debugging print
            soup = BeautifulSoup(html_content, 'lxml')
            text_content = soup.get_text(separator=' ', strip=True).lower()
            print(text_content[:500])  # Print first 500 characters

            # --- hCaptcha Detection and Solving ---
            hcaptcha_element = soup.find('h-captcha') # Look for h-captcha tag
            if hcaptcha_element:
                captcha_sitekey_hcaptcha = hcaptcha_element.get('data-sitekey') # Extract sitekey from data-sitekey attribute
                if captcha_sitekey_hcaptcha:
                    logger.warning(f"hCaptcha detected on {url}, sitekey: {captcha_sitekey_hcaptcha[:20]}...")
                    captcha_response = await solve_captcha(url, captcha_sitekey_hcaptcha, 'hcaptcha', current_proxy) # Call solve_captcha with 'hcaptcha' type
                    if captcha_response:
                        metrics['captcha_solved'].inc()
                        logger.info(f"hCaptcha solved, proceeding with request...")
                        captcha_params = {'h-captcha-response': captcha_response} # h-captcha parameter name
                        resp_after_captcha = await session.get(url, params=captcha_params, headers=headers, proxy=f"http://{current_proxy}" if current_proxy else None, timeout=CLIENT_TIMEOUT, ssl=ssl_context)
                        if resp_after_captcha.status == 200:
                            html_content = await resp_after_captcha.text()
                            soup = BeautifulSoup(html_content, 'lxml')
                            text_content = soup.get_text(separator=' ', strip=True).lower()
                        else:
                            logger.error(f"hCaptcha solve failed to get 200 OK, status: {resp_after_captcha.status}")
                            metrics['crawl_errors'].labels(type='captcha_failed_http_hcaptcha', proxy=proxy_key).inc()
                            update_proxy_score(proxy_key, False)
                            return None
                    else:
                        logger.error(f"hCaptcha solving failed for {url}")
                        metrics['crawl_errors'].labels(type='captcha_unsolved_hcaptcha', proxy=proxy_key).inc()
                        update_proxy_score(proxy_key, False)
                        return None
            # --- End hCaptcha Detection and Solving ---


            captcha_sitekey_v2 = re.search(r'data-sitekey="([^"]+)"', html_content) # reCAPTCHA v2 detection
            captcha_sitekey_v3 = re.search(r'sitekey: ?"([^"]+)"', html_content) # reCAPTCHA v3 detection
            captcha_sitekey_invisible = re.search(r'recaptcha\.render\("([^"]+)", {', html_content) # Invisible reCAPTCHA

            if captcha_sitekey_v2 and not hcaptcha_element: # Only check for reCAPTCHA v2 if hCaptcha is NOT present
                captcha_sitekey = captcha_sitekey_v2.group(1)
                logger.warning(f"ReCAPTCHA v2 detected on {url}, sitekey: {captcha_sitekey[:20]}...")
                captcha_response = await solve_captcha(url, captcha_sitekey, 'recaptcha_v2', current_proxy)
                if captcha_response:
                    metrics['captcha_solved'].inc()
                    logger.info(f"reCAPTCHA v2 solved, proceeding with request...")
                    captcha_params = {'g-recaptcha-response': captcha_response}
                    resp_after_captcha = await session.get(url, params=captcha_params, headers=headers, proxy=f"http://{current_proxy}" if current_proxy else None, timeout=CLIENT_TIMEOUT, ssl=ssl_context) # Pass ssl_context for captcha retry
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

            elif captcha_sitekey_v3 and not hcaptcha_element: # Only check for reCAPTCHA v3 if hCaptcha is NOT present
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

            elif captcha_sitekey_invisible and not hcaptcha_element: # Only check for invisible reCAPTCHA if hCaptcha is NOT present
                captcha_sitekey = captcha_sitekey_invisible.group(1)
                logger.warning(f"Invisible reCAPTCHA detected on {url}, element ID: {captcha_sitekey[:20]}...")
                captcha_response = await solve_captcha(url, captcha_sitekey, 'recaptcha_v2', current_proxy)
                if captcha_response:
                    metrics['captcha_solved'].inc()
                    logger.info(f"Invisible reCAPTCHA solved, proceeding with request...")
                    captcha_params = {'g-recaptcha-response': captcha_response}
                    resp_after_captcha = await session.get(url, params=captcha_params, headers=headers, proxy=f"http://{current_proxy}" if current_proxy else None, timeout=CLIENT_TIMEOUT, ssl=ssl_context) # Pass ssl_context for invisible captcha retry
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

            if BAD_PATH_PATTERN.search(parsed_url.path):
                logger.warning(f"Skipping URL due to bad path pattern: {url}")
                metrics['crawl_errors'].labels(type='bad_path_pattern', proxy=proxy_key).inc()
                return None

            domain = urlparse(url).netloc.lower() # Extract domain for generic domain check
            if domain in GENERIC_DOMAINS or any(blocked in domain for blocked in GENERIC_DOMAINS): # Improved generic domain check
                logger.warning(f"Skipping generic domain URL: {url}")
                metrics['crawl_errors'].labels(type='generic_domain', proxy=proxy_key).inc()
                return None


            domain = urlparse(url).netloc.replace('www.', '')

            emails = extract_emails(html_content, domain)
            phones = extract_phone_numbers(text_content)
            address = extract_address(soup)
            enhanced_data = await enhanced_data_parsing(soup, text_content)

            extracted_data.email_addresses = emails
            extracted_data.phone_numbers = phones
            extracted_data.physical_address = address
            extracted_data = extracted_data.copy(update=enhanced_data)

            relevance_score = calculate_relevance(text_content, term)
            extracted_data.relevance_score = relevance_score
            metrics['avg_relevance_score'].set(relevance_score)

            # --- Relevance Check DISABLED (Option 1) ---
            # if relevance_score < 0.3:
            #     logger.warning(f"Low relevance score ({relevance_score:.2f}) for {url}, skipping detailed extraction.")
            #     metrics['extraction_failure'].inc()
            #     return None
            # --- End Relevance Check Disabled ---


            license_numbers = extract_license_numbers(text_content)
            extracted_data.license_numbers = license_numbers
            logger.info(f"Data extracted successfully from {url}, relevance: {relevance_score:.2f}, emails: {emails}, phones: {phones}")
            metrics['extraction_success'].inc()
            update_proxy_score(proxy_key, True) # Reward proxy on success

            return extracted_data

        except aiohttp.ClientError as e:
            logger.error(f"aiohttp ClientError for {url} using proxy: {current_proxy if current_proxy else 'None'}: {e}")
            metrics['request_errors'].inc()
            metrics['crawl_errors'].labels(type='client_error', proxy=proxy_key).inc()
            update_proxy_score(proxy_key, False)
            pass # Important: Continue to the next attempt in the retry loop
        except asyncio.TimeoutError:
            logger.error(f"Asyncio timeout for {url} using proxy: {current_proxy if current_proxy else 'None'}")
            metrics['request_errors'].inc()
            metrics['crawl_errors'].labels(type='timeout_error', proxy=proxy_key).inc()
            update_proxy_score(proxy_key, False)
            pass # Important: Continue to the next attempt in the retry loop
        except Exception as e_crawl:
            logger.exception(f"Unexpected error during crawl and extraction for {url} using proxy: {current_proxy if current_proxy else 'None'}: {e_crawl}")
            metrics['page_extraction_errors'].inc()
            metrics['crawl_errors'].labels(type='extraction_error', proxy=proxy_key).inc()
            update_proxy_score(proxy_key, False)
            pass # Important: Continue to the next attempt in the retry loop
        finally:
            if current_proxy:
                PROXY_HEALTH[current_proxy] = time.time()

    # --- Fallback to Direct Scraping (No Proxy) ---
    if use_proxy and proxy_pool and not current_proxy: # If proxies were intended but none available initially
        logger.warning(f"All proxies failed or none available. Attempting direct scraping without proxy for {url}")
    elif use_proxy and proxy_pool: # If proxy retries failed
        logger.warning(f"Proxy attempts failed for {url}. Falling back to direct scraping without proxy.")

    try: # Direct scraping attempt
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        headers.update(random.choice(HEADERS_LIST))
        headers['Referer'] = urlparse(url).netloc

        resp = await session.get(url, headers=headers, timeout=CLIENT_TIMEOUT) # No proxy here

        metrics['http_status_codes'].labels(code=resp.status).inc()

        if resp.status in [403, 404, 429, 500, 503]: # Still handle HTTP errors even without proxy
            logger.warning(f"HTTP error {resp.status} for {url} during direct scraping.")
            metrics['crawl_errors'].labels(type=f'http_{resp.status}', proxy='no_proxy').inc() # Label as no_proxy
            return None # Stop if direct scraping also fails with these errors

        html_content = await resp.text()
        print("----- HTML Content (First 500 chars) - Direct Scraping -----")  # Debugging print
        print(html_content[:500])  # Print first 500 characters
        print("----- Text Content (First 500 chars) - Direct Scraping -----")  # Debugging print
        soup = BeautifulSoup(html_content, 'lxml')
        text_content = soup.get_text(separator=' ', strip=True).lower()
        print(text_content[:500])  # Print first 500 characters


        domain = urlparse(url).netloc.lower() # Extract domain for generic domain check (even for direct scraping)
        if domain in GENERIC_DOMAINS or any(blocked in domain for blocked in GENERIC_DOMAINS): # Generic domain check for direct scraping
            logger.warning(f"Skipping generic domain URL (direct scrape): {url}")
            metrics['crawl_errors'].labels(type='generic_domain', proxy='no_proxy').inc()
            return None


        emails = extract_emails(html_content, domain)
        phones = extract_phone_numbers(text_content)
        address = extract_address(soup)
        enhanced_data = await enhanced_data_parsing(soup, text_content)

        extracted_data.email_addresses = emails
        extracted_data.phone_numbers = phones
        extracted_data.physical_address = address
        extracted_data = extracted_data.copy(update=enhanced_data)

        relevance_score = calculate_relevance(text_content, term)
        extracted_data.relevance_score = relevance_score
        metrics['avg_relevance_score'].set(relevance_score)

        # --- Relevance Check DISABLED (Option 1) ---
        # if relevance_score < 0.3:
        #     logger.warning(f"Low relevance score (direct scrape, {relevance_score:.2f}) for {url}, skipping detailed extraction.")
        #     metrics['extraction_failure'].inc()
        #     return None
        # --- End Relevance Check Disabled ---


        license_numbers = extract_license_numbers(text_content)
        extracted_data.license_numbers = license_numbers
        logger.info(f"Data extracted successfully from {url} (direct scrape), relevance: {relevance_score:.2f}, emails: {emails}, phones: {phones}")
        metrics['extraction_success'].inc()


        return extracted_data

    except aiohttp.ClientError as e:
        logger.error(f"aiohttp ClientError during direct scraping for {url}: {e}")
        metrics['request_errors'].inc()
        metrics['crawl_errors'].labels(type='client_error', proxy='no_proxy').inc() # Label as no_proxy
        return None
    except asyncio.TimeoutError:
        logger.error(f"Asyncio timeout during direct scraping for {url}")
        metrics['request_errors'].inc()
        metrics['crawl_errors'].labels(type='timeout_error', proxy='no_proxy').inc() # Label as no_proxy
        return None
    except Exception as e_direct_crawl:
        logger.exception(f"Unexpected error during direct scraping for {url}: {e_direct_crawl}")
        metrics['page_extraction_errors'].inc()
        metrics['crawl_errors'].labels(type='extraction_error', proxy='no_proxy').inc() # Label as no_proxy
        return None
    # --- End Fallback to Direct Scraping ---

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
                user_pass, host_port = auth_part.split(':'), host_part.split(':')
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

    max_attempts = 2 if CAPTCHA_BALANCE > 5 else 1

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
                if not re.match(r'^[\w-]{40,100}$', result['code']):
                    logger.error(f"Invalid CAPTCHA solution format: {result['code']}")
                    return None
                logger.info(f"2Captcha solved successfully for {page_url} (Type: {captcha_type})...")
                cost = CAPTCHA_COST.get(captcha_type, 0.003) # Default cost if type not found (shouldn't happen)
                if captcha_type in CAPTCHA_COST:
                    cost = CAPTCHA_COST[captcha_type] # Use specific cost if available
                with captcha_lock:
                    CAPTCHA_BALANCE -= cost
                    logger.info(f"2Captcha balance updated, cost: ${cost:.4f}, remaining balance: ${CAPTCHA_BALANCE:.2f}")
                return result['code']
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
    data = {}
    social_links = []
    for platform in ["facebook", "twitter", "linkedin", "instagram"]:
        links = soup.select(f'a[href*="{platform}.com"]')
        social_links.extend([link['href'] for link in links if link.has_attr('href')])
    data["social_media"] = list(set(social_links))

    prop_count = 0
    number_words = re.findall(r'\b\d+\b', text_content)
    for num in number_words[:100]:
        if 10 < int(num) < 10000:
            prop_count = int(num)
            break
    data["estimated_properties"] = prop_count

    service_areas = []
    location_keywords = ["serving", "areas", "cover", "locations"]
    for elem in soup.find_all(['p', 'div', 'section']):
        text = elem.get_text().lower()
        if any(kw in text for kw in location_keywords):
            service_areas.extend(re.findall(r'\b[A-Z][a-z]+(?: [A-Z][a-z]+)*\b', elem.get_text()))
    data["service_areas"] = list(set(service_areas))

    software_keywords = {
        "guesty": ["guesty", "guestyguru"],
        "airdna": ["airdna", "air dna"],
        "pricelabs": ["pricelabs", "price labs"]
    }
    detected_software = []
    for sw, terms in software_keywords.items():
        if any(term in text_content for term in terms):
            detected_software.append(sw)
    data["management_software"] = detected_software

    return data


class PropertyManagerData(BaseModel):
    city: str = "N/A"
    search_term: str = "N/A"
    company_name: str = "N/A"
    website_url: HttpUrl
    email_addresses: List[EmailStr] = []
    phone_numbers: List[str] = []
    physical_address: str = "N/A"
    relevance_score: float = Field(default=0.0, ge=0.0, le=1.0)
    social_media: List[str] = []
    estimated_properties: Optional[int] = 0
    service_areas: List[str] = []
    management_software: List[str] = []
    license_numbers: List[str] = []
    llm_category: Optional[str] = None

    @field_validator('phone_numbers')
    def validate_phone_number(cls, values: List[str]) -> List[str]:
        return [phone.strip() for phone in values]


@lru_cache(maxsize=1000)
async def analyze_with_ollama(company_name, website_text, model_name='deepseek-r1:latest'):
    try:
        prompt = f"""Analyze if website is related to VACATION RENTALS (like Airbnbs) this should be obvious. Look for these indicators:\n\n- Keywords: 'vacation rentals', 'short term rentals', 'cabin rentals', 'condo rentals', 'beach rentals', 'Airbnb', 'rentals', 'book a stay', 'find rentals' etc there could be many more giveaways.\n- Website Functionality:  Listings of properties for short-term rent, search bar, filters for location/dates/amenities, booking or reservation features for vacation rentals, properties.\n- Target Audience:  Content aimed at travelers looking to rent vacation properties.\n\nWebsite Company Name: {company_name}\n\nWebsite Text Content:\n{website_text[:3000]}... (truncated for brevity)\n\nBased on these VACATION RENTAL indicators, categorize the website as:\n'Vacation Rental Website' OR 'Not Vacation Rental Related'.\n\nRespond with ONLY ONE of these categories. Do not add any extra text."""
        response = await asyncio.to_thread(
            ollama.chat,
            model=model_name,
            messages=[{'role': 'user', 'content': prompt}]
        )
        category_prediction = response['message']['content'].strip()
        logger.debug(f"[Ollama Analysis] Company: {company_name}, Predicted Category: {category_prediction}")
        return category_prediction
    except Exception as e:
        logger.error(f"Error during Ollama analysis for {company_name}: {e}")
        return None


async def analyze_batch(urls, session):
    logger.info(f"Starting batch LLM analysis for {len(urls)} URLs...")
    start_time_llm_batch = time.time()

    tasks = [fetch_text(url, CLIENT_TIMEOUT, session) for url in urls]
    website_texts = await asyncio.gather(*tasks, return_exceptions=True)

    combined_text = '\n---\n'.join([
        f"URL: {url}\nCONTENT:\n{text if not isinstance(text, Exception) else 'Failed to fetch content'}"
        for url, text in zip(urls, website_texts)
    ])

    try:
        response = await asyncio.to_thread(
            ollama.chat,
            model='deepseek-r1:latest',
            messages=[{'role': 'user', 'content': f"Analyze these {len(urls)} vacation rental sites:\n{combined_text[:15000]}"}]
        )
        categories = [cat.strip() for cat in response['message']['content'].strip().split('\n') if cat.strip()]
        categories = categories[:len(urls)]
    except Exception as e_ollama:
        logger.error(f"Error during Ollama batch analysis: {e_ollama}")
        categories = ["LLM Analysis Failed"] * len(urls)

    duration_llm_batch = time.time() - start_time_llm_batch
    logger.info(f"Batch LLM analysis completed in {duration_llm_batch:.2f} seconds for {len(urls)} URLs. Example categories: {categories[:3]}")

    return categories


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
        else:
            PROXY_SCORES[proxy] = max(0, PROXY_SCORES[proxy] + PROXY_SCORE_CONFIG['failure']) # Use failure score from config
        logger.debug(f"Updated score for proxy {proxy}, new score: {PROXY_SCORES[proxy]}")
     else:
        logger.debug("update_proxy_score called with proxy=None, skipping score update.")


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
    decoded_html_content = decode_emails(html_content) # Decode first
    # More permissive email regex (allows for more TLDs and variations)
    email_matches = list(set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})*", decoded_html_content, re.IGNORECASE)))
    print("----- Raw Email Matches (Refined Regex + Decoding) -----") # Debugging print
    print(email_matches)
    return email_matches

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
    return phone_matches


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
                    r'\b(City|State|Zip|Postal)\b', line, re.IGNORECASE
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
    text_content_lower = text_content.lower()
    total_keyword_frequency = 0

    for keyword in RELEVANCE_KEYWORDS: # Check for multiple keywords
        keyword_frequency = text_content_lower.count(keyword.lower())
        total_keyword_frequency += keyword_frequency
        logger.debug(f"Keyword: '{keyword}', Frequency: {keyword_frequency}") # DEBUG: Log keyword frequencies

    word_count = len(text_content_lower.split())
    normalized_frequency = total_keyword_frequency / word_count if word_count else 0

    logger.debug(f"Total Keyword Frequency: {total_keyword_frequency}, Word Count: {word_count}, Relevance Score: {normalized_frequency:.2f}") # DEBUG: Log score components
    return normalized_frequency


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
        self.tokens = self.rate
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
        self.tokens = min(self.rate, self.tokens + tokens_to_add) #  tokens don't exceed rate
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
    domain = urlparse(url).netloc.lower() # Extract domain using urlparse
    if domain in GENERIC_DOMAINS or any(blocked in domain for blocked in GENERIC_DOMAINS): # More accurate check
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
                        else:
                            PROXY_SCORES[proxy] = max(0, PROXY_SCORES.get(proxy, 0) + PROXY_SCORE_CONFIG['failure']) # Use failure score from config
                        # No score update if exception during health check - handled in update_proxy_score
                except Exception as e:
                   logger.warning(f"Proxy health check failed for {proxy}: {e}") # Log individual proxy health check failures
                update_proxy_score(proxy, False) # Penalize proxy on health check failure
             # await session.close() # Let session pool manage closing - handled by session manager
            await asyncio.sleep(3600)  # Check every 1 hour - reduced frequency for proxy health check
        except Exception as e:
            logger.error(f"Proxy health check loop error: {str(e)}")
            await asyncio.sleep(600) # Reduced frequency of health check loop error logging

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
            if "brd.superproxy.io" in proxy_string:  # Apply Bright Data HTTP fix
                proxy_string = proxy_string.replace("https://", "http://") + "?type=http"  # Force HTTP for Bright Data
            proxies.append(proxy_string)
            provider_counts['brightdata'] += 1

    logger.info(f"Proxy pool 구성: {provider_counts}")

    if not proxies:  # Check if proxies list is empty after configuration
        raise ValueError("No proxy credentials found in environment variables")

        healthy_proxies = [p for p, s in PROXY_SCORES.items() if s > 20] # More explicit variable name for score
        if healthy_proxies:
            logger.debug(f"Using healthy proxies, count: {len(healthy_proxies)}")
            return healthy_proxies
        else:
            logger.warning("No healthy proxies found, using refreshed pool.")
            logger.debug("No healthy proxies found, using fresh pool (limited size).")
            return proxies  # Return ALL proxies if no healthy ones