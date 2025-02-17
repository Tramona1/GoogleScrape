# utils.py - FINALIZED version with critical fixes and robots.txt commented out
import re
from collections import defaultdict
from urllib.parse import urlparse, urljoin
import random
import asyncio
import aiohttp
import logging
import os
from prometheus_client import Counter
from bs4 import BeautifulSoup  # Import BeautifulSoup4
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, wait_exponential_jitter # Import tenacity decorators
import urllib.robotparser  # Import for robots.txt

logger = logging.getLogger(__name__)
LOG_LEVEL = logging.DEBUG

# Proxy management
PROXY_SCORES = defaultdict(float)
PROXY_HEALTH = defaultdict(float)
CAPTCHA_BALANCE = 0.0
PROXY_POOL = []

# URL patterns
BAD_PATH_PATTERN = re.compile(r'(wp-json|wp-admin|\.php|\.aspx)')
GENERIC_DOMAINS = {'facebook.com', 'twitter.com', 'linkedin.com'}

# Prometheus metrics - INITIALIZED ONLY ONCE IN UTILS.PY
metrics = {
    'serpapi_errors': Counter('serpapi_errors', 'SerpAPI errors'),
    'captcha_requests': Counter('captcha_requests', 'CAPTCHA challenges'),
    'captcha_failed': Counter('captcha_failed', 'Failed CAPTCHAs'),
    'crawled_pages': Counter('crawled_pages', 'Successful crawls'),
    'extraction_failure': Counter('extraction_failure', 'Failed extractions'),
    'http_errors': Counter('http_errors', 'HTTP errors during crawling') # New metric for HTTP errors
}

# Session management
session_pool = None

async def get_session():
    global session_pool
    if not session_pool:
        session_pool = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=75),
            timeout=aiohttp.ClientTimeout(total=45)
        )
    return session_pool

async def close_session_pool():
    global session_pool
    if session_pool:
        await session_pool.close()
        session_pool = None

# Proxy functions
def rotate_proxy(proxy_pool):
    if proxy_pool:
        return random.choice(proxy_pool)
    return None

def update_proxy_score(proxy_key, success):
    PROXY_SCORES[proxy_key] += 1 if success else -1

def decay_proxy_scores():
    for proxy in list(PROXY_SCORES.keys()):
        PROXY_SCORES[proxy] *= 0.95
        if abs(PROXY_SCORES[proxy]) < 0.01:
            del PROXY_SCORES[proxy]

# URL handling
def normalize_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')

def is_valid_url(url):
    try:
        return all(urlparse(url)[:2])
    except:
        return False

def can_crawl(url, user_agent='PropertyManagerCrawler/1.3'):
    # --- ROBOTS.TXT & ETHICAL CRAWLING DISABLED FOR TESTING ---
    return True  # Always return True for testing - bypass robots.txt
    # --- END ROBOTS.TXT & ETHICAL CRAWLING DISABLED FOR TESTING ---

    # --- Original robots.txt check code (commented out) ---
    # try:
    #     parsed_url = urlparse(url)
    #     base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    #     robots_url = urljoin(base_url, '/robots.txt')
    #     rp = urllib.robotparser.RobotFileParser()
    #     rp.set_url(robots_url)
    #     rp.read()

    #     async def fetch_robots_txt():
    #         async with aiohttp.ClientSession() as robots_session:
    #             try:
    #                 async with robots_session.get(robots_url, timeout=10) as response:
    #                     if response.status == 200:
    #                         rp.parse(await response.text())
    #             except aiohttp.ClientError as e:
    #                 logger.warning(f"Error fetching robots.txt: {e}")
    #             return rp.can_fetch(user_agent, url)

    #     return asyncio.run(fetch_robots_txt())
    # except Exception as e:
    #     logger.warning(f"Robots.txt check error for {url}: {e}")
    #     return True
    # --- End original robots.txt check code ---


# --- UPDATED crawl_and_extract_async with BeautifulSoup4 and tenacity ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=10, jitter=1),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    reraise=True
)
async def crawl_and_extract_async(session, context, proxy_pool=None):
    try:
        logger.debug(f"Crawling URL: {context['url']}") # Log before crawling

        # --- ROBOTS.TXT CHECK DISABLED FOR TESTING ---
        # if not can_crawl(context['url']):
        #     logger.warning(f"Robots.txt disallowed crawl for {context['url']}, skipping.")
        #     return None
        # --- END ROBOTS.TXT CHECK DISABLED FOR TESTING ---


        async with session.get(context['url'], proxy=f"http://{proxy_pool}" if proxy_pool else None, timeout=45) as response: # Use proxy if provided
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            html_content = await response.text()
            soup = BeautifulSoup(html_content, 'html.parser') # Parse HTML with BeautifulSoup4

            # --- Basic Example Extraction - Adapt this to your target websites! ---
            company_name_element = soup.find('h1') # Example: Find the main heading (adapt selector!)
            company_name = company_name_element.text.strip() if company_name_element else "N/A"

            email_addresses = []
            email_links = soup.find_all('a', href=re.compile(r'mailto:', re.IGNORECASE))
            for link in email_links:
                email = link['href'].replace('mailto:', '').strip()
                if email and '@' in email: # Basic email validation
                    email_addresses.append(email)
            email_addresses = list(set(email_addresses)) # Remove duplicates

            phone_numbers = [] # Placeholder - You need to implement phone number extraction

            logger.debug(f"Extracted data from {context['url']}: Company Name: {company_name}, Emails: {email_addresses}") # Log extracted data

            return {
                "website_url": context['url'],
                "company_name": company_name,
                "email_addresses": email_addresses,
                "phone_numbers": phone_numbers, # Placeholder
                "city": context.get('city', 'N/A'), # Pass city context
                "search_term": context.get('term', 'N/A') # Pass search term context
            }

    except aiohttp.ClientResponseError as http_err: # Catch HTTP errors specifically
        metrics['http_errors'].inc() # Increment HTTP error metric
        logger.error(f"HTTP error {http_err.status} crawling {context['url']}: {http_err}")
        return None # Or handle differently based on status code (retry on 5xx, not on 404?)
    except aiohttp.ClientError as e:
        logger.error(f"Client error crawling {context['url']}: {e}")
        return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout crawling {context['url']}: Timeout for {context['url']}")
        return None
    except Exception as e_extract:
        logger.exception(f"Extraction error from {context['url']}: {e_extract}") # Log full exception traceback
        metrics['extraction_failure'].inc()
        return None

async def analyze_batch(urls, session=None): # Placeholder remains the same for now
    await asyncio.sleep(1)
    return ["General Services" for _ in urls]

def get_proxy_pool():
    if os.getenv("USE_PROXY", "False").lower() == "true":
        return [os.getenv("PROXY_HOST")]
    return []