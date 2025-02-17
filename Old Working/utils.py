# utils.py
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import time
import logging
import urllib.robotparser
from urllib.parse import urlparse, urljoin
import os
from twocaptcha import TwoCaptcha
from pydantic import BaseModel, EmailStr, HttpUrl, confloat, validator, Field
from typing import List, Optional
import ollama
from html import unescape
import random
import socket
from fake_useragent import UserAgent
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from urllib.parse import urlparse
import struct


MAX_CAPTCHA_ATTEMPTS = 3
logger = logging.getLogger(__name__)

GENERIC_DOMAINS = {
    "example.com", "orbitz.com", "blogspot.com", "google.com", "facebook.com",
    "youtube.com", "twitter.com", "linkedin.com", "instagram.com", "pinterest.com",
    "yelp.com", "tripadvisor.com", "reddit.com", "wikipedia.org", "mediawiki.org",
    "amazon.com", "ebay.com", "craigslist.org", "indeed.com", "glassdoor.com",
    "zillow.com", "realtor.com", "apartments.com", "airbnb.com", "vrbo.com",
    "booking.com", "allpropertymanagement.com", "airdna.co", "airdna.com",
    "vacasa.com", "cozycozy.com", "kayak.com", "expedia.com", "news-outlet.com",
    "social-media.com", "tripadvisor.com", "evolve.com",
    "homes-and-villas.marriott.com", "hometogo.com", "www.hometogo.com",
    "whimstay.com", "www.whimstay.com", "avantstay.com", "houfy.com",
}

BAD_PATH_PATTERN = re.compile(r'/(category|tag/page)/', re.IGNORECASE)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
]

CLIENT_TIMEOUT = 30 # Define a timeout value, you can adjust this

PROXY_HEALTH = {} # Proxy health check dictionary

def get_proxy_pool():
    proxies = []
    # Oxylabs
    if os.getenv('OXLABS_USER') and os.getenv('OXLABS_PASS'):
        proxies.append(f"{os.getenv('OXLABS_USER')}:{os.getenv('OXLABS_PASS')}@pr.oxylabs.io:7777") # Corrected Oxylabs port
    # Smartproxy
    if os.getenv('SMARTPROXY_USER') and os.getenv('SMARTPROXY_PASS'):
        proxies.append(f"{os.getenv('SMARTPROXY_USER')}:{os.getenv('SMARTPROXY_PASS')}@gate.smartproxy.com:7000")
    # Bright Data
    if os.getenv('BRIGHTDATA_USER') and os.getenv('BRIGHTDATA_PASS'):
        proxies.append(f"{os.getenv('BRIGHTDATA_USER')}:{os.getenv('BRIGHTDATA_PASS')}@brd.superproxy.io:33335") # Corrected Bright Data port to 33335
    return [p for p in proxies if PROXY_HEALTH.get(p, 0) < 3]  # Max 3 failures


PROXY_REGEX = r'^([\w-]+(:[\S]+)?@)?([a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)+|(\d{1,3}\.){3}\d{1,3}):\d+$' # Updated regex

def validate_proxy_config(): # Validation function
    if os.getenv("USE_PROXY", "False").lower() == "true": # Only check proxy config if USE_PROXY is true
        proxy_pool = get_proxy_pool() # Get proxy pool using the function
        logger.debug(f"DEBUG: PROXY_POOL inside validate_proxy_config(): {proxy_pool}") # Log PROXY_POOL in validation

        # Add proxy validation for default credentials
        if any("your_proxy_host" in proxy for proxy in proxy_pool):
            logger.error("Default proxy credentials detected in PROXY_POOL. Please update your environment variables with your actual proxy credentials.")
            return False

        if not proxy_pool: # More specific logging for empty PROXY_POOL
            logger.warning("Invalid proxy configuration detected: PROXY_POOL is empty.")
            return False

        invalid_proxies = [proxy for proxy in proxy_pool if not re.match(PROXY_REGEX, proxy)] # Identify invalid proxies
        if invalid_proxies: # More specific logging for invalid regex match
            logger.warning(f"Invalid proxy configuration detected: Proxies failed regex validation: {invalid_proxies}")
            return False

        logger.info(f"PROXY_POOL configured with {len(proxy_pool)} proxies.")
        return True # Return True if valid
    else:
        logger.info("Proxy usage disabled (USE_PROXY=false).")
        return True # Return True as proxy config is valid (disabled)


async def solve_captcha(page_url, captcha_sitekey, captcha_type, current_proxy=None, rotation_attempts=0):
    if not os.getenv('TWOCAPTCHA_API_KEY'):
        logger.warning("TWOCAPTCHA_API_KEY not set, CAPTCHA solving disabled.")
        return None

    MAX_CAPTCHA_ATTEMPTS = 3
    solver = TwoCaptcha(os.getenv('TWOCAPTCHA_API_KEY'))
    try:
        balance = await asyncio.to_thread(solver.balance)
    except Exception as e_balance:
        logger.error(f"Error checking 2Captcha balance: {e_balance}. CAPTCHA solving disabled.")
        logger.warning("Ensure TWOCAPTCHA_API_KEY is correctly set and your balance is sufficient.")
        return None

    balance = await asyncio.to_thread(solver.balance)
    if balance < 2:
        logger.error(f"2Captcha balance too low: ${balance}. CAPTCHA solving disabled.")
        return None

    kwargs = {}
    if current_proxy:
        kwargs["default_proxy"] = current_proxy
        logger.debug(f"CAPTCHA solving using proxy: {current_proxy}")

    try:
        if captcha_type == 'recaptcha_v2':
            result = await asyncio.to_thread(
                solver.recaptcha,
                apiKey=os.getenv('TWOCAPTCHA_API_KEY'),
                sitekey=captcha_sitekey,
                url=page_url,
                version='v2',
                attempts=MAX_CAPTCHA_ATTEMPTS,
                **kwargs
            )
        elif captcha_type == 'recaptcha_v3':
            result = await asyncio.to_thread(
                solver.recaptcha,
                apiKey=os.getenv('TWOCAPTCHA_API_KEY'),
                sitekey=captcha_sitekey,
                url=page_url,
                version='v3',
                action='verify',
                min_score=0.5,
                attempts=MAX_CAPTCHA_ATTEMPTS,
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
                attempts=MAX_CAPTCHA_ATTEMPTS,
                **kwargs
            )


        if result and 'code' in result:
            if not re.match(r'^[\w-]{40,100}$', result['code']):
                logger.error(f"Invalid CAPTCHA solution format: {result['code']}")
                return None
            logger.info(f"2Captcha solved successfully for {page_url} (Type: {captcha_type})...")
            return result['code']
        else:
            logger.error(f"2Captcha solve failed, no code returned (Type: {captcha_type})")
            if rotation_attempts >= MAX_CAPTCHA_ATTEMPTS:
                raise Exception("Max CAPTCHA attempts reached - no code returned")
            return None

    except Exception as e:
        logger.error(f"Error during CAPTCHA solving for {page_url}: {e}")
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

    @validator('phone_numbers', each_item=True)
    def validate_phone_number(cls, phone):
        return phone.strip()

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

def detect_license_info(soup):
    license_numbers = []
    license_patterns = [
        r'license\s*#?\s*(\d{5,12})',
        r'license\s*number\s*:\s*(\d{5,12})',
        r'lic\s*#?\s*(\d{5,12})',
        r'license\s*:\s*(\d{5,12})'
    ]
    text_content = soup.get_text()
    for pattern in license_patterns:
        matches = re.finditer(pattern, text_content, re.IGNORECASE)
        for match in matches:
            license_numbers.append(match.group(1))
    return list(set(license_numbers))

def decode_emails(text):
    text = re.sub(r'\b(\w+)\s*\[?@\(?AT\)?\]?\s*(\w+)\s*\[?\.\(?DOT\)?\]?\s*(\w+)\b',
                r'\1@\2.\3', text, flags=re.IGNORECASE)
    text = re.sub(r'\b(\w+)\s*\(\s*at\s*\)\s*(\w+)\s*\(\s*dot\s*\)\s*(\w+)\b',
                r'\1@\2.\3', text, flags=re.IGNORECASE)
    text = unescape(text)
    return text

def validate_email(email):
    return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email)

def validate_phone(phone):
    """Strict international phone validation returning match object"""
    cleaned = re.sub(r'\D', '', phone)
    match = re.fullmatch(
        r'^\+?[1-9]\d{1,3}'  # Country code
        r'\d{6,14}$',        # National number
        cleaned
    )
    return match if match and 8 <= len(cleaned) <= 15 else None

ua = UserAgent()
def get_random_user_agent():
    return ua.random

@retry(wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5),
       retry_error_callback=lambda retry_state: logger.warning(
           f"Retry attempt {retry_state.attempt_number} after error: {retry_state.outcome.exception()}"))
async def fetch_url(session, url, proxy_pool=None, retry_state=None, ssl_context=None): # Added ssl_context=None
    # Add random delay to mimic human behavior
    await asyncio.sleep(random.uniform(1, 3))

    proxy_url_to_use = None
    current_proxy = None

    if os.getenv("USE_PROXY", "False").lower() == "true":
        if proxy_pool and len(proxy_pool) > 0: # Check for non-empty proxy_pool
            current_proxy = random.choice(proxy_pool)
            proxy_url_to_use = f"https://{current_proxy}" # Use HTTPS
            logger.debug(f"Fetching {url} using proxy from PROXY_POOL: {current_proxy}")
        elif os.getenv('PROXY_HOST') and os.getenv('PROXY_PORT') and os.getenv('PROXY_USER') and os.getenv('PROXY_PASSWORD'):
            proxy_url_to_use = f"https://{os.getenv('PROXY_USER')}:{os.getenv('PROXY_PASSWORD')}@{os.getenv('PROXY_HOST')}:{os.getenv('PROXY_PORT')}" # Use HTTPS
            logger.debug(f"Fetching {url} using configured proxy.")
        else:
            logger.warning("Proxy settings enabled but incomplete. Not using proxy for this request.")
    else:
        logger.debug("Proxy usage disabled.")

    attempt_number = retry_state.attempt_number if retry_state else 1

    try:
        headers = {
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            "X-Forwarded-For": socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff))),
            'Accept-Encoding': 'gzip, deflate, br',
        }
        logger.debug(f"Request Headers for {url} (Attempt {attempt_number}): {headers}")
        logger.debug(f"Fetching URL: {url} with proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}")
        async with session.get(url, timeout=25, proxy=proxy_url_to_use, headers=headers, ssl=ssl_context) as response: # Pass ssl=ssl_context
            logger.debug(f"Response status for {url} (Attempt {attempt_number}): {response.status}")

            if response.status == 429:
                retry_after = int(response.headers.get('Retry-After', random.randint(30, 60)))
                logger.warning(f"Rate limited. Waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                raise Exception("Rate limit exceeded")
            elif response.status == 403:
                logger.warning(f"Forbidden (403) for {url} with proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}.") # Removed proxy blacklisting for now
                if current_proxy and proxy_pool and current_proxy in proxy_pool:
                    if len(proxy_pool) > 1: # Only remove if there are other proxies
                        proxy_pool.remove(current_proxy)
                    else:
                        logger.info(f"Proxy {current_proxy} removed from pool. Remaining proxies: {len(proxy_pool)}")
                raise aiohttp.ClientError(f"Forbidden 403, blacklisted proxy if used")

            response.raise_for_status()
            return await response.text()
    except aiohttp.ClientError as e:
        logger.warning(f"aiohttp ClientError for {url}: {e}")
        raise e
    except asyncio.TimeoutError as e:
        logger.warning(f"Async request timeout for {url} (Attempt {attempt_number}): {e} | Proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}")
        raise e
    except Exception as e:
        if "Rate limit" in str(e): # Catch-all for rate limit exceptions, including custom ones
            logger.warning(f"Rate limit encountered for {url} (Attempt {attempt_number}): {e} | Proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}")
        logger.error(f"Unexpected error in fetch_url for {url} (Attempt {attempt_number}): {e} | Proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}")
        raise e

async def crawl_and_extract_async(session, context, proxy_pool=None, ssl_context=None): # Added ssl_context=None
    """Core crawling logic with proxy rotation"""
    try:
        proxy_url_to_use = None
        if os.getenv("USE_PROXY", "False").lower() == "true":
            if proxy_pool and len(proxy_pool) > 0:  # Check for non-empty proxy_pool
                proxy = random.choice(proxy_pool)
                proxy_url_to_use = f"https://{proxy}"  # Use HTTPS
                logger.debug(f"crawl_and_extract_async: Using proxy: {proxy}")
            else:
                logger.error("Proxy usage enabled but proxy pool is empty. Not using proxy for this request.")
                proxy_url_to_use = None # Explicitly set to None when proxy pool is empty and proxy is enabled
        else:
            logger.debug("crawl_and_extract_async: Proxy usage disabled.")
            proxy_url_to_use = None # Explicitly set to None when proxy is disabled


        headers = {"User-Agent": random.choice(USER_AGENTS)}

        async with session.get(context['url'],
                             proxy=proxy_url_to_use,
                             headers=headers,
                             timeout=CLIENT_TIMEOUT,
                             ssl=ssl_context) as response: # Pass ssl=ssl_context
            text = await response.text()
            return await extract_contact_info(session, text, context, proxy_pool=proxy_pool, ssl_context=ssl_context) # Direct call to extract_contact_info, Pass ssl_context

    except Exception as e:
        logger.error(f"Error crawling {context['url']}: {str(e)}")
        return None


def is_valid_url(url):
    if not url:
        logger.debug(f"URL '{url}' is invalid: Empty URL")
        return False
    parsed = urlparse(url)
    if not parsed.netloc:
        logger.debug(f"URL '{url}' is invalid: No netloc")
        return False
    if parsed.netloc in GENERIC_DOMAINS:
        logger.debug(f"URL '{url}' is invalid: Generic domain: {parsed.netloc}")
        return False
    if BAD_PATH_PATTERN.search(url):
        logger.debug(f"URL '{url}' is invalid: Bad path pattern")
        return False
    if parsed.netloc.endswith(".pdf"):
        logger.debug(f"URL '{url}' is invalid: PDF file")
        return False
    if parsed.netloc.endswith((".jpg", ".jpeg", ".png", ".gif")):
        logger.debug(f"URL '{url}' is invalid: Image file")
        return False

    blocked_domain_endings = (
        "tripadvisor.com", "airbnb.com", "vrbo.com",
        "booking.com", "apartments.com"
    )  # Reduced list

    if parsed.netloc.lower().endswith(blocked_domain_endings):
        logger.debug(f"URL '{url}' is invalid: Blocked domain ending: {parsed.netloc}")
        return False

    if parsed.netloc.lower() == "google.com" and "/travel" in parsed.path.lower():
        logger.debug(f"URL '{url}' is invalid: Google Travel URL")
        return False

    logger.debug(f"URL '{url}' is valid")
    return True


async def extract_contact_info(session, text, context, proxy_pool=None, ssl_context=None): # extract_contact_info function (UPDATED to accept session, proxy_pool, Removed ssl_context)
    logger.debug(f"Entering extract_contact_info for url: {context.get('url')} with context: {context}")
    city = context.get("city", "N/A")
    search_term = context.get("term", "N/A")

    company_name = "N/A"
    email_addresses = []
    phone_numbers = []
    physical_address = "N/A"
    relevance_score = 0.0
    llm_category = "Not Rental Related"

    extracted_data = { # No changes here
        "city": context.get("city", "N/A"),
        "search_term": context.get("term", "N/A"),
        "company_name": company_name,
        "website_url": context.get('url'), # Get URL from context
        "email_addresses": [],
        "phone_numbers": [],
        "physical_address": physical_address,
        "relevance_score": relevance_score,
        "social_media": [],
        "estimated_properties": 0,
        "service_areas": [],
        "management_software": [],
        "license_numbers": [],
        "llm_category": llm_category
    }

    try:
        soup = BeautifulSoup(text, 'html.parser')
        text_content = soup.get_text(separator=' ', strip=True).lower()

        logger.debug(f"Processing URL for phone/email extraction: {context.get('url')}")
        logger.debug(f"Extracted Text Content (first 500 chars): {text_content[:500]}...")

        if "captcha" in text_content or "verify you are human" in text_content:
            logger.warning(f"Basic CAPTCHA detection triggered at {context.get('url')}. Attempting 2Captcha solve...")

            # --- Dynamic sitekey extraction ---
            soup = BeautifulSoup(text, 'html.parser')
            captcha_sitekey = None
            captcha_type = None

            # --- UPDATED Sitekey Extraction Logic ---
            ginput_recaptcha_div = soup.find('div', class_='ginput_container ginput_recaptcha gform-initialized') # NEW SELECTOR
            if ginput_recaptcha_div and 'data-sitekey' in ginput_recaptcha_div.attrs: # Check for new div
                captcha_sitekey = ginput_recaptcha_div['data-sitekey']
                captcha_type = 'recaptcha_v2' # Assuming v2 based on HTML snippets
                logger.debug(f"Detected reCAPTCHA v2 (ginput_recaptcha), sitekey: {captcha_sitekey}")
            elif soup.find('div', class_='g-recaptcha'): # Fallback to original v2 selector
                recaptcha_v2_div = soup.find('div', class_='g-recaptcha') # Original v2 selector
                if recaptcha_v2_div and 'data-sitekey' in recaptcha_v2_div.attrs:
                    captcha_sitekey = recaptcha_v2_div['data-sitekey']
                    captcha_type = 'recaptcha_v2'
                    logger.debug(f"Detected reCAPTCHA v2 (g-recaptcha), sitekey: {captcha_sitekey}")
            elif soup.find('div', class_='gf_invisible ginput_recaptchav3'): # Fallback to original v3 selector
                recaptcha_v3_div = soup.find('div', class_='gf_invisible ginput_recaptchav3') # Original v3 selector
                if recaptcha_v3_div and 'data-sitekey' in recaptcha_v3_div.attrs:
                    captcha_sitekey = recaptcha_v3_div['data-sitekey']
                    captcha_type = 'recaptcha_v3'
                    logger.debug(f"Detected reCAPTCHA v3-like, sitekey: {captcha_sitekey}")
            # --- END UPDATED Sitekey Extraction Logic ---


            if captcha_sitekey:
                captcha_proxy = random.choice(proxy_pool) if proxy_pool else None
                captcha_code = await solve_captcha(context.get('url'), captcha_sitekey, captcha_type, captcha_proxy)
                if captcha_code:
                    if not re.match(r'^[\w-]{40,100}$', captcha_code):
                        logger.error(f"Invalid CAPTCHA solution format: {captcha_code}")
                        return None
                    logger.info(f"2Captcha solved successfully for {context.get('url')} (Type: {captcha_type})...")
                else:
                    logger.warning(f"2Captcha solve failed for {context.get('url')} (Type: {captcha_type}). Skipping extraction.")
                    return None
            else:
                logger.warning(f"No CAPTCHA sitekey found on {context.get('url')}, skipping 2Captcha solve.")
                return None

        company_name = ( # Company name extraction - no changes
            soup.find('h1').get_text(strip=True)
            if soup.find('h1') else None
        ) or (
            urlparse(context.get('url')).netloc.split('.')[-2].title() if urlparse(context.get('url')).netloc.split('.')[-2].title() else None
        ) or (
            soup.title.get_text().split('|')[0].strip()
            if soup.title else None
        ) or (
            urlparse(context.get('url')).netloc.replace('www.', '').split('.')[0].title()
        ) or "N/A"
        extracted_data["company_name"] = company_name
        logger.debug(f"Detected Company Name: {company_name} for URL: {context.get('url')}")
        logger.debug(f"Extracted City: {context.get('city', 'N/A')}")
        logger.debug(f"Extracted Company: {company_name}")

        phone_pattern_compiled = re.compile(r'''
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
            ''', re.VERBOSE | re.IGNORECASE)

        tel_links_phones = set() # Tel link extraction - no changes
        for a_tag in soup.find_all('a', href=re.compile(r'^tel:')):
            tel_link = a_tag['href']
            phone_number_from_link = tel_link[4:]
            logger.debug(f"Tel Link Phone Candidate: {phone_number_from_link}")
            validated_phone = validate_phone(phone_number_from_link)
            if validated_phone:  # Check if match object exists
                validated_phone_number_str = validated_phone.group(0).strip()
                tel_links_phones.add(validated_phone_number_str)
                logger.debug(f"Validated Tel Link Phone: {validated_phone_number_str}")
            else:
                regex_match_tel_link = phone_pattern_compiled.search(phone_number_from_link)
                if regex_match_tel_link:
                    validated_tel_link_phone = validate_phone(regex_match_tel_link.group(0))
                    if validated_tel_link_phone:
                        validated_tel_link_phone_str = validated_tel_link_phone.group(0).strip()
                        tel_links_phones.add(validated_tel_link_phone_str)
                        logger.debug(f"Regex-Validated Tel Link Phone: {validated_tel_link_phone_str}")

        text_phones = set() # Text phone extraction - no changes
        raw_phone_matches = set(phone_pattern_compiled.findall(text))
        logger.debug(f"Raw Text Phone Matches: {raw_phone_matches}")
        for raw_phone in raw_phone_matches:
            phone_candidate = raw_phone[0] if isinstance(raw_phone, tuple) else raw_phone
            if phone_candidate:
                validated_phone = validate_phone(phone_candidate)
                if validated_phone:  # Now checks for match object
                    validated_phone_number_str = validated_phone.group(0).strip()
                    if validated_phone_number_str not in tel_links_phones:
                        text_phones.add(validated_phone_number_str)
                        logger.debug(f"Validated Text Phone: {validated_phone_number_str}")
                    else:
                        logger.debug(f"Skipping duplicate phone (already in tel links): {validated_phone_number_str}")

        phone_numbers = list(tel_links_phones.union(text_phones))
        extracted_data["phone_numbers"] = phone_numbers
        logger.debug(f"Phone Numbers Found: {phone_numbers}")

        email_matches = set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", decode_emails(text))) # Email Extraction - no changes
        validated_emails = [email.strip() for email in email_matches if validate_email(email)]
        email_addresses = validated_emails
        logger.debug(f"Email Addresses Found: {email_addresses}")

        address_element = soup.find('address') # Address extraction - no changes
        physical_address = address_element.get_text(separator=', ', strip=True) if address_element else "N/A"
        extracted_data["physical_address"] = physical_address

        enhanced_data = await enhanced_data_parsing(soup, text_content) # Enhanced data parsing - no changes - AWAIT IS HERE NOW
        extracted_data.update(enhanced_data)

        license_numbers = detect_license_info(soup) # License numbers - no changes
        extracted_data["license_numbers"] = license_numbers

        extracted_data["relevance_score"] = relevance_score # Relevance score - no changes
        extracted_data["llm_category"] = await analyze_with_ollama(company_name, text_content) # Ollama analysis - no changes

        try: # Data validation - no changes
            validated_data = PropertyManagerData(**extracted_data)
            return validated_data.dict()
        except Exception as e_validation:
            logger.error(f"Data validation failed for {context.get('url')}: {e_validation}")
            logger.debug(f"Data that failed validation: {extracted_data}")
            return None

    except aiohttp.ClientError as e: # Exception handling - no changes
        logger.error(f"Request error crawling {context.get('url')}: {e}")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error in extract_contact_info for {context.get('url')}: {e}")
        return None