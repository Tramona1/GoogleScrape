# utils.py
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import time
import logging
import urllib.robotparser
from urllib.parse import urlparse, urljoin
import json
import os
from twocaptcha import TwoCaptcha
from pydantic import BaseModel, EmailStr, HttpUrl, confloat, validator, Field
from typing import List, Optional
import ollama
from html import unescape
import random
import socket
import struct
from fake_useragent import UserAgent
from tenacity import retry, wait_exponential, stop_after_attempt
from urllib.parse import urlparse # Make sure this import is here
import time # Make sure time is imported

MAX_CAPTCHA_ATTEMPTS = 3  # Top of file
logger = logging.getLogger(__name__)

GENERIC_DOMAINS = {
    "example.com", "wordpress.com", "wixsite.com",
    "blogspot.com", "google.com", "facebook.com",
    "youtube.com", "twitter.com", "linkedin.com",
    "instagram.com", "pinterest.com", "yelp.com",
    "tripadvisor.com",
    "wikipedia.org", "mediawiki.org",
    "amazon.com", "ebay.com", "craigslist.org", "indeed.com",
    "glassdoor.com", "zillow.com", "realtor.com", "apartments.com",
    "airbnb.com", "vrbo.com", "booking.com",
    "allpropertymanagement.com",
    "airdna.co", "airdna.com",
    "vacasa.com", "cozycozy.com",
    "kayak.com", "expedia.com",
    "news-outlet.com", "social-media.com",
    "tripadvisor.com",
    "evolve.com",
    "homes-and-villas.marriott.com",
    "hometogo.com",
    "www.hometogo.com",
    "whimstay.com",
    "www.whimstay.com",
    "avantstay.com",
    "houfy.com",
}

BAD_PATH_PATTERN = re.compile(r'/(category|tag|page)/', re.IGNORECASE)

def is_valid_url(url):
    if not url:
        logger.debug(f"URL '{url}' is invalid: Empty URL") # Added Debug Log
        return False
    parsed = urlparse(url)
    if not parsed.netloc:
        logger.debug(f"URL '{url}' is invalid: No netloc") # Added Debug Log
        return False
    if parsed.netloc in GENERIC_DOMAINS:
        logger.debug(f"URL '{url}' is invalid: Generic domain: {parsed.netloc}") # Added Debug Log
        return False
    if BAD_PATH_PATTERN.search(url):
        logger.debug(f"URL '{url}' is invalid: Bad path pattern") # Added Debug Log
        return False
    if parsed.netloc.endswith(".pdf"):
        logger.debug(f"URL '{url}' is invalid: PDF file") # Added Debug Log
        return False
    if parsed.netloc.endswith((".jpg", ".jpeg", ".png", ".gif")):
        logger.debug(f"URL '{url}' is invalid: Image file") # Added Debug Log
        return False

    blocked_domain_endings = (
        "tripadvisor.com",
        "houfy.com",
        "airbnb.com", "vrbo.com", "booking.com", "yelp.com", "apartments.com",
        "allpropertymanagement.com", "airdna.com", "airdna.co", "instagram.com",
        "facebook.com", "vacasa.com", "cozycozy.com", "kayak.com", "expedia.com",
        "news-outlet.com", "social-media.com",
        "youtube.com", "hometogo.com", "www.hometogo.com", "whimstay.com", "www.whimstay.com", "avantstay.com",
    ) # Use tuple for endswith check

    if parsed.netloc.lower().endswith(blocked_domain_endings):
        logger.debug(f"URL '{url}' is invalid: Blocked domain ending: {parsed.netloc}") # Added Debug Log
        return False

    if parsed.netloc.lower() == "google.com" and "/travel" in parsed.path.lower():
        logger.debug(f"URL '{url}' is invalid: Google Travel URL") # Added Debug Log
        return False

    logger.debug(f"URL '{url}' is valid") # Added Debug Log
    return True


CAPTCHA_SITE_KEY = "6LcDC4AqAAAAAHV3xk9CtSubWOrCezicg7ze-rja"

PROXY_POOL = [
    f"{os.getenv('PROXY_USER_1')}:{os.getenv('PROXY_PASS_1')}@pr.oxylabs.io:7777",
    f"{os.getenv('PROXY_USER_2')}:{os.getenv('PROXY_PASS_2')}@gate.smartproxy.com:7000",
    f"{os.getenv('PROXY_USER_3')}:{os.getenv('PROXY_PASS_3')}@brd.superproxy.io:33335",
]

# Enhanced Proxy Validation using Regex
PROXY_REGEX = r'^[\w-]+(:[\w-]+)?@[\w.-]+:\d+$'  # Allows URL-encoded credentials
if not PROXY_POOL or not all(re.match(PROXY_REGEX, proxy) for proxy in PROXY_POOL):
    logger.warning(
        "PROXY_POOL in utils.py is not configured correctly. Please use the format 'user:password@host:port'. CAPTCHA proxy rotation and proxy usage may NOT work without a valid PROXY_POOL.")
else:
    logger.info(f"PROXY_POOL configured with {len(PROXY_POOL)} proxies.")


async def solve_captcha(page_url, current_proxy=None, rotation_attempts=0):
    MAX_CAPTCHA_ATTEMPTS = 3  # Must be defined at top
    solver = TwoCaptcha(os.getenv('TWOCAPTCHA_API_KEY'))
    if not os.getenv('TWOCAPTCHA_API_KEY'):
        logger.warning("TWOCAPTCHA_API_KEY not set, CAPTCHA solving disabled (solve_captcha function called).")
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
        result = await asyncio.to_thread(
            solver.recaptcha,
            apiKey=os.getenv('TWOCAPTCHA_API_KEY'),
            sitekey=CAPTCHA_SITE_KEY,
            url=page_url,
            version='v3',
            action='verify',
            min_score=0.5,  # Correct: min_score set to 0.5
            attempts=MAX_CAPTCHA_ATTEMPTS,
            **kwargs
        )
        if result and 'code' in result:
            logger.info(
                f"[CAPTCHA SOLVED] reCAPTCHA v3 - Score: {result.get('score', 'N/A')} | Action: verify {f'| Proxy: {current_proxy}' if current_proxy else ''}")

            score = result.get('score')
            if not result or result.get('score') is None:
                logger.warning("CAPTCHA score missing - rejecting solution")
                logger.debug(f"Full 2Captcha Response (Score Missing): {result}") # Log full response
                return None
            elif score < 0.5:
                logger.warning(f"Low CAPTCHA confidence score: {score}. Rotating proxy.")
                if rotation_attempts < MAX_CAPTCHA_ATTEMPTS:
                    rotation_attempts += 1
                    if PROXY_POOL and len(PROXY_POOL) > 1:
                        rotated_proxy = random.choice(PROXY_POOL)
                        if rotated_proxy == current_proxy:
                            rotated_proxy = random.choice([p for p in PROXY_POOL if p != current_proxy])
                        logger.info(f"Retrying CAPTCHA solve with rotated proxy (Attempt {rotation_attempts}): {rotated_proxy}")
                        return await solve_captcha(page_url, rotated_proxy, rotation_attempts=rotation_attempts)
                    else:
                        logger.warning(
                            "PROXY_POOL not configured or only one proxy available, cannot rotate proxy for low CAPTCHA score.")
                else:
                    logger.warning("Max rotation attempts reached for low score. CAPTCHA solve likely to fail.")
                    return None
            return result['code']
        else:
            logger.error("2Captcha solve failed, no code returned")
            if rotation_attempts >= MAX_CAPTCHA_ATTEMPTS:
                raise Exception("Max CAPTCHA attempts reached - no code returned")
            return None

    except Exception as e:
        logger.error(f"CAPTCHA solve failed due to exception: {str(e)[:200]}")
        if rotation_attempts >= MAX_CAPTCHA_ATTEMPTS:
            raise Exception("Max CAPTCHA attempts reached - exception during solve")
        return None


def enhanced_data_parsing(soup, text_content):
    """Additional parsing for key property management data"""
    data = {}

    # 1. Social Media Links
    social_links = []
    for platform in ["facebook", "twitter", "linkedin", "instagram"]:
        links = soup.select(f'a[href*="{platform}.com"]')
        social_links = []
        for link in links:
            if link.has_attr('href'):
                social_links.append(link['href'])
        data["social_media"] = list(set(social_links))

    # 2. Property Count Estimation
    prop_count = 0
    number_words = re.findall(r'\b\d+\b', text_content)
    for num in number_words[:100]:  # Check first 100 numbers
        if 10 < int(num) < 10000:  # Reasonable property count range
            prop_count = int(num)
            break
    data["estimated_properties"] = prop_count

    # 3. Service Areas
    service_areas = []
    location_keywords = ["serving", "areas", "cover", "locations"]
    for elem in soup.find_all(['p', 'div', 'section']):
        text = elem.get_text().lower()
        if any(kw in text for kw in location_keywords):
            service_areas.extend(re.findall(r'\b[A-Z][a-z]+(?: [A-Z][a-z]+)*\b', elem.get_text()))
    data["service_areas"] = list(set(service_areas))

    # 4. Management Software Detection
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
    """Analyzes website text using Ollama LLM to categorize the business."""
    try:
        # Updated Prompt - Classifier Prompt Fix
        prompt = f"""Analyze if website has anything to do with Vacation Rentals, Property Management, or Airbnbs. Key indicators:
1. Anything about rentals
2. a search bar where you can search for a property
3. anything about property management
4. anything about airbnbs
5. anything about vacation rentals
6. anything about real estate
7. anything to do about Airbnbs
8. anything to do with Vacation Rentals
9. anything to do with Property Management

Website Company Name: {company_name}

Website Text Content:
{website_text[:3000]}... (truncated for brevity)

Based on these indicators, categorize the website as 'Rental Property Manager/Host' or 'Not Rental Related'.

Label as 'Rental Property Manager/Host' if 1+ indicators are suggested in the text, otherwise label as 'Not Rental Related'.  Just respond with ONLY one of these categories: 'Rental Property Manager/Host', 'Not Rental Related'. Do not add any extra text."""

        response = await ollama.chat(
            model=model_name,
            messages=[{'role': 'user', 'content': prompt}]
        )
        category_prediction = response.message.content.strip()
        logger.debug(f"[Ollama Analysis] Company: {company_name}, Predicted Category: {category_prediction}")
        return category_prediction

    except Exception as e:
        logger.error(f"Error during Ollama analysis for {company_name}: {e}")
        return None


def detect_license_info(soup):
    """Extract license numbers from website content."""
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
    """Decode common email obfuscation patterns."""
    text = re.sub(r'\b(\w+)\s*\[?@\(?AT\)?\]?\s*(\w+)\s*\[?\.\(?DOT\)?\]?\s*(\w+)\b',
                r'\1@\2.\3', text, flags=re.IGNORECASE)
    text = re.sub(r'\b(\w+)\s*\(\s*at\s*\)\s*(\w+)\s*\(\s*dot\s*\)\s*(\w+)\b',
                r'\1@\2.\3', text, flags=re.IGNORECASE)
    text = unescape(text)
    return text


ua = UserAgent()
def get_random_user_agent():
    return ua.random


@retry(wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5), retry_error_callback=lambda retry_state: logger.warning(f"Retry attempt {retry_state.attempt_number} after error: {retry_state.outcome.exception()}"))
async def fetch_url(session, url, proxy_pool=None, retry_state=None): # Modified to accept proxy_pool
    """Asynchronously fetches a URL with proxy support and retry logic using tenacity."""
    proxy_url_to_use = None
    current_proxy = None # Initialize current_proxy

    if os.getenv("USE_PROXY", "False").lower() == "true":
        if proxy_pool: # Use provided proxy pool if available (e.g., PROXY_POOL)
            current_proxy = random.choice(proxy_pool) # Select proxy from pool
            proxy_url_to_use = f"http://{current_proxy}"
            logger.debug(f"Fetching {url} using proxy from PROXY_POOL: {current_proxy}")
        elif os.getenv('PROXY_HOST') and os.getenv('PROXY_PORT') and os.getenv('PROXY_USER') and os.getenv('PROXY_PASSWORD'): # Fallback to individual proxy settings
            proxy_url_to_use = f"http://{os.getenv('PROXY_USER')}:{os.getenv('PROXY_PASSWORD')}@{os.getenv('PROXY_HOST')}:{os.getenv('PROXY_PORT')}"
            logger.debug(f"Fetching {url} using configured proxy.")
        else:
            logger.warning("Proxy settings enabled but incomplete. Not using proxy for this request.")

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
        async with session.get(url, url, timeout=25, proxy=proxy_url_to_use, headers=headers, ssl=False) as response: # Corrected url=url to just url
            logger.debug(f"Response status for {url} (Attempt {attempt_number}): {response.status}")

            if response.status == 429: # Handle 429 Too Many Requests
                retry_after = int(response.headers.get('Retry-After', 10)) # Get Retry-After header, default 10 secs
                logger.warning(f"Rate limited (429) for {url}. Waiting {retry_after} seconds before retry.")
                await asyncio.sleep(retry_after) # Wait before retrying
                raise aiohttp.ClientError(f"Rate limit 429, retry after {retry_after} seconds") # Raise to trigger retry
            elif response.status == 403: # Handle 403 Forbidden
                logger.warning(f"Forbidden (403) for {url} with proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}. Blacklisting proxy (if used).")
                if current_proxy and proxy_pool and current_proxy in proxy_pool:
                    proxy_pool.remove(current_proxy) # Remove the problematic proxy
                    logger.info(f"Proxy {current_proxy} removed from pool. Remaining proxies: {len(proxy_pool)}")
                raise aiohttp.ClientError(f"Forbidden 403, blacklisted proxy if used") # Raise to trigger retry (and proxy rotation if retrying)


            response.raise_for_status()
            return await response.text()
    except aiohttp.ClientTimeout as e:
        logger.warning(f"Request Timeout for {url} (Attempt {attempt_number}): {e} | Proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}")
        raise e
    except aiohttp.ServerDisconnectedError as e:
        logger.warning(f"Server Disconnected for {url} (Attempt {attempt_number}): {e} | Proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}")
        raise e
    except aiohttp.TooManyRedirects as e:
        logger.warning(f"Too Many Redirects for {url} (Attempt {attempt_number}): {e} | Proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}")
        logger.warning(f"URL likely redirecting in a loop or exceeding redirect limit: {url}") # More descriptive warning
        return None # Or decide not to retry for redirect loops
    except aiohttp.ClientError as e: # Catch general ClientError last
        logger.warning(f"aiohttp ClientError for {url} (Attempt {attempt_number}): {e} | Proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}")
        raise e
    except asyncio.TimeoutError as e:
        logger.warning(f"Async request timeout for {url} (Attempt {attempt_number}): {e} | Proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}")
        raise e


async def extract_contact_info(session, url, context, proxy_pool=None): # Modified to accept proxy_pool
    """
    Asynchronously crawls a URL and extracts contact information with enhancements.
    """
    logger.debug(f"Entering extract_contact_info for url: {url} with context: {context}")
    city = context.get("city", "N/A")
    search_term = context.get("term", "N/A")

    company_name = "N/A"
    email_addresses = []
    phone_numbers = []
    physical_address = "N/A"
    relevance_score = 0.0
    llm_category = "Not Rental Related"

    extracted_data = {
        "City": city,
        "Search Term": search_term,
        "Company Name": company_name,
        "website_url": url,
        "Email Addresses": email_addresses,
        "Phone Numbers": phone_numbers,
        "Physical Address": physical_address,
        "Relevance Score": relevance_score,
        "social_media": [],
        "estimated_properties": 0,
        "service_areas": [],
        "management_software": [],
        "license_numbers": [],
        "llm_category": llm_category
    }


    try:
        # Pass proxy_pool to fetch_url
        text = await fetch_url(session, url, proxy_pool=proxy_pool)
        if text is None:
            logger.warning(f"Failed to fetch content from {url} (fetch_url returned None). Extraction skipped.")
            return None

        soup = BeautifulSoup(text, 'html.parser')
        text_content = soup.get_text(separator=' ', strip=True).lower()

        logger.debug(f"Processing URL for phone/email extraction: {url}") # Added Debug Log
        logger.debug(f"Extracted Text Content (first 500 chars): {text_content[:500]}...") # Added Debug Log

        if "captcha" in text_content or "verify you are human" in text_content:
            logger.warning(f"Basic CAPTCHA detection triggered at {url}. Attempting 2Captcha solve...")
            captcha_proxy = random.choice(proxy_pool) if proxy_pool else None # Use proxy from pool for CAPTCHA
            captcha_code = await solve_captcha(url, captcha_proxy) # Use utils.solve_captcha - Corrected Call
            if captcha_code:
                logger.info(
                    f"2Captcha solved successfully for {url}. CAPTCHA code: {captcha_code} {f'| Proxy: {captcha_proxy}' if captcha_proxy else ''}")
            else:
                logger.warning(f"2Captcha solve failed for {url}. Skipping extraction.")
                return None

        # --- Corrected Company Name Detection ---
        company_name = (
            soup.find('h1').get_text(strip=True)
            if soup.find('h1') else None
        ) or (
            soup.title.get_text().split('|')[0].strip()
            if soup.title else None
        ) or (
            urlparse(url).netloc.replace('www.', '').split('.')[0].title()
        ) or "N/A"
        extracted_data["Company Name"] = company_name
        logger.debug(f"Detected Company Name: {company_name} for URL: {url}")

        # --- Improved Phone Regex ---
        # More comprehensive regex to catch various formats
        phone_pattern = r'''
            (?x)            # Enable verbose regex for better readability
            \b              # Word boundary
            (?:             # Non-capturing group for optional country code
              \+?1          # Optional +1 country code
              [-.\s]?       # Optional separator
            )?
            \(?             # Optional opening parenthesis
            \d{3}           # Area code (3 digits)
            \)?             # Optional closing parenthesis
            [-.\s]?         # Optional separator
            \d{3}           # Exchange code (3 digits)
            [-.\s]?         # Optional separator
            \d{4}           # Subscriber number (4 digits)
            \b              # Word boundary
            (?:             # Non-capturing group for extensions (optional)
              \s*           # Optional whitespace
              (?:           # Non-capturing group for extension markers
                ext\.?        # "ext" or "ext."
              | extension   # "extension"
              | x           # "x"
              )
              \s*           # Optional whitespace
              \d+           # Extension number (1 or more digits)
            )?
        '''
        phone_matches = set(re.findall(phone_pattern, text, re.VERBOSE | re.IGNORECASE)) # Added re.VERBOSE and re.IGNORECASE
        phone_numbers = [phone.strip() for phone in phone_matches]
        extracted_data["Phone Numbers"] = phone_numbers
        logger.debug(f"Phone Numbers Found: {phone_numbers}") # Added Debug Log

        email_matches = set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", decode_emails(text)))
        email_addresses = [email.strip() for email in email_matches]
        extracted_data["Email Addresses"] = email_addresses
        logger.debug(f"Email Addresses Found: {email_addresses}") # Added Debug Log

        address_element = soup.find('address')
        physical_address = address_element.get_text(separator=', ', strip=True) if address_element else "N/A"
        extracted_data["Physical Address"] = physical_address

        # Enhanced Data Parsing (Social Media, Property Count, etc.)
        enhanced_data = enhanced_data_parsing(soup, text_content)
        extracted_data.update(enhanced_data)

        license_numbers = detect_license_info(soup)
        extracted_data["license_numbers"] = license_numbers

        # --- Placeholder for Relevance Score and Ollama Analysis (To be implemented later) ---
        extracted_data["Relevance Score"] = relevance_score # Placeholder - Implement Relevance Score Logic
        # extracted_data["llm_category"] = await analyze_with_ollama(company_name, text) # Implement Ollama Analysis

        try:
            validated_data = PropertyManagerData(**extracted_data)
            return validated_data.dict()
        except Exception as e_validation:
            logger.error(f"Data validation failed for {url}: {e_validation}")
            logger.debug(f"Data that failed validation: {extracted_data}")
            return None

    except aiohttp.ClientError as e:
        logger.error(f"Request error crawling {url}: {e}")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error in extract_contact_info for {url}: {e}")
        return None