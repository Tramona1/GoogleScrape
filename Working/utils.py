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
from fake_useragent import UserAgent
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from urllib.parse import urlparse
import struct  # Add this import at the top with other imports

MAX_CAPTCHA_ATTEMPTS = 3
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
        "tripadvisor.com",
        "houfy.com",
        "airbnb.com", "vrbo.com", "booking.com", "yelp.com", "apartments.com",
        "allpropertymanagement.com", "airdna.com", "airdna.co", "instagram.com",
        "facebook.com", "vacasa.com", "cozycozy.com", "kayak.com", "expedia.com",
        "news-outlet.com", "social-media.com",
        "youtube.com", "hometogo.com", "www.hometogo.com", "whimstay.com", "www.whimstay.com", "avantstay.com",
    )

    if parsed.netloc.lower().endswith(blocked_domain_endings):
        logger.debug(f"URL '{url}' is invalid: Blocked domain ending: {parsed.netloc}")
        return False

    if parsed.netloc.lower() == "google.com" and "/travel" in parsed.path.lower():
        logger.debug(f"URL '{url}' is invalid: Google Travel URL")
        return False

    logger.debug(f"URL '{url}' is valid")
    return True


CAPTCHA_SITE_KEY = "6LcDC4AqAAAAAHV3xk9CtSubWOrCezicg7ze-rja"

PROXY_POOL = [
    f"{os.getenv('PROXY_USER_1')}:{os.getenv('PROXY_PASS_1')}@pr.oxylabs.io:7777",
    f"{os.getenv('PROXY_USER_2')}:{os.getenv('PROXY_PASS_2')}@gate.smartproxy.com:7000",
    f"{os.getenv('PROXY_USER_3')}:{os.getenv('PROXY_PASS_3')}@brd.superproxy.io:33335",
]

PROXY_REGEX = r'^([\w-]+(:[\w-]+)?@)?[\d.a-z-]+:\d+$'  # Allow IPs/hostnames
if not PROXY_POOL or not all(re.match(PROXY_REGEX, proxy) for proxy in PROXY_POOL):
    logger.warning(
        "PROXY_POOL in utils.py is not configured correctly. Please use the format 'user:password@host:port'. CAPTCHA proxy rotation and proxy usage may NOT work without a valid PROXY_POOL.")
else:
    logger.info(f"PROXY_POOL configured with {len(PROXY_POOL)} proxies.")


async def solve_captcha(page_url, current_proxy=None, rotation_attempts=0):
    MAX_CAPTCHA_ATTEMPTS = 3
    solver = TwoCaptcha(os.getenv('TWOCAPTCHA_API_KEY'))
    if not os.getenv('TWOCAPTCHA_API_KEY'):
        logger.warning("TWOCAPTCHA_API_KEY not set, CAPTCHA solving disabled.")
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
            min_score=0.5,
            attempts=MAX_CAPTCHA_ATTEMPTS,
            **kwargs
        )
        if result and 'code' in result:
            logger.debug(f"Full 2Captcha Response: {result}") # Log full response for debugging
            # Validate score exists and meets threshold
            if not result.get('score'): # Check if score is present
                logger.warning("CAPTCHA score missing - rejecting solution")
                return None
            elif result['score'] < 0.5:  # Threshold check
                logger.warning(f"Low CAPTCHA confidence score: {result['score']}. Rotating proxy.")
                if rotation_attempts < MAX_CAPTCHA_ATTEMPTS:
                    if PROXY_POOL and len(PROXY_POOL) > 1:
                        remaining_proxies = [p for p in PROXY_POOL if p != current_proxy] # Proxy rotation fix
                        rotated_proxy = random.choice(remaining_proxies) if remaining_proxies else None # Proxy rotation fix
                        if rotated_proxy:
                            logger.info(f"Retrying CAPTCHA solve with rotated proxy (Attempt {rotation_attempts + 1}/{MAX_CAPTCHA_ATTEMPTS}): {rotated_proxy}") # f-string fix
                            return await solve_captcha(page_url, rotated_proxy, rotation_attempts=rotation_attempts + 1) # Recursive call with rotated proxy and incremented attempts
                        else:
                            logger.warning("No proxies left to rotate to after CAPTCHA failure.")
                            return None
                    else:
                        logger.warning("PROXY_POOL not configured or exhausted, cannot rotate proxy.")
                        return None
                else:
                     logger.error(f"Max CAPTCHA rotation attempts reached with low score. Solve likely to fail.")
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
    social_links = [] # Initialize social_links outside the loop - Social media parsing bug fix
    for platform in ["facebook", "twitter", "linkedin", "instagram"]:
        links = soup.select(f'a[href*="{platform}.com"]')
        social_links.extend([link['href'] for link in links if link.has_attr('href')]) # Extend instead of reset
    data["social_media"] = list(set(social_links))

    # 2. Property Count Estimation
    prop_count = 0
    number_words = re.findall(r'\b\d+\b', text_content)
    for num in number_words[:100]:
        if 10 < int(num) < 10000:
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
        # Optimized Prompt - More focused on vacation rental management indicators
        prompt = f"""Analyze if website is related to VACATION RENTAL MANAGEMENT. Look for these indicators:

- Property management services for vacation rentals
- Mentions of 'property management fees' or 'management agreements' for vacation rentals
- Features like 'cleaning checklists', 'owner portal access', 'availability calendars' geared towards vacation rentals
- References to 'STR license numbers' or vacation rental regulations
- Focus on managing short-term/vacation rentals for property owners, NOT long-term residential rentals.

Website Company Name: {company_name}

Website Text Content:
{website_text[:3000]}... (truncated for brevity)

Based on the presence of these VACATION RENTAL MANAGEMENT indicators, categorize the website as:
'Vacation Rental Property Manager' OR 'Not Vacation Rental Related'.

Just respond with ONLY ONE of these categories. Do not add any extra text."""


        response = await asyncio.to_thread( # Async Ollama fix
            ollama.chat,
            model=model_name,
            messages=[{'role': 'user', 'content': prompt}]
        )
        category_prediction = response['message']['content'].strip() # Access content from result
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


def validate_email(email):
    """Validate email format."""
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)

def validate_phone(phone):
    """Validate phone number format (basic)."""
    return re.match(r"^(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$", phone)


ua = UserAgent()
def get_random_user_agent():
    return ua.random


@retry(wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5), retry_error_callback=lambda retry_state: logger.warning(f"Retry attempt {retry_state.attempt_number} after error: {retry_state.outcome.exception()}"))
async def fetch_url(session, url, proxy_pool=None, retry_state=None):
    """Asynchronously fetches a URL with proxy support and retry logic using tenacity."""
    proxy_url_to_use = None
    current_proxy = None

    if os.getenv("USE_PROXY", "False").lower() == "true":
        if proxy_pool:
            current_proxy = random.choice(proxy_pool)
            proxy_url_to_use = f"http://{current_proxy}"
            logger.debug(f"Fetching {url} using proxy from PROXY_POOL: {current_proxy}")
        elif os.getenv('PROXY_HOST') and os.getenv('PROXY_PORT') and os.getenv('PROXY_USER') and os.getenv('PROXY_PASSWORD'):
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
        async with session.get(url, timeout=25, proxy=proxy_url_to_use, headers=headers, ssl=False) as response:
            logger.debug(f"Response status for {url} (Attempt {attempt_number}): {response.status}")

            if response.status == 429:
                retry_after = int(response.headers.get('Retry-After', 10))
                logger.warning(f"Rate limited (429) for {url}. Waiting {retry_after} seconds before retry.")
                await asyncio.sleep(retry_after)
                raise aiohttp.ClientError(f"Rate limit 429, retry after {retry_after} seconds")
            elif response.status == 403:
                logger.warning(f"Forbidden (403) for {url} with proxy: {proxy_url_to_use if proxy_url_to_use else 'No Proxy'}. Blacklisting proxy (if used).")
                if current_proxy and proxy_pool and current_proxy in proxy_pool:
                    proxy_pool.remove(current_proxy)
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


async def extract_contact_info(session, url, context, proxy_pool=None):
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
        "city": context.get("city", "N/A"), # Corrected keys to lowercase
        "search_term": context.get("term", "N/A"), # Corrected keys to lowercase
        "company_name": company_name, # Corrected keys to lowercase
        "website_url": url,
        "email_addresses": [],
        "phone_numbers": [],
        "physical_address": physical_address, # Corrected keys to lowercase
        "relevance_score": relevance_score, # Corrected keys to lowercase
        "social_media": [],
        "estimated_properties": 0,
        "service_areas": [],
        "management_software": [],
        "license_numbers": [],
        "llm_category": llm_category
    }

    try:
        text = await fetch_url(session, url, proxy_pool=proxy_pool)
        if text is None:
            logger.warning(f"Failed to fetch content from {url} (fetch_url returned None). Extraction skipped.")
            return None

        soup = BeautifulSoup(text, 'html.parser')
        text_content = soup.get_text(separator=' ', strip=True).lower()

        logger.debug(f"Processing URL for phone/email extraction: {url}")
        logger.debug(f"Extracted Text Content (first 500 chars): {text_content[:500]}...")

        if "captcha" in text_content or "verify you are human" in text_content:
            logger.warning(f"Basic CAPTCHA detection triggered at {url}. Attempting 2Captcha solve...")
            captcha_proxy = random.choice(proxy_pool) if proxy_pool else None
            captcha_code = await solve_captcha(url, captcha_proxy)
            if captcha_code:
                logger.info(
                    f"2Captcha solved successfully for {url}. CAPTCHA code: {captcha_code} {f'| Proxy: {captcha_proxy}' if captcha_proxy else ''}")
            else:
                logger.warning(f"2Captcha solve failed for {url}. Skipping extraction.")
                return None

        # --- Improved Company Name Detection ---
        company_name = (
            soup.find('h1').get_text(strip=True)
            if soup.find('h1') else None
        ) or (
            urlparse(url).netloc.split('.')[-2].title() if urlparse(url).netloc.split('.')[-2].title() else None # Domain-based fallback - Improved
        ) or (
            soup.title.get_text().split('|')[0].strip()
            if soup.title else None
        ) or (
            urlparse(url).netloc.replace('www.', '').split('.')[0].title()
        ) or "N/A"
        extracted_data["company_name"] = company_name # Corrected key to lowercase
        logger.debug(f"Detected Company Name: {company_name} for URL: {url}")
        logger.debug(f"Extracted City: {context.get('city', 'N/A')}") # Debug log for city
        logger.debug(f"Extracted Company: {company_name}") # Debug log for company name


        # --- Improved Phone Regex ---
        phone_pattern = r'(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}' # Enhanced phone regex
        phone_matches = set(re.findall(phone_pattern, text))
        # Data Validation - Validate and filter phone numbers
        validated_phones = [phone.strip() for phone in phone_matches if validate_phone(phone) and validate_phone(phone).group(0) not in ['', '1', '+1'] ] # Apply validation here + stronger filter
        phone_numbers = validated_phones
        extracted_data["phone_numbers"] = phone_numbers # Corrected key case
        logger.debug(f"Phone Numbers Found: {phone_numbers}")

        email_matches = set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", decode_emails(text)))
         # Data Validation - Validate and filter emails
        validated_emails = [email.strip() for email in email_matches if validate_email(email)] # Apply validation here
        email_addresses = validated_emails
        extracted_data["email_addresses"] = email_addresses # Corrected key case
        logger.debug(f"Email Addresses Found: {email_addresses}")

        address_element = soup.find('address')
        physical_address = address_element.get_text(separator=', ', strip=True) if address_element else "N/A"
        extracted_data["physical_address"] = physical_address # Corrected key to lowercase

        enhanced_data = enhanced_data_parsing(soup, text_content)
        extracted_data.update(enhanced_data)

        license_numbers = detect_license_info(soup)
        extracted_data["license_numbers"] = license_numbers

        extracted_data["relevance_score"] = relevance_score # Corrected key to lowercase
        extracted_data["llm_category"] = await analyze_with_ollama(company_name, text_content) # Corrected text argument


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