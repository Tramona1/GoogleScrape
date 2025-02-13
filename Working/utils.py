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
import ollama # Import Ollama library

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
    "airbnb.com", "vrbo.com", "booking.com",  # Vacation rental platforms
    "allpropertymanagement.com", # Property management directory
    "airdna.co", "airdna.com",  # AirDNA (data analytics for vacation rentals)
    "vacasa.com",
    "cozycozy.com",
    "kayak.com",
    "expedia.com",
    "news-outlet.com", # Placeholder for generic news outlets (you can refine this)
    "social-media.com", # Placeholder for generic social media sites (you can refine)
    "tripadvisor.com",
    "evolve.com",
    "homes-and-villas.marriott.com",
    "hometogo.com", # ADDED: Exclude hometogo.com
    "www.hometogo.com", # ADDED: Exclude www.hometogo.com (just to be sure)
    "whimstay.com", # ADDED: Exclude whimstay.com
    "www.whimstay.com", # ADDED: Exclude www.whimstay.com (just to be sure)
    "avantstay.com", # ADDED: Exclude avantstay.com - ADDED TO DO NOT SCRAPE LIST
    "houfy.com",      # ADDED: Exclude houfy.com - ADDED TO DO NOT SCRAPE LIST
}

BAD_PATH_PATTERN = re.compile(r'/(category|tag|page)/', re.IGNORECASE)  # Precompiled regex


def is_valid_url(url):
    """Filter out low-quality URLs and specific unwanted domains."""
    if not url:
        return False
    parsed = urlparse(url)
    if not parsed.netloc:
        return False
    if parsed.netloc in GENERIC_DOMAINS:
        return False
    if BAD_PATH_PATTERN.search(url):
        return False
    if parsed.netloc.endswith(".pdf"):
        return False
    if parsed.netloc.endswith(".jpg") or parsed.netloc.endswith(".jpeg") or parsed.netloc.endswith(".png") or parsed.netloc.endswith(".gif"):
        return False

    netloc_lower = parsed.netloc.lower()

    # Explicitly filter out tripadvisor.com here as well (redundancy, but good)
    if "tripadvisor.com" in netloc_lower:
        return False

    # More flexible filtering using "in" for categories
    if any(domain in netloc_lower for domain in [
        "airbnb.com", "vrbo.com", "booking.com", "yelp.com", "apartments.com",
        "allpropertymanagement.com", "airdna.com", "airdna.co", "instagram.com",
        "facebook.com", "vacasa.com", "cozycozy.com", "kayak.com", "expedia.com",
        "news-outlet.com", "social-media.com",
        ".org", # Consider adding .org blocking here if needed, but be careful
        "youtube.com",
    ]):
        return False

    # Block google.com/travel specifically - added condition
    if parsed.netloc.lower() == "google.com" and "/travel" in parsed.path.lower():
        return False

    return True


# def can_crawl(url, user_agent='PropertyManagerCrawler/1.3'):
#     """Checks robots.txt for crawling permission."""
#     try:
#         parsed_url = urlparse(url)
#         base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
#         robots_url = urljoin(base_url, '/robots.txt')
#         rp = urllib.robotparser.RobotFileParser()
#         rp.set_url(robots_url)
#         rp.read()
#         return rp.can_fetch(user_agent, url)
#     except Exception as e:
#         logger.warning(f"Robots.txt check error for {url}: {e}")
#         return True


CAPTCHA_SITE_KEY = "6LcDC4AqAAAAAHV3xk9CtSubWOrCezicg7ze-rja" # SITEKEY FOR walkervr.com - reCAPTCHA V3


async def solve_captcha(page_url):
    """Solves reCAPTCHA v3 using 2Captcha service."""
    solver = TwoCaptcha(os.getenv('TWOCAPTCHA_API_KEY'))
    if not os.getenv('TWOCAPTCHA_API_KEY'):
        logger.warning("TWOCAPTCHA_API_KEY not set, CAPTCHA solving disabled (solve_captcha function called).")
        return None

    balance = await asyncio.to_thread(solver.balance)
    if balance < 2:
        logger.error(f"2Captcha balance too low: ${balance}. CAPTCHA solving disabled.")
        return None

    try:
        result = await asyncio.to_thread(
            solver.recaptcha,
            apiKey=os.getenv('TWOCAPTCHA_API_KEY'),
            sitekey=CAPTCHA_SITE_KEY,
            url=page_url,
            version='v3',
            action='verify',
            min_score=0.7
        )
        if result and 'code' in result:
            logger.info(f"[CAPTCHA SOLVED] reCAPTCHA v3 - Score: {result.get('score', 'N/A')} | Action: verify")
            if result.get('score', 0) < 0.5:
                logger.warning(f"[LOW SCORE] reCAPTCHA v3 - Score: {result.get('score')} | Action: verify - Low score, might be flagged.")
            return result['code']
        else:
            logger.error(f"2Captcha solve failed, no code returned for {page_url}: {result}")
            return None
    except Exception as e:
        logger.error(f"CAPTCHA solve failed: {str(e)[:200]}")
        return None


def enhanced_data_parsing(soup, text_content, base_url):
    """Additional parsing for key property management data"""
    data = {}

    # 1. Social Media Links
    social_links = []
    for platform in ["facebook", "twitter", "linkedin", "instagram"]:
        links = soup.select(f'a[href*="{platform}.com"]')
        social_links.extend(link['href'] for link in links if link.has_attr('href'))
    data["social_media"] = list(set(social_links))

    # 2. Property Count Estimation
    prop_count = 0
    number_words = re.findall(r'\b\d+\b', text_content)
    for num in number_words[:100]:  # Check first 100 numbers
        if 10 < int(num) < 10000:   # Reasonable property count range
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
    license_numbers: List[str] = [] # Added for license info

    @validator('phone_numbers', each_item=True)
    def validate_phone_number(cls, phone):
        return phone.strip()


async def analyze_with_ollama(company_name, website_text, model_name='deepseek-r1:latest'):
    """Analyzes website text using Ollama LLM to categorize the business."""
    try:
        prompt = f"""You are analyzing a website to determine if it belongs to a vacation rental property management company.

        Website Company Name: {company_name}

        Website Text Content:
        ```
        {website_text[:3000]}... (truncated for brevity)
        ```

        Based on the company name and website text content, categorize the website into one of these categories:
        'Rental Property Manager/Host', 'Cleaning Service Provider (rental focused)', 'Real Estate Photography/Videography (rental focused)', or 'Not Rental Related'.

        Just respond with the category name. Choose ONLY from these categories: 'Rental Property Manager/Host', 'Cleaning Service Provider (rental focused)', 'Real Estate Photography/Videography (rental focused)', or 'Not Rental Related'. Do not add any extra text."""

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


async def extract_contact_info(session, url, context):
    """
    Asynchronously crawls a URL and extracts contact information with enhancements.
    """
    city = context.get("city", "N/A")
    search_term = context.get("term", "N/A")

    company_name = "N/A"
    email_addresses = []
    phone_numbers = []
    physical_address = "N/A"
    relevance_score = 0.0
    llm_category = "Not Rental Related" # Default LLM category

    extracted_data = {
        "City": city,
        "Search Term": search_term,
        "Company Name": company_name,
        "website_url": url, # IMPORTANT: Field name is "website_url" - matching Pydantic model
        "Email Addresses": email_addresses,
        "Phone Numbers": phone_numbers,
        "Physical Address": physical_address,
        "Relevance Score": relevance_score,
        "social_media": [],
        "estimated_properties": 0,
        "service_areas": [],
        "management_software": [],
        "license_numbers": [], # Initialize license_numbers in extracted_data
        "llm_category": llm_category # Initialize llm_category in extracted_data
    }

    # if not can_crawl(url): # COMMENT OUT THIS LINE - Disable robots.txt check
    #     logger.warning(f"Robots.txt disallowed crawling: {url}")
    #     return None # COMMENT OUT THIS LINE - Disable robots.txt check

    try:
        headers = {'User-Agent': 'PropertyManagerCrawler/1.3 (your-email@example.com)'} # User-Agent updated
        async with session.get(url, headers=headers, timeout=20) as response:
            response.raise_for_status()
            text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')
            text_content = soup.get_text(separator=' ', strip=True).lower()
            base_url = url # Get base URL for urljoin in enhanced_data_parsing

            if "captcha" in text_content or "verify you are human" in text_content:
                logger.warning(f"Basic CAPTCHA detection triggered at {url}. Attempting 2Captcha solve...")
                captcha_code = await solve_captcha(url)
                if captcha_code:
                    logger.info(f"2Captcha solved successfully for {url}. CAPTCHA code: {captcha_code}")
                else:
                    logger.warning(f"2Captcha solve failed for {url}. Skipping extraction.")
                    return None

            # --- Content Relevance Scoring ---
            service_keywords = ["vacation rental property management", "short term rental management", "airbnb property management", "vrbo property management", "holiday rental management"]
            keyword_hits = sum(1 for kw in service_keywords if kw in text_content)
            relevance_score = keyword_hits / len(service_keywords) if service_keywords else 0.0
            extracted_data["Relevance Score"] = relevance_score

            if relevance_score < 0.1:
                logger.info(f"Low relevance score ({relevance_score:.2f}) for: {url} - may not be highly relevant.")
            elif relevance_score > 0.2: # Example threshold to trigger LLM - adjust as needed
                llm_category_prediction = await analyze_with_ollama(company_name, text_content) # Call LLM for higher relevance sites
                if llm_category_prediction:
                    extracted_data["llm_category"] = llm_category_prediction # Store LLM category


            # 1. Company Name Extraction
            title_tag = soup.select_one('head title')
            og_title_tag = soup.find("meta", property="og:title")
            if og_title_tag and og_title_tag.get("content"):
                company_name = og_title_tag["content"].strip()
            elif title_tag:
                company_name = title_tag.text.strip()
            else:
                company_name = url
            extracted_data["Company Name"] = company_name

            # 2. Email Addresses Extraction
            email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
            generic_email_domains = ["info", "sales", "support", "contact", "admin", "noreply"]
            all_email_matches = re.findall(email_pattern, text)
            email_list = []
            for email in all_email_matches:
                if not any(generic in email.lower() for generic in generic_email_domains) and "@" in email and "." in email.split('@')[1]:
                    email_list.append(email)
            extracted_data["Email Addresses"] = list(set(email_list))

            # 3. Phone Numbers Extraction
            phone_pattern = r"(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}"
            phone_matches = re.findall(phone_pattern, text)
            extracted_data["Phone Numbers"] = list(set(phone_matches))

            # 4. Physical Address Detection
            address_div = soup.find("div", itemprop="address", itemscope="", itemtype="http://schema.org/PostalAddress")
            if address_div:
                try:
                    address_json = json.loads(address_div.encode_contents().decode('utf-8').replace('<br/>', r'\n').replace('<br>', r'\n'))
                    physical_address = json.dumps(address_json, indent=2)
                except json.JSONDecodeError:
                    physical_address = address_div.get_text(separator='\n', strip=True)
            else:
                footer = soup.select_one('footer')
                if footer:
                    address_candidate = footer.get_text(separator=' ', strip=True)
                    extracted_data["Physical Address"] = address_candidate[:250] + "..." if len(address_candidate) > 250 else address_candidate
                else:
                    extracted_data["Physical Address"] = "N/A - No schema.org or footer address found"

            # 5. Crawl for Contact Page
            contact_page_links = []
            for link in soup.select('a[href*="contact"], a[href*="contact-us"], a[href*="get-in-touch"], a[href*="contactus"]'):
                href = link.get('href')
                contact_page_url = urljoin(url, href)
                contact_page_links.append(contact_page_url)

            for contact_url in set(contact_page_links):
                if not contact_url.startswith('http'):
                    continue
                try:
                    async with session.get(contact_url, headers=headers, timeout=15) as contact_response:
                        contact_response.raise_for_status()
                        contact_text = await contact_response.text()
                        contact_soup = BeautifulSoup(contact_text, 'html.parser')

                        contact_email_matches = re.findall(email_pattern, contact_text)
                        for email in contact_email_matches:
                            if not any(generic in email.lower() for generic in generic_email_domains) and "@" in email and "." in email.split('@')[1]:
                                email_list.append(email)
                        extracted_data["Email Addresses"] = list(set(email_list))

                        contact_phone_matches = re.findall(phone_pattern, contact_text)
                        phone_numbers.extend(contact_phone_matches)
                        extracted_data["Phone Numbers"] = list(set(phone_numbers))

                except aiohttp.ClientError as e_contact:
                    logger.warning(f"  Error accessing contact page {contact_url}: {e_contact}")

            # --- Enhanced Data Parsing ---
            enhanced_data = enhanced_data_parsing(soup, text_content, base_url) # Pass base_url here
            extracted_data.update(enhanced_data)

            # --- Data Enrichment ---
            license_numbers = detect_license_info(soup)
            extracted_data["license_numbers"] = license_numbers


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