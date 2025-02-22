import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re # 1. Add Phone Normalization - IMPORT RE
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
    conlist
)
from typing import List, Optional
import ollama
import html # 2. Implement HTML Entity Decoding - IMPORT HTML
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
import phonenumbers
from email_validator import validate_email, EmailNotValidError # NEW IMPORT - Import email_validator library

# REMOVE THIS LINE FROM UTILS.PY:
# from crawler import decode_html_entities, normalize_phone # FIX 6: Phone Number Parsing - Import phonenumbers

MAX_CAPTCHA_ATTEMPTS = 3
logger = logging.getLogger(__name__)

GENERIC_DOMAINS = {
    "orbitz.com",
    "blogspot.com",
    "google.com",
    "facebook.com",
    "youtube.com",
    "twitter.com",
    "linkedin.com",
    "instagram.com",
    "pinterest.com",
    "yelp.com",
    "tripadvisor.com",
    "reddit.com",
    "amazon.com",
    "ebay.com",
    "indeed.com",
    "glassdoor.com",
    "zillow.com",
    "realtor.com",
    "apartments.com",
    "booking.com",
    "allpropertymanagement.com",
    "airdna.co",
    "airdna.com",
    "vacasa.com",
    "cozycozy.com",
    "kayak.com",
    "expedia.com",
    "news-outlet.com",
    "social_media.com",
    "evolve.com",
    "airbnb.com",
    "vrbo.com",
    "homes-and-villas.marriott.com",
    "hometogo.com",
    "hometogo.com",
    "whimstay.com",
    "whimstay.com",
    "avantstay.com",
    "houfy.com",
    "redawning.com",
    "corporatehousing.com",
    "corporatehousing.com",
    "onefinestay.com",
    "theblueground.com",
    "awning.com",
    "airconcierge.net",
    "fiscalnote.com",
    "news.com",
    "investopedia.com",
    "bankrate.com",
    "bloomberg.com",
    "hotpads.com",
    "zumper.com",
    "homes.com",
    "redfin.com",
    "apartmentfinder.com",
    "forrent.com",
    "airbtics.com",
    "furnishedfinder.com",
    "corporatehousingbyowner.com",
    "zumperrentals.com",
    "corporatehousingbyowner.com",
    "wimdu.com",
    "quora.com",
    "airconcierge.net",
    "staysophari.com",
    "windermere.com",
    "zipcar.com",
    "awning.com",
    "tesla.com",
    "geekwire.com",
    "skyrun.com",
    "plumguide.com",
    "wander.com",
    "itrip.net",
    "theblueground.com",
    "wimdu.com",
    "tiktok.com",
    "yahoo.com",
    "foxla.com",
    "latimes.com",
    "biggerpockets.com", # Added biggerpockets.com
    "marriott.com", # Added marriott.com
    "casamundo.com", # Added casamundo.com
    "hichee.com", # Added hichee.com
    "newswire.com",
    "avalara.com",
    "attorneysecrets.com",
    "hellolanding.com",
    "psychologytoday.com",
    "corporates.com",
    "businessinsider.com",
    "mashvisor.com",
    "padmapper.com",
    "byowner.com",
    "hotelscombined.com",
    "claz.org",
    "paahq.com",
    "trip101.com",
    "keystonenewsroom.com",
    "hotelscombined.com",
    "www.hotelscombined.com",
    "patch.com",
    "ocregister.com",
    "nancyfornewport.com",
    "heritagelawllp.com",
    "newportbeachindy.com",
    "ocrealtors.org",
    "steadily.com",
    "valiaoc.com",
    "flipkey.com",
    "obrag.org",
    "priorityonesd.com",
    "missionbeach.com",
    "sandiegoshorttermrentals.com",
    "independent.com",
    "noozhawk.com",
    "seacliffsb.com",
    "sandiegouniontribune.com",
    "swellproperty.com",
    "voiceofsandiego.org",
    "crestmontrealty.com",
    "sandiegocoastrentals.com",
    "ashleypetersonlaw.com",
    "socalrha.org",
    "sacbee.com",
    "sacramentocityexpress.com",
    "enewspaper.latimes.com",
    "lamag.com",
    "laprogressive.com",
    "keepneighborhoodsfirst.org",
    "nbclosangeles.com",
    "cd13.lacity.gov",
    "losangelespropertymanagementgroup.com",
    "culvercity.org",
    "abc7.com",
    "unitehere11.org",
    "fiorelaw.com",
    "laist.com",
    "therealdeal.com",
    "planning.lacounty.gov",
    "propublica.org",
    "pstribune.com",
    "desertsun.com",
    "ecode360.com",
    "airhostsforum.com",
    "longbeachin.org",
    "audacy.com",
    "newsday.com",
    "change.org",
    "nwpb.org",
    "the-independent.co",
    "mybelmontheights.org",
    "presstelegram.com",
    "lbpost.com",
    "longbeachwa.gov",
    "discoverourcoast.com",
    "northcoastcurrent.com",
    "bnbcalc.com",
    "nextdoor.com", # Added nextdoor.com
    "zola.com", # Added zola.com
    "tripinn.com", # Added tripinn.com
    "bluepillow.com", # Added bluepillow.com
    "perfectforweddings.com", # Added perfectforweddings.com
    "momondo.com", # Added momondo.com
    "hotels.com", # Added hotels.com
    "hotelscombined.com", # Added hotelscombined.com
    "www.hotelscombined.com", # Added www.hotelscombined.com - although redundant, kept for clarity if user had it explicitly
    "holidaylettings.co.uk", # Added holidaylettings.co.uk
    "www.holidaylettings.co.uk", # Added www.holidaylettings.co.uk - redundant but kept if user had it.
    "vacatia.com", # Added vacatia.com
    "home-to-go.ca", # Added home-to-go.ca
    "www.home-to-go.ca", # Added www.home-to-go.ca - redundant but kept if user had it.
    "get-rates.com", # Added get-rates.com
    "www.get-rates.com", # Added www.get-rates.com - redundant but kept if user had it.
    "momondo.ca", # Added momondo.ca
    "www.momondo.ca", # Added www.momondo.ca - redundant but kept if user had it.
    "traveloka.com", # Added traveloka.com
    "www.traveloka.com", # Added www.traveloka.com - redundant but kept if user had it.
    "traveloka.com.au", # Added traveloka.com.au
    "www.traveloka.com.au", # Added www.traveloka.com.au - redundant but kept if user had it.
    "traveloka.com.en-th", # Added traveloka.com.en-th
    "www.traveloka.com.en-th", # Added www.traveloka.com.en-th - redundant but kept if user had it.
    "booked.net", # Added booked.net
    "www.booked.net", # Added www.booked.net - redundant but kept if user had it.
    "perfectforweddings.com", # Added perfectforweddings.com - repeated entry, removed one.
    "visitsacramento.com", # Added visitsacramento.com
    "www.visitsacramento.com", # Added www.visitsacramento.com - redundant but kept if user had it.
    "visitsyv.com", # Added visitsyv.com
    "www.visitsyv.com", # Added www.visitsyv.com - redundant but kept if user had it.
    "visitsantabarbara.com", # Added visitsantabarbara.com - assuming typo in original data and meant visitsantabarbara.com
    "www.visitsantabarbara.com", # Added www.visitsantabarbara.com - redundant but kept if user had it.
    "visitsantacruz.com", # Assuming visitsantacruz.com was intended, but not in the list. If it appears, add it.
    "www.visitsantacruz.com", # Assuming www.visitsantacruz.com was intended, but not in the list. If it appears, add it.
    "visitsanjuancapistrano.com", # Assuming visitsanjuancapistrano.com was intended, but not in the list. If it appears, add it.
    "www.visitsanjuancapistrano.com", # Assuming www.visitsanjuancapistrano.com was intended, but not in the list. If it appears, add it.
    "visitsandiego.com", # Assuming visitsandiego.com was intended, but not in the list. If it appears, add it.
    "www.visitsandiego.com", # Assuming www.visitsandiego.com was intended, but not in the list. If it appears, add it.
    "visitsanmarcos.com", # Assuming visitsanmarcos.com was intended, but not in the list. If it appears, add it.
    "www.visitsanmarcos.com", # Assuming www.visitsanmarcos.com was intended, but not in the list. If it appears, add it.
    "visitsanmarcospromotions.com", # Assuming visitsanmarcospromotions.com was intended, but not in the list. If it appears, add it.
    "www.visitsanmarcospromotions.com", # Assuming www.visitsanmarcospromotions.com was intended, but not in the list. If it appears, add it.
    "visitsantamonica.com", # Added visitsantamonica.com
    "www.visitsantamonica.com", # Added www.visitsantamonica.com - redundant but kept if user had it.
    "visitsacramento.com", # Added visitsacramento.com - repeated, removed one
    "www.visitsacramento.com", # Added www.visitsacramento.com - repeated, removed one
    "visitsacramento.com", # Added visitsacramento.com - repeated, removed one
    "www.visitsacramento.com", # Added www.visitsacramento.com - repeated, removed one
    "visitpasadena.com", # Added visitpasadena.com
    "www.visitpasadena.com", # Added www.visitpasadena.com - redundant but kept if user had it.
    "visitgreaterpalmsprings.com", # Added visitgreaterpalmsprings.com
    "www.visitgreaterpalmsprings.com", # Added www.visitgreaterpalmsprings.com - redundant but kept if user had it.
    "visitcarlsbad.com", # Added visitcarlsbad.com
    "www.visitcarlsbad.com", # Added www.visitcarlsbad.com - redundant but kept if user had it.
    "ssf.net", # Added ssf.net (city of South San Francisco)
    "www.ssf.net", # Added www.ssf.net - redundant but kept if user had it.
    "escondido.gov", # Added escondido.gov (city of Escondido)
    "www.escondido.gov", # Added www.escondido.gov - redundant but kept if user had it.
    "cityofsacramento.gov", # Added cityofsacramento.gov
    "www.cityofsacramento.gov", # Added www.cityofsacramento.gov - redundant but kept if user had it.
    "cityofdhs.org", # Added cityofdhs.org (Desert Hot Springs)
    "www.cityofdhs.org", # Added www.cityofdhs.org - redundant but kept if user had it.
    "ci.anaheim.ca.us", # Added ci.anaheim.ca.us - assuming it was intended, though anaheim.net is used later.
    "www.ci.anaheim.ca.us", # Added www.ci.anaheim.ca.us - redundant but kept if user had it.
    "anaheim.net", # Added anaheim.net (city of Anaheim)
    "www.anaheim.net", # Added www.anaheim.net - redundant but kept if user had it.
    "longbeach.gov", # Added longbeach.gov
    "www.longbeach.gov", # Added www.longbeach.gov - redundant but kept if user had it.
    "carlsbadca.gov", # Added carlsbadca.gov (city of Carlsbad)
    "www.carlsbadca.gov", # Added www.carlsbadca.gov - redundant but kept if user had it.
    "palmdesert.gov", # Added palmdesert.gov (city of Palm Desert)
    "www.palmdesert.gov", # Added www.palmdesert.gov - redundant but kept if user had it.
    "www.lacity.com", # Added www.lacity.com - redundant but kept if user had it.
    "lacounty.gov", # Added lacounty.gov (LA County)
    "www.lacounty.gov", # Added www.lacounty.gov - redundant but kept if user had it.
    "ssf.ca.us", # Added ssf.ca.us - assuming typo and meant .net, but kept in case it's valid
    "www.ssf.ca.us", # Added www.ssf.ca.us - redundant but kept if user had it.
    "ci.carlsbad.ca.us", # Added ci.carlsbad.ca.us - repeated entry, removed one.
    "www.ci.carlsbad.ca.us", # Added www.ci.carlsbad.ca.us - repeated entry, removed one.
    "ci.pasadena.ca.us", # Added ci.pasadena.ca.us - repeated entry, removed one.
    "www.ci.pasadena.ca.us", # Added www.ci.pasadena.ca.us - repeated entry, removed one.
    "ci.palm-springs.ca.us", # Added ci.palm-springs.ca.us - repeated entry, removed one.
    "www.ci.palm-springs.ca.us", # Added www.ci.palm-springs.ca.us - repeated entry, removed one.
    "ci.newport-beach.ca.us", # Added ci.newport-beach.ca.us - repeated entry, removed one.
    "www.ci.newport-beach.ca.us", # Added www.ci.newport-beach.ca.us - repeated entry, removed one.
    "ci.anaheim.ca.us", # Added ci.anaheim.ca.us - repeated entry, removed one.
    "www.ci.anaheim.ca.us", # Added www.ci.anaheim.ca.us - repeated entry, removed one.
    "ci.long-beach.ca.us", # Added ci.long-beach.ca.us - repeated entry, removed one.
    "www.ci.long-beach.ca.us", # Added www.ci.long-beach.ca.us - repeated entry, removed one.
    "ci.santa-monica.ca.us", # Added ci.santa-monica.ca.us - repeated entry, removed one.
    "www.ci.santa-monica.ca.us", # Added www.ci.santa-monica.ca.us - repeated entry, removed one.
    "ci.san-diego.ca.us", # Added ci.san-diego.ca.us - repeated entry, removed one.
    "www.ci.san-diego.ca.us", # Added www.ci.san-diego.ca.us - repeated entry, removed one.
    "ci.santa-barbara.ca.us", # Added ci.santa-barbara.ca.us - repeated entry, removed one.
    "www.ci.santa-barbara.ca.us", # Added www.ci.santa-barbara.ca.us - repeated entry, removed one.
    "zillow.com", # Added zillow.com - repeated, removed one.
    "realtor.com", # Added realtor.com - repeated, removed one.
    "apartments.com", # Added apartments.com - repeated, removed one.
    "redfin.com", # Added redfin.com - repeated, removed one.
    "homes.com", # Added homes.com - repeated, removed one.
    "zumper.com", # Added zumper.com - repeated, removed one.
    "trulia.com",
    "homesnap.com",
    "rent.com",
    "realtytrac.com",
    "loopnet.com",
    "propublica.org",
    "keyhousing.com",
    "hostaway.com",
    "guesty.com",
    "propublica.org",
    "masterhost.ca",
    "masterhost.com",

}

filtered_domains = set()
for domain in GENERIC_DOMAINS:
    if not domain.endswith(".org"):
        filtered_domains.add(domain)

GENERIC_DOMAINS = filtered_domains

BAD_PATH_SEGMENTS = ["/news", "/video", "/business", "/articles", "/news", "/story", "/Homes-for-sale", "news", "articles", "story", "homes-for-sale", '/articles', '/news', '/story', '/homes-for-sale', '/blog'] # List of path segments to block - ADDED NEW PATHS - ADDED AGAIN FOR GOOD MEASURE

filtered_domains = set()
for domain in GENERIC_DOMAINS:
    if not domain.endswith(".org"):
        filtered_domains.add(domain)

GENERIC_DOMAINS = filtered_domains

BAD_PATH_PATTERN = re.compile(r'/(category|tag|author|news|articles|story|homes-for-sale|articles|news|story|Homes-for-sale)/', re.IGNORECASE) # Removed 'page' from bad path pattern - UPDATED BAD_PATH_PATTERN - UPDATED AGAIN FOR GOOD MEASURE

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0', # Added more user agents
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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

def get_headers(url):
    """Get randomized headers for requests"""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': f'en-US,en;q=0.{random.randint(5,9)}',
        'Referer': random.choice([
            'https://www.google.com/',
            'https://www.bing.com/',
            'https://duckduckgo.com/',
            'https://search.yahoo.com/',
            'https://www.startpage.com/'
        ]),
        'Sec-Fetch-Dest': 'document' if random.random() > 0.3 else 'empty',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin' if random.random() > 0.5 else 'cross-site',
        'X-Requested-With': random.choice(['XMLHttpRequest', 'None']),
        'Alt-Used': urlparse(url).netloc,
        'Sec-CH-UA': f'"Chromium";v="{random.randint(100, 115)}", "Not:A-Brand";v="{random.randint(10, 99)}"'
    }

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

PROXY_HEALTH = {} # --- PROXY DISABLED ---
CAPTCHA_BALANCE = float(os.getenv("INITIAL_CAPTCHA_BALANCE", "10.0"))
CAPTCHA_COST = {
    'recaptcha_v2': 0.0025,
    'recaptcha_v3': 0.005,
     'hcaptcha': 0.0025 # ADDED hCaptcha cost - same as reCAPTCHA v2 for now
}
BRIGHTDATA_PORTS = [22225, 22226, 33335] # --- PROXY DISABLED ---
BRIGHTDATA_GEO = ["us-east", "us-west"] # --- PROXY DISABLED ---
BRIGHTDATA_PORTS_CYCLE = cycle(BRIGHTDATA_PORTS) # --- PROXY DISABLED ---
BRIGHTDATA_GEO_CYCLE = cycle(BRIGHTDATA_GEO) # --- PROXY DISABLED ---
PROXY_PROVIDERS = cycle(['oxylabs', 'smartproxy', 'brightdata']) # Proxy provider rotation - ADDED # --- PROXY DISABLED ---
PROXY_ROTATION_TYPE = os.getenv("PROXY_ROTATION_TYPE", "provider_weighted") # Rotation type - ADDED # --- PROXY DISABLED ---
PROXY_WEIGHTS = {'oxylabs': 0.4, 'smartproxy': 0.3, 'brightdata': 0.3} # Weights for provider rotation - ADDED # --- PROXY DISABLED ---

BRIGHTDATA_USER = os.getenv("BRIGHTDATA_USER") # --- PROXY DISABLED ---
BRIGHTDATA_PASS = os.getenv("BRIGHTDATA_PASS") # ADDED BRIGHTDATA_PASS for proxy auth # --- PROXY DISABLED ---
IMPERSONATIONS = ["chrome120", "chrome119"]
captcha_lock = threading.Lock()

PROXY_SCORE_CONFIG = { # Updated PROXY_SCORE_CONFIG - LESS AGGRESSIVE SCORING # --- PROXY DISABLED ---
    'success': +20,    # Reduced reward
    'failure': -5,     # Reduced penalty
    'timeout': -5,     # Reduced timeout penalty
    'min_score': 10,   # Lower minimum score
    'disable_threshold': 15, # Disable proxies if average score <15
    'reactivation_delay': 3600, # 1 hour cooldown
    'initial': 70      # Increased from 50
} # Adjusted proxy score config to be less aggressive # --- PROXY DISABLED ---

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
        'rate_limit_errors': Counter('rate_limit_errors', 'Rate Limit Errors'), # Monitor Rate-Limiting and CAPTCHA Challenges - metrics to track rate-limiting and CAPTCHA challenges
        'page_extraction_errors': Counter('page_extraction_errors', 'Page Extraction Errors'),
        'proxy_errors': Counter('proxy_errors', 'Proxy Errors'),
        'request_errors': Counter('request_errors', 'Request Errors', ['type']), # MODIFIED: Request errors with type
        'url_attempts': Counter('url_attempts', 'URL Attempts', ['status']), # MODIFIED: URL attempts metric
        'extraction_success': Counter('extraction_success', 'Successful data extractions'),
        'crawl_errors': Counter('crawl_errors', 'Crawl errors', ['type', 'proxy']),
        'proxy_health_state': Gauge('proxy_health_state', 'Proxy Health State (1=active, 0=disabled)'), # Proxy health state metric # --- PROXY DISABLED ---
        'proxy_fallback_requests': Counter('proxy_fallback_requests', 'Requests served without proxies'), # Proxy fallback requests metric # --- PROXY DISABLED ---
        'proxy_effectiveness': Summary('proxy_effectiveness', 'Success rate of proxy vs direct requests') # Proxy effectiveness summary # --- PROXY DISABLED ---
    }


metrics = configure_metrics()
crawl_latency_summary = Summary('crawl_latency_seconds', 'Latency of page crawls')

PROXY_SCORES = defaultdict(lambda: PROXY_SCORE_CONFIG['initial']) # Updated PROXY_SCORES - INITIAL SCORE NOW FROM CONFIG # --- PROXY DISABLED ---
PROXY_ROTATION_LOCK = threading.Lock() # --- PROXY DISABLED ---
_session = None
proxy_cycle = None # --- PROXY DISABLED ---
PROXY_DISABLED_UNTIL = 0 # Track when proxies are disabled until # --- PROXY DISABLED ---

SSL_WHITELIST = ["brd.superproxy.io", "gate.smartproxy.com"] # ADDED SSL_WHITELIST # --- PROXY DISABLED ---

# --- Playwright Shared Instance ---
_playwright_instance = None
_browser = None

async def get_playwright_instance():
    global _playwright_instance
    if _playwright_instance is None:
        _playwright_instance = async_playwright()
        await _playwright_instance.__aenter__()
    return _playwright_instance

async def get_browser_instance(proxy=None): # --- PROXY DISABLED --- #async def get_browser_instance(proxy=None):
    async with async_playwright() as p:
        proxy_args = {}
        # if proxy: # Configure proxy if provided # --- PROXY DISABLED ---
        #     proxy_parts = proxy.split('@') # --- PROXY DISABLED ---
        #     if len(proxy_parts) == 2: # --- PROXY DISABLED ---
        #         auth_user_pass, proxy_host_port = proxy_parts # --- PROXY DISABLED ---
        #         auth = auth_user_pass.split(':') # --- PROXY DISABLED ---
        #         if len(auth) == 2: # --- PROXY DISABLED ---
        #             proxy_user, proxy_pass = auth # --- PROXY DISABLED ---
        #             proxy_args = { # --- PROXY DISABLED ---
        #                 "proxy": { # --- PROXY DISABLED ---
        #                     "server": f"http://{proxy_host_port}", # --- PROXY DISABLED ---
        #                     "username": proxy_user, # --- PROXY DISABLED ---
        #                     "password": proxy_pass # --- PROXY DISABLED ---
        #                 } # --- PROXY DISABLED ---
        #             } # --- PROXY DISABLED ---
        #         else: # --- PROXY DISABLED ---
        #              proxy_args = {"proxy": {"server": f"http://{proxy_host_port}"}} # No auth # --- PROXY DISABLED ---
        #     else: # --- PROXY DISABLED ---
        #         proxy_args = {"proxy": {"server": f"http://{proxy}"}} # No auth # --- PROXY DISABLED ---

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
    resolver = AsyncResolver(nameservers=[ # MODIFIED: DNS Cache Rotation - Resolver inside get_session
        '8.8.8.8',
        '1.1.1.1',
        '9.9.9.9',
        '208.67.222.222', # OpenDNS
        '208.67.220.220'  # OpenDNS
    ])
    if not _session or _session.closed:
        ssl_context = False # Default SSL context - can be adjusted if needed globally
        _session = ClientSession(
            connector=TCPConnector(limit=75, resolver=resolver, ssl=ssl_context), # MODIFIED: Use resolver in connector
            timeout=aiohttp.ClientTimeout(total=45) # MODIFIED: Increased timeout to 45s
        )
        logger.debug("DEBUG: Created a new aiohttp ClientSession.") # ADDED DEBUG LOG
    return _session


async def close_session():
    global _session
    if _session and not _session.closed:
        await _session.close()
        logger.debug("DEBUG: Closed the aiohttp ClientSession.") # ADDED DEBUG LOG
    _session = None

def should_use_proxies(proxy_pool_to_use): # Added should_use_proxies function
    return False # --- PROXY DISABLED ---
    global PROXY_DISABLED_UNTIL
    if not proxy_pool_to_use:
        metrics['proxy_health_state'].set(0) # Set proxy health to disabled
        return False

    if time.time() < PROXY_DISABLED_UNTIL: # Check if still in cooldown
        metrics['proxy_health_state'].set(0) # Set proxy health to disabled
        return False

    avg_score = sum(PROXY_SCORES[p] for p in proxy_pool_to_use) / len(proxy_pool_to_use) if proxy_pool_to_use else 100 # Default to 100 if no proxies

    if avg_score < PROXY_SCORE_CONFIG['disable_threshold']:
        PROXY_DISABLED_UNTIL = time.time() + PROXY_SCORE_CONFIG['reactivation_delay'] # Set cooldown
        logger.warning(f"Proxy health below threshold ({avg_score:.2f} < {PROXY_SCORE_CONFIG['disable_threshold']}) - disabling proxies for {PROXY_SCORE_CONFIG['reactivation_delay']//3600} hour.")
        metrics['proxy_health_state'].set(0) # Set proxy health to disabled
        return False

    metrics['proxy_health_state'].set(1) # Set proxy health to active
    return True


async def headless_fallback(url): # MODIFIED: Headless Browser Fallback - Implementation
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True) # Launch in headless mode
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                locale='en-US'
            )
            page = await context.new_page()

            # Simulate human-like navigation
            await page.mouse.move(random.randint(0, 300), random.randint(0, 300), steps=5) # Move mouse slightly
            await asyncio.sleep(random.uniform(0.5, 1.5)) # Small pause

            await page.goto(url, timeout=60000) # Increased timeout for headless
            content = await page.content() # Get content after page load
            await browser.close()
            logger.info(f"Headless browser fetched content for {url}") # Log success
            return content # Return content if successful
    except Exception as e: # Catch exceptions in headless browsing
        logger.error(f"Headless fallback failed for {url}: {str(e)[:200]}") # Log error
        metrics['request_errors'].labels(type='headless_fallback_failed').inc() # MODIFIED: Metric for headless failure
        return None # Return None on failure

def rotate_proxy(proxy_pool):
    return None # --- PROXY DISABLED ---
    if not proxy_pool or not should_use_proxies(proxy_pool): # Check should_use_proxies here
        return None  # Full proxy bypass

    # Select from proxies with scores above minimum
    healthy_proxies = [p for p in proxy_pool if PROXY_SCORES.get(p, 0) > PROXY_SCORE_CONFIG['min_score']]

    if healthy_proxies:
        # Prioritize proxies with highest score
        best_proxy = max(healthy_proxies, key=lambda x: PROXY_SCORES[x])
        logger.debug(f"Rotating to best healthy proxy: {best_proxy} (score: {PROXY_SCORES[best_proxy]})")
        return best_proxy
    else:
        logger.warning("No healthy proxies available, using random fallback.")
        return random.choice(proxy_pool) # Fallback to random if no healthy proxies


def decay_proxy_scores(): # Commenting out proxy score decay
    pass # --- PROXY DISABLED ---
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


async def crawl_and_extract_async(session, context, proxy_pool=None, current_proxy=None, depth=0): # FIX 4: Missing Internal Link Following - Add depth parameter
    url = context['url']
    city = context['city'] # City from context - this is what we want to use
    term = context['term']
    logger.debug(f"Entering crawl_and_extract_async for URL: {url}, City: {city}, Term: {term}, Proxy: Direct") # DEBUG LOG - ENTRY POINT # --- PROXY DISABLED --- #Proxy: {current_proxy if current_proxy else 'Direct'}
    # Initialize with minimal data # MODIFIED: Persist URLs on Failure - Initialize with status='attempted'
    extracted_data = PropertyManagerData(
         city=city, # Assign city from context - DIRECT ASSIGNMENT HERE
         url=url,
         searchKeywords=[term],
         link=url, # Set link to be the same as url for now
         status='attempted' # MODIFIED: Persist URLs on Failure - Initial status
    )
    if not validate_request(url): # Validate URL at start
        metrics['crawl_errors'].labels(type='invalid_url', proxy='no_proxy').inc() # Label as no_proxy
        return extracted_data # MODIFIED: Persist URLs on Failure - Return even on invalid URL

    start_time = time.time()

    proxy_key = 'no_proxy' # --- PROXY DISABLED --- #current_proxy if current_proxy else 'no_proxy' # Determine proxy_key based on current_proxy
    proxy_retries = 0
    use_proxy = False # --- PROXY DISABLED --- #os.getenv("USE_PROXY", "False").lower() == "true"
    use_proxy_for_request = False # --- PROXY DISABLED --- #should_use_proxies(proxy_pool) # Determine if proxies should be used for this request

    if use_proxy and use_proxy_for_request and proxy_pool and not current_proxy:
        current_proxy = rotate_proxy(proxy_pool)
        proxy_key = current_proxy if current_proxy else 'no_proxy' # Update proxy_key after rotation
        if not current_proxy:
            logger.warning("No proxy available initially, will try without proxy if proxy attempts fail.")

    parsed_url = urlparse(url)
    try_again_with_new_proxy = False # --- PROXY DISABLED --- #True
    e_aiohttp = None
    hcaptcha_element = None
    attempted_proxy = None # --- PROXY DISABLED --- #current_proxy # Track which proxy was attempted

    # while try_again_with_new_proxy and use_proxy and use_proxy_for_request and proxy_pool and attempted_proxy: # Check attempted_proxy instead of current_proxy # --- PROXY DISABLED ---
    #     try_again_with_new_proxy = False # --- PROXY DISABLED ---
    #     hcaptcha_element = None # --- PROXY DISABLED ---
    #     try: # --- PROXY DISABLED ---
    #         headers = {'User-Agent': random.choice(USER_AGENTS)} # --- PROXY DISABLED ---
    #         headers.update(random.choice(HEADERS_LIST)) # --- PROXY DISABLED ---
    #         headers['Referer'] = urlparse(url).netloc # --- PROXY DISABLED ---

    #         ssl_context = False if any(d in attempted_proxy for d in SSL_WHITELIST) else None # Use attempted_proxy for SSL whitelist check # --- PROXY DISABLED ---

    #         if os.getenv("IMPERSONATE_CHROME", "false").lower() == "true": # --- PROXY DISABLED ---
    #             impersonate_str = random.choice(IMPERSONATIONS) # --- PROXY DISABLED ---
    #             logger.debug(f"Impersonating: {impersonate_str}") # --- PROXY DISABLED ---
    #             resp = await session.get(url, headers=headers, proxy=f"http://{attempted_proxy}" if attempted_proxy else None, impersonate=impersonate_str, timeout=CLIENT_TIMEOUT, ssl=ssl_context) # Use attempted_proxy # --- PROXY DISABLED ---
    #         else: # --- PROXY DISABLED ---
    #             resp = await session.get(url, headers=headers, proxy=f"http://{attempted_proxy}" if attempted_proxy else None, timeout=CLIENT_TIMEOUT, ssl=ssl_context) # Use attempted_proxy # --- PROXY DISABLED ---

    #         metrics['http_status_codes'].labels(code=resp.status).inc() # --- PROXY DISABLED ---

    #         if resp.status in [403, 404, 429, 500, 503]: # --- PROXY DISABLED ---
    #             logger.warning(f"HTTP error {resp.status} for {url} using proxy: {attempted_proxy if attempted_proxy else 'None'}") # Use attempted_proxy # --- PROXY DISABLED ---
    #             metrics['crawl_errors'].labels(type=f'http_{resp.status}', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #             update_proxy_score(attempted_proxy, False) # Update score for attempted_proxy # --- PROXY DISABLED ---

    #             if resp.status == 403 and proxy_pool and proxy_retries < 2: # --- PROXY DISABLED ---
    #                 proxy_retries += 1 # --- PROXY DISABLED ---
    #                 current_proxy = rotate_proxy(proxy_pool) # --- PROXY DISABLED ---
    #                 attempted_proxy = current_proxy # Update attempted_proxy for retry # --- PROXY DISABLED ---
    #                 proxy_key = attempted_proxy if attempted_proxy else 'no_proxy' # Update proxy_key # --- PROXY DISABLED ---
    #                 logger.info(f"Retrying {url} with a different proxy due to 403, attempt {proxy_retries + 1}, proxy: {attempted_proxy if attempted_proxy else 'Direct'}") # --- PROXY DISABLED ---
    #                 try_again_with_new_proxy = True # --- PROXY DISABLED ---
    #                 continue # --- PROXY DISABLED ---
    #             else: # --- PROXY DISABLED ---
    #                 metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #                 return None # Stop proxy attempts and fallback to direct # --- PROXY DISABLED ---

    #         if resp.status == 405: # --- PROXY DISABLED ---
    #             logger.warning(f"HTTP 405 error for {url}, trying with allow_redirects=False") # --- PROXY DISABLED ---
    #             metrics['crawl_errors'].labels(type='http_405', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #             resp = await session.get(url, headers=headers, proxy=f"http://{attempted_proxy}" if attempted_proxy else None, allow_redirects=False, timeout=CLIENT_TIMEOUT, ssl=ssl_context) # Use attempted_proxy # --- PROXY DISABLED ---
    #             if resp_after_captcha.status != 200: # --- PROXY DISABLED ---
    #                 logger.warning(f"HTTP 405 retry failed, status {resp.status}") # --- PROXY DISABLED ---
    #                 update_proxy_score(attempted_proxy, False) # Update score for attempted_proxy # --- PROXY DISABLED ---
    #                 metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #                 return None # Stop proxy attempts and fallback to direct # --- PROXY DISABLED ---

    #         if resp.status == 503 and "amazonaws.com/captcha" in str(resp.url): # --- PROXY DISABLED ---
    #             logger.warning(f"AWS CAPTCHA detected at {resp.url} for {url}") # --- PROXY DISABLED ---
    #             metrics['captcha_requests'].inc() # --- PROXY DISABLED ---
    #             metrics['captcha_failed'].inc() # --- PROXY DISABLED ---
    #             update_proxy_score(attempted_proxy, False) # Update score for attempted_proxy # --- PROXY DISABLED ---
    #             metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #             return None # Stop proxy attempts and fallback to direct # --- PROXY DISABLED ---

    #         html_content = await resp.text() # --- PROXY DISABLED ---
    #         print("----- HTML Content (First 500 chars) - aiohttp with proxy -----") # --- PROXY DISABLED ---
    #         print(html_content[:500]) # --- PROXY DISABLED ---
    #         print("----- Text Content (First 500 chars) - aiohttp with proxy -----") # --- PROXY DISABLED ---
    #         soup = BeautifulSoup(html_content, 'lxml') # --- PROXY DISABLED ---
    #         text_content = soup.get_text(separator=' ', strip=True).lower() # --- PROXY DISABLED ---
    #         print(text_content[:500]) # --- PROXY DISABLED ---

    #         hcaptcha_element = soup.find('h-captcha') # --- PROXY DISABLED ---
    #         if hcaptcha_element: # --- PROXY DISABLED ---
    #             logger.warning(f"hCaptcha element initially detected by BeautifulSoup, but might be JS rendered. Proceeding with Playwright check for {url}.") # --- PROXY DISABLED ---
    #         else: # --- PROXY DISABLED ---
    #             logger.debug(f"hCaptcha not found in initial HTML, proceeding with BeautifulSoup parsing for {url}") # --- PROXY DISABLED ---
    #             captcha_sitekey_v2 = re.search(r'data-sitekey="([^"]+)"', html_content) # --- PROXY DISABLED ---
    #             captcha_sitekey_v3 = re.search(r'sitekey: ?"([^"]+)"', html_content) # --- PROXY DISABLED ---
    #             captcha_sitekey_invisible = re.search(r'recaptcha\.render\("([^"]+)", {', html_content) # --- PROXY DISABLED ---

    #             if captcha_sitekey_v2 and not hcaptcha_element: # --- PROXY DISABLED ---
    #                 captcha_sitekey = captcha_sitekey_v2.group(1) # --- PROXY DISABLED ---
    #                 logger.warning(f"ReCAPTCHA v2 detected on {url}, sitekey: {captcha_sitekey[:20]}...") # --- PROXY DISABLED ---
    #                 captcha_response = await solve_captcha(url, captcha_sitekey, 'recaptcha_v2', attempted_proxy) # Use attempted_proxy # --- PROXY DISABLED ---
    #                 if captcha_response: # --- PROXY DISABLED ---
    #                     metrics['captcha_solved'].inc() # --- PROXY DISABLED ---
    #                     logger.info(f"reCAPTCHA v2 solved, proceeding with request...") # --- PROXY DISABLED ---
    #                     captcha_params = {'g-recaptcha-response': captcha_response} # --- PROXY DISABLED ---
    #                     resp_after_captcha = await session.get(url, params=captcha_params, headers=headers, proxy=f"http://{attempted_proxy}" if attempted_proxy else None, timeout=CLIENT_TIMEOUT, ssl=ssl_context) # Use attempted_proxy # --- PROXY DISABLED ---
    #                     if resp_after_captcha.status == 200: # --- PROXY DISABLED ---
    #                         html_content = await resp_after_captcha.text() # --- PROXY DISABLED ---
    #                         soup = BeautifulSoup(html_content, 'lxml') # --- PROXY DISABLED ---
    #                         text_content = soup.get_text(separator=' ', strip=True).lower() # --- PROXY DISABLED ---
    #                     else: # --- PROXY DISABLED ---
    #                         logger.error(f"reCAPTCHA v2 solve failed to get 200 OK, status: {resp_after_captcha.status}") # --- PROXY DISABLED ---
    #                         metrics['crawl_errors'].labels(type='captcha_failed_http', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #                         update_proxy_score(attempted_proxy, False) # Update score for attempted_proxy # --- PROXY DISABLED ---
    #                         metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #                         return None # Stop proxy attempts and fallback to direct # --- PROXY DISABLED ---
    #                 else: # --- PROXY DISABLED ---
    #                     logger.error(f"ReCAPTCHA v2 solving failed for {url}") # --- PROXY DISABLED ---
    #                     metrics['crawl_errors'].labels(type='captcha_unsolved_v2', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #                     update_proxy_score(attempted_proxy, False) # Update score for attempted_proxy # --- PROXY DISABLED ---
    #                     metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #                     return None # Stop proxy attempts and fallback to direct # --- PROXY DISABLED ---

    #             elif captcha_sitekey_v3 and not hcaptcha_element: # --- PROXY DISABLED ---
    #                 captcha_sitekey = captcha_sitekey_v3.group(1) # --- PROXY DISABLED ---
    #                 logger.warning(f"ReCAPTCHA v3 detected on {url}, sitekey: {captcha_sitekey[:20]}...") # --- PROXY DISABLED ---
    #                 captcha_response = await solve_captcha(url, captcha_sitekey, 'recaptcha_v3', attempted_proxy) # Use attempted_proxy # --- PROXY DISABLED ---
    #                 if captcha_response: # --- PROXY DISABLED ---
    #                     metrics['captcha_solved'].inc() # --- PROXY DISABLED ---
    #                     logger.info(f"reCAPTCHA v3 solved (although v3 solving is skipped by default).") # --- PROXY DISABLED ---
    #                 else: # --- PROXY DISABLED ---
    #                     logger.warning(f"ReCAPTCHA v3 solving skipped or failed for {url}") # --- PROXY DISABLED ---
    #                     metrics['crawl_errors'].labels(type='captcha_skipped_v3', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #                     metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #                     return None # Stop proxy attempts and fallback to direct # --- PROXY DISABLED ---

    #             elif captcha_sitekey_invisible and not hcaptcha_element: # --- PROXY DISABLED ---
    #                 captcha_sitekey = captcha_sitekey_invisible.group(1) # --- PROXY DISABLED ---
    #                 logger.warning(f"Invisible reCAPTCHA detected on {url}, element ID: {captcha_sitekey[:20]}...") # --- PROXY DISABLED ---
    #                 captcha_response = await solve_captcha(url, captcha_sitekey, 'recaptcha_v2', attempted_proxy) # Use attempted_proxy # --- PROXY DISABLED ---
    #                 if captcha_response: # --- PROXY DISABLED ---
    #                     metrics['captcha_solved'].inc() # --- PROXY DISABLED ---
    #                     logger.info(f"Invisible reCAPTCHA solved, proceeding with request...") # --- PROXY DISABLED ---
    #                     captcha_params = {'g-recaptcha-response': captcha_response} # --- PROXY DISABLED ---
    #                     resp_after_captcha = await session.get(url, params=captcha_params, headers=headers, proxy=f"http://{attempted_proxy}" if attempted_proxy else None, timeout=CLIENT_TIMEOUT, ssl=ssl_context) # Use attempted_proxy # --- PROXY DISABLED ---
    #                     if resp_after_captcha.status == 200: # --- PROXY DISABLED ---
    #                         html_content = await resp_after_captcha.text() # --- PROXY DISABLED ---
    #                         soup = BeautifulSoup(html_content, 'lxml') # --- PROXY DISABLED ---
    #                         text_content = soup.get_text(separator=' ', strip=True).lower() # --- PROXY DISABLED ---
    #                     else: # --- PROXY DISABLED ---
    #                         logger.error(f"Invisible reCAPTCHA solve failed to get 200 OK, status: {resp_after_captcha.status}") # --- PROXY DISABLED ---
    #                         metrics['crawl_errors'].labels(type='captcha_failed_http_invisible', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #                         update_proxy_score(attempted_proxy, False) # Update score for attempted_proxy # --- PROXY DISABLED ---
    #                         metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #                         return None # Stop proxy attempts and fallback to direct # --- PROXY DISABLED ---
    #                 else: # --- PROXY DISABLED ---
    #                     logger.error(f"Invisible reCAPTCHA solving failed for {url}") # --- PROXY DISABLED ---
    #                     metrics['crawl_errors'].labels(type='captcha_unsolved_invisible', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #                     update_proxy_score(attempted_proxy, False) # Update score for attempted_proxy # --- PROXY DISABLED ---
    #                     metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #                     return None # Stop proxy attempts and fallback to direct # --- PROXY DISABLED ---


    #             if BAD_PATH_PATTERN.search(parsed_url.path): # --- PROXY DISABLED ---
    #                 logger.warning(f"Skipping URL due to bad path pattern: {url}") # --- PROXY DISABLED ---
    #                 metrics['crawl_errors'].labels(type='bad_path_pattern', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #                 return None # --- PROXY DISABLED ---

    #             domain = urlparse(url).netloc.lower() # --- PROXY DISABLED ---
    #             if domain in GENERIC_DOMAINS or any(blocked in domain for blocked in GENERIC_DOMAINS): # --- PROXY DISABLED ---
    #                 logger.warning(f"Skipping generic domain URL: {url}") # --- PROXY DISABLED ---
    #                 metrics['crawl_errors'].labels(type='generic_domain', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #                 return None # --- PROXY DISABLED ---


    #             domain = urlparse(url).netloc.replace('www.', '') # --- PROXY DISABLED ---

    #             emails = extract_emails(html_content, domain) # --- PROXY DISABLED ---
    #             phones = extract_phone_numbers(text_content) # --- PROXY DISABLED ---
    #             address = extract_address(soup) # --- PROXY DISABLED ---
    #             extracted_name = extract_company_name(url) # --- PROXY DISABLED ---
    #             if not extracted_name or extracted_name == "N/A": # --- PROXY DISABLED ---
    #                 extracted_name_element = soup.find("h1") or soup.find("title") # --- PROXY DISABLED ---
    #                 if extracted_name_element: # --- PROXY DISABLED ---
    #                     extracted_name = extracted_name_element.text.strip() # --- PROXY DISABLED ---
    #                 else: # --- PROXY DISABLED ---
    #                     extracted_name = "N/A" # --- PROXY DISABLED ---


    #             address_text = extract_address(soup) # Address extraction - KEEP THIS if you still want to extract address info # --- PROXY DISABLED ---
    #             # Basic City extraction from address - REMOVE CITY EXTRACTION LOGIC # --- REMOVE --- # --- PROXY DISABLED ---
    #             # address_parts = address_text.split(',') # --- REMOVE --- # --- PROXY DISABLED ---
    #             # if len(address_parts) >= 2: # --- REMOVE --- # --- PROXY DISABLED ---
    #             #     extracted_city = address_parts[-2].strip() # Assume city is second to last part # --- REMOVE --- # --- PROXY DISABLED ---
    #             # else: # --- REMOVE --- # --- PROXY DISABLED ---
    #             extracted_city = "N/A" # Default if city extraction fails (though we are not using it now for database city) # --- KEEP DEFAULT VALUE --- # --- PROXY DISABLED ---


    #             extracted_data.name = extracted_name # --- PROXY DISABLED ---
    #             extracted_data.email = emails # --- PROXY DISABLED ---
    #             extracted_data.phoneNumber = phones # --- PROXY DISABLED ---
    #             extracted_data.city = city # DIRECTLY ASSIGN CITY FROM CONTEXT - NO MORE OVERWRITING # --- PROXY DISABLED ---


    #             logger.info(f"Data extracted successfully from {url} (proxy scrape), proxy: {attempted_proxy}, name: {extracted_name}, emails: {emails}, phones: {phones}, city: {city}") # Log proxy and city from context # --- PROXY DISABLED ---
    #             metrics['extraction_success'].inc() # --- PROXY DISABLED ---
    #             metrics['proxy_effectiveness'].observe(1) # Record proxy success # --- PROXY DISABLED ---
    #             logger.debug(f"Exiting crawl_and_extract_async successfully for {url} (proxy), returning data: {extracted_data}") # DEBUG LOG - EXIT SUCCESS # --- PROXY DISABLED ---

    #             return extracted_data # --- PROXY DISABLED ---

    #     except aiohttp.ClientError as e: # --- PROXY DISABLED ---
    #         logger.error(f"aiohttp ClientError during proxy scraping for {url} with proxy {attempted_proxy}: {e}") # Log proxy in error message # --- PROXY DISABLED ---
    #         metrics['request_errors'].inc() # --- PROXY DISABLED ---
    #         metrics['crawl_errors'].labels(type='client_error', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #         update_proxy_score(attempted_proxy, False) # Update score for attempted_proxy # --- PROXY DISABLED ---
    #         metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #         logger.debug(f"Exiting crawl_and_extract_async with aiohttp.ClientError for {url} (proxy), returning None") # DEBUG LOG - EXIT FAILURE # --- PROXY DISABLED ---
    #         return None # Fallback to direct scrape # --- PROXY DISABLED ---
    #     except asyncio.TimeoutError: # --- PROXY DISABLED ---
    #         logger.error(f"Asyncio timeout during proxy scraping for {url} with proxy {attempted_proxy}") # Log proxy in timeout error # --- PROXY DISABLED ---
    #         metrics['request_errors'].inc() # --- PROXY DISABLED ---
    #         metrics['crawl_errors'].labels(type='timeout_error', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #         update_proxy_score(attempted_proxy, False) # Update score for attempted_proxy # --- PROXY DISABLED ---
    #         metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #         logger.debug(f"Exiting crawl_and_extract_async with asyncio.TimeoutError for {url} (proxy), returning None") # DEBUG LOG - EXIT FAILURE # --- PROXY DISABLED ---
    #         return None # Fallback to direct scrape # --- PROXY DISABLED ---
    #     except Exception as e_proxy_crawl: # --- PROXY DISABLED ---
    #         logger.exception(f"Unexpected error during proxy scraping for {url} with proxy {attempted_proxy}: {e_proxy_crawl}") # Log proxy in exception # --- PROXY DISABLED ---
    #         metrics['page_extraction_errors'].inc() # --- PROXY DISABLED ---
    #         metrics['crawl_errors'].labels(type='extraction_error', proxy=proxy_key).inc() # --- PROXY DISABLED ---
    #         update_proxy_score(attempted_proxy, False) # Update score for attempted_proxy # --- PROXY DISABLED ---
    #         metrics['proxy_fallback_requests'].inc() # Increment fallback counter # --- PROXY DISABLED ---
    #         logger.debug(f"Exiting crawl_and_extract_async with unexpected error for {url} (proxy), returning None") # DEBUG LOG - EXIT FAILURE # --- PROXY DISABLED ---
    #         return None # Fallback to direct scrape # --- PROXY DISABLED ---

    #     metrics['proxy_fallback_requests'].inc() # Increment fallback counter if proxy loop finishes without success # --- PROXY DISABLED ---
    #     try_again_with_new_proxy = False # Ensure loop exit if proxy attempts fail # --- PROXY DISABLED ---


    # --- Fallback to Direct Scraping (No Proxy) ---
    # if use_proxy and use_proxy_for_request and proxy_pool and not attempted_proxy: # Check for proxy pool being used and proxy being rotated # --- PROXY DISABLED ---
    #     logger.warning(f"All proxies failed or none available. Attempting direct scraping without proxy for {url}") # --- PROXY DISABLED ---
    # elif use_proxy and use_proxy_for_request and proxy_pool and attempted_proxy: # Condition for fallback logging - if proxy was attempted and failed # --- PROXY DISABLED ---
    #     logger.warning(f"Proxy attempts failed for {url}. Falling back to direct scraping without proxy.") # --- PROXY DISABLED ---
    # else: # Log direct scraping attempt even when proxies are not enabled/healthy # --- PROXY DISABLED ---
    logger.info(f"Attempting direct scraping for {url} (proxies disabled or not used).")


    try: # Direct scraping attempt
        headers = get_headers(url) # MODIFIED: Enhanced Header Rotation - Use get_headers()
        headers['Referer'] = urlparse(url).netloc # Set referer for direct requests as well

        logger.debug(f"Fetching URL (direct): {url}") # Debug log for direct fetch URL

        resp = await session.get(url, headers=headers, timeout=CLIENT_TIMEOUT) # No proxy here

        metrics['http_status_codes'].labels(code=resp.status).inc()

        if resp.status in [403, 404, 429, 500, 503]: # Still handle HTTP errors even without proxy
            logger.warning(f"HTTP error {resp.status} for {url} during direct scraping.")
            metrics['crawl_errors'].labels(type=f'http_{resp.status}', proxy='no_proxy').inc() # Label as no_proxy
            if resp.status == 403: # MODIFIED: Headless Browser Fallback - Check for 403 and try headless
                logger.info(f"Trying headless browser fallback for {url} due to 403.")
                headless_html_content = await headless_fallback(url) # Call headless_fallback
                if headless_html_content: # If headless fallback successful
                    html_content = headless_html_content # Use headless content
                    soup = BeautifulSoup(html_content, 'lxml') # Parse headless content
                    text_content = soup.get_text(separator=' ', strip=True).lower() # Extract text from headless content
                    logger.info(f"Headless browser fallback successful for {url}.") # Log success
                else: # If headless fallback failed
                    metrics['proxy_effectiveness'].observe(0) # Record direct request failure
                    logger.debug(f"Exiting crawl_and_extract_async with HTTP error during direct scrape for {url}, returning None") # DEBUG LOG - EXIT FAILURE
                    return extracted_data # MODIFIED: Persist URLs on Failure - Return extracted_data

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
            logger.debug(f"Crawl Filter - Generic Domain: {url} - Domain: {domain}") # DEBUG LOG - GENERIC DOMAIN FILTER
            metrics['crawl_errors'].labels(type='generic_domain', proxy='no_proxy').inc()
            metrics['proxy_effectiveness'].observe(0) # Record direct request failure
            logger.debug(f"Exiting crawl_and_extract_async due to generic domain (direct scrape) for {url}, returning None") # DEBUG LOG - EXIT FAILURE
            return extracted_data # MODIFIED: Persist URLs on Failure - Return extracted_data


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
        # Basic City extraction from address - REMOVE CITY EXTRACTION LOGIC
        # address_parts = address_text.split(',') # --- REMOVE ---
        # if len(address_parts) >= 2: # --- REMOVE ---
        #     extracted_city = address_parts[-2].strip() # Assume city is second to last part # --- REMOVE ---
        # else: # --- REMOVE ---


        extracted_data.name = extracted_name
        extracted_data.email = emails
        extracted_data.phoneNumber = phones
        extracted_data.city = city # DIRECTLY ASSIGN CITY FROM CONTEXT - NO MORE OVERWRITING
        extracted_data.searchKeywords = [term] # Assign searchKeywords here
        extracted_data.status = 'success' # MODIFIED: Persist URLs on Failure - Set status to 'success'

        # FIX 4: Missing Internal Link Following - Implementation of depth tracking
        internal_links = [urljoin(url, link) for link in extract_internal_links(soup, parsed_url.netloc)] # Placeholder for internal link extraction
        # FIX 7: Internal Link Handling - Use config constant instead of hardcoded value
        if depth < MAX_DEPTH:
            logger.info(f"Following internal links from {url} at depth {depth}, found {len(internal_links)} links.")
            # Placeholder for processing internal links - You'll need to implement process_internal_links function
            # await process_internal_links(session, internal_links, context, proxy_pool, depth + 1) # Example call - implement process_internal_links function

        logger.info(f"Data extracted successfully from {url} (direct scrape), name: {extracted_name}, emails: {emails}, phones: {phones}, city: {city}") # Log city from context
        metrics['extraction_success'].inc()
        metrics['proxy_effectiveness'].observe(0) # Record direct request success (using 0 to differentiate from proxy success)
        logger.debug(f"Exiting crawl_and_extract_async successfully (direct scrape) for {url}, returning data: {extracted_data}") # DEBUG LOG - EXIT SUCCESS


        return extracted_data

    except aiohttp.ClientError as e:
        logger.error(f"aiohttp ClientError during direct scraping for {url}: {e}")
        metrics['request_errors'].labels(type='aiohttp_client_error').inc() # MODIFIED: Request errors with type - aiohttp_client_error
        metrics['crawl_errors'].labels(type='client_error', proxy='no_proxy').inc() # Label as no_proxy
        metrics['proxy_effectiveness'].observe(0) # Record direct request failure
        extracted_data.status = f'failed: aiohttp.ClientError' # MODIFIED: Persist URLs on Failure - Set status to 'failed'
        logger.debug(f"Exiting crawl_and_extract_async with aiohttp.ClientError during direct scrape for {url}, returning None") # DEBUG LOG - EXIT FAILURE
        return extracted_data # MODIFIED: Persist URLs on Failure - Return extracted_data
    except asyncio.TimeoutError:
        logger.error(f"Asyncio timeout during direct scraping for {url}")
        metrics['request_errors'].labels(type='asyncio_timeout').inc() # MODIFIED: Request errors with type - asyncio_timeout
        metrics['crawl_errors'].labels(type='timeout_error', proxy='no_proxy').inc() # Label as no_proxy
        metrics['proxy_effectiveness'].observe(0) # Record direct request failure
        extracted_data.status = f'failed: asyncio.TimeoutError' # MODIFIED: Persist URLs on Failure - Set status to 'failed'
        logger.debug(f"Exiting crawl_and_extract_async with asyncio.TimeoutError during direct scrape for {url}, returning None") # DEBUG LOG - EXIT FAILURE
        return extracted_data # MODIFIED: Persist URLs on Failure - Return extracted_data
    except Exception as e_direct_crawl: # MODIFIED: General exception handling
        logger.exception(f"Unexpected error during direct scraping for {url}: {e_direct_crawl}")
        metrics['page_extraction_errors'].inc()
        metrics['crawl_errors'].labels(type='extraction_error', proxy='no_proxy').inc() # Label as no_proxy
        metrics['proxy_effectiveness'].observe(0) # Record direct request failure
        logger.debug(f"Exiting crawl_and_extract_async with unexpected error during direct scrape for {url}, returning None") # DEBUG LOG - EXIT FAILURE
        return extracted_data # MODIFIED: Persist URLs on Failure - Return extracted_data
    # --- End Fallback to Direct Scraping ---
    except Exception as e: # MODIFIED: General exception handling
        extracted_data.status = f'error: {str(e)[:100]}' # MODIFIED: Persist URLs on Failure - Set status to 'error'
        return extracted_data # MODIFIED: Persist URLs on Failure - Return extracted_data
    logger.debug(f"Exiting crawl_and_extract_async (all attempts failed) for {url}, returning None") # DEBUG LOG - EXIT FAILURE
    metrics['proxy_effectiveness'].observe(0) # Record overall failure
    return extracted_data # MODIFIED: Persist URLs on Failure - Return extracted_data

# FIX 4: Missing Internal Link Following - Placeholder for internal link extraction function - Implement this based on your needs
def extract_internal_links(soup, domain):
    internal_links = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href')
        if href:
            absolute_url = urljoin(f"https://{domain}", href) # Assuming https, adjust if needed or get scheme from original URL
            parsed_link = urlparse(absolute_url)
            if parsed_link.netloc == domain: # Check if the link is internal to the same domain
                internal_links.add(absolute_url)
    return list(internal_links)


def get_proxy_pool():
    return [] # --- PROXY DISABLED ---
    proxies = []
    provider_counts = {
        'oxylabs': 0,
        'smartproxy': 0,
        'brightdata': 0
    }

    if PROXY_ROTATION_TYPE == "provider_weighted": # Provider-weighted rotation
        providers_list = []
        for provider, weight in PROXY_WEIGHTS.items():
            providers_list.extend([provider] * int(weight * 100)) # Create weighted list
        provider_choice = random.choice(providers_list) # Random weighted choice

        if provider_choice == 'oxylabs' and os.getenv('OXLABS_USER') and os.getenv('OXLABS_PASS'):
            proxies.append(f"{os.getenv('OXLABS_USER')}:{os.getenv('OXLABS_PASS')}@pr.oxylabs.io:7777")
            provider_counts['oxylabs'] += 1
        elif provider_choice == 'smartproxy' and os.getenv('SMARTPROXY_USER') and os.getenv('SMARTPROXY_PASS'):
            proxies.append(f"{os.getenv('SMARTPROXY_USER')}:{os.getenv('SMARTPROXY_PASS')}@gate.smartproxy.com:7000")
            provider_counts['smartproxy'] += 1
        elif provider == 'brightdata' and os.getenv('BRIGHTDATA_USER') and os.getenv('BRIGHTDATA_PASS'):
            port = next(BRIGHTDATA_PORTS_CYCLE)
            proxy_string = f"{os.getenv('BRIGHTDATA_USER')}:{os.getenv('BRIGHTDATA_PASS')}@brd.superproxy.io:{port}"
            if "brd.superproxy.io" in proxy_string:
                proxy_string = proxy_string.replace("https://", "http://") + "?type=http"
            proxies.append(proxy_string)
            provider_counts['brightdata'] += 1

    elif PROXY_ROTATION_TYPE == "round_robin": # Round-robin rotation
        provider = next(PROXY_PROVIDERS) # Cycle through providers
        if provider == 'oxylabs' and os.getenv('OXLABS_USER') and os.getenv('OXLABS_PASS'):
            proxies.append(f"{os.getenv('OXLABS_USER')}:{os.getenv('OXLABS_PASS')}@pr.oxylabs.io:7777")
            provider_counts['oxylabs'] += 1
        elif provider == 'smartproxy' and os.getenv('SMARTPROXY_USER') and os.getenv('SMARTPROXY_PASS'):
            proxies.append(f"{os.getenv('SMARTPROXY_USER')}:{os.getenv('SMARTPROXY_PASS')}@gate.smartproxy.com:7000")
            provider_counts['smartproxy'] += 1
        elif provider == 'brightdata' and os.getenv('BRIGHTDATA_USER') and os.getenv('BRIGHTDATA_PASS'):
            port = next(BRIGHTDATA_PORTS_CYCLE)
            proxy_string = f"{os.getenv('BRIGHTDATA_USER')}:{os.getenv('BRIGHTDATA_PASS')}@brd.superproxy.io:{port}"
            if "brd.superproxy.io" in proxy_string:
                proxy_string = proxy_string.replace("https://", "http://") + "?type=http"
            proxies.append(proxy_string)
            provider_counts['brightdata'] += 1
    logger.info(f"Proxy pool êµ¬ì„±: {provider_counts}")

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


PROXY_REGEX = r'^(.+?:.+?@)?[\w\.-]+:\d+(\?.*)?$' # Modified regex to allow query parameters # --- PROXY DISABLED ---
EMAIL_FILTER_SUFFIXES = [
    '.jpg', '.png', 'wixpress.com', 'sentry.io',
    '.gif', '.jpeg', '.svg', '.bmp',  # Image suffixes
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', # Document suffixes
    'example.com', 'domain.com', 'yourdomain.com', 'email.com', 'name.com', # Placeholder domains
    'no-reply', 'noreply', 'donotreply', 'do-not-reply', 'no.reply', # No-reply indicators
    'unsubscribe', 'optout', 'opt-out', # Unsubscribe/opt-out related
    'newsletter', 'marketing', 'promo', 'ads', 'advertising', # Marketing/promotional
    'support', 'help', 'feedback', 'info', # Generic info/support (use with caution - might filter valid emails)
    'sales', # Generic sales (use with caution - might filter valid emails)
    'techsupport', 'webmaster', 'postmaster', 'abuse', # Technical/admin emails
    'privacy', 'legal', 'terms', 'policy', # Legal/policy emails
    'careers', 'jobs', 'hr', 'recruiting', # HR/recruitment emails
    'billing', 'accounts', 'finance', 'payments', # Billing/finance emails
    'notifications', 'alerts', 'updates', 'messages', # Notification emails
    'cdn.', '.css', '.js', '.xml', '.json', '.txt', '.ico', '.rss', '.atom',  # File extensions and CDN
    'w3.org', 'apache.org', 'mozilla.org', 'opensource.org', 'ietf.org', # Common website infrastructure domains
    'github.com', 'stackoverflow.com', # Developer/community platforms
    'wordpress.com', 'squarespace.com', 'weebly.com', 'godaddy.com', # Website builder platforms
    'wikimedia.org', 'wikipedia.org', 'creativecommons.org', # Educational/non-profit
    'youtu.be', 'vimeo.com', 'dailymotion.com', 'soundcloud.com', # Video/audio platforms
    'paypal.com', 'stripe.com', 'shopify.com', 'amazon.com', 'ebay.com' # E-commerce/payment platforms (add more relevant ones)
    # ... Add more suffixes based on your observations of irrelevant emails ...
]

def validate_proxy_config():
    return True # --- PROXY DISABLED ---
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


async def solve_captcha(page_url, captcha_sitekey, captcha_type, current_proxy=None): # --- PROXY DISABLED --- #async def solve_captcha(page_url, captcha_sitekey, captcha_type, current_proxy=None):
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
    # if current_proxy: # --- PROXY DISABLED ---
    #     kwargs["default_proxy"] = current_proxy # --- PROXY DISABLED ---
    #     logger.debug(f"CAPTCHA solving using proxy: {current_proxy}") # --- PROXY DISABLED ---

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
    # No enhanced data parsing needed for the new schema fields in this basic example
    return None


class PropertyManagerData(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = "N/A"  # Property Manager Name
    email: List[EmailStr] = Field(default_factory=list)  # List of emails
    phoneNumber: List[str] = Field(default_factory=list)  # List of phone numbers
    city: Optional[str] = "N/A" # City -  Now represents City from CITIES list
    url: Optional[HttpUrl] = None  # Website URL
    searchKeywords: List[str] = Field(default_factory=list) # Search Keywords - from SEARCH_TERMS
    latLngPoint: Optional[str] = None  # latLngPoint - Keep if you intend to use it later
    status: Optional[str] = 'pending' # MODIFIED: Status field for tracking crawl attempts
    lastEmailSentAt: Optional[datetime.datetime] = None # lastEmailSentAt - Keep if you intend to use it later

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
    pass # --- PROXY DISABLED ---
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
    text = decode_html_entities(text) # 2. Implement HTML Entity Decoding - USE decode_html_entities
    return text

# EMAIL_FILTER_SUFFIXES = ['.jpg', '.png', 'wixpress.com', 'sentry.io'] # Email filter suffixes - ADDED # Replaced with expanded list above
# FIX 5: Email Validation Overkill - Use more permissive regex
# EMAIL_PATTERN = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}(?:\.[A-Z|a-z]{2,})?\b' # REMOVED - No longer needed

def extract_emails(html_content, domain): # Updated extract_emails to use email-validator
    decoded_html_content = decode_emails(html_content)
    # Use a basic regex to find potential email-like strings (for initial extraction)
    potential_emails = list(set(re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', decoded_html_content, re.IGNORECASE)))

    valid_emails = []
    for email_candidate in potential_emails:
        try:
            email = validate_email(email_candidate, check_deliverability=False).normalized # Use email-validator for validation, deliverability check is computationally expensive and not strictly needed for extraction
            valid_emails.append(email) # If valid, append the normalized email
        except EmailNotValidError as e:
            logger.debug(f"Invalid email address found and filtered out: {email_candidate} - Reason: {e}") # Log invalid emails, but don't raise error

    # Filter emails by suffix (same filtering logic as before)
    valid_emails = [email for email in valid_emails if not any(email.lower().endswith(suffix) for suffix in EMAIL_FILTER_SUFFIXES)]

        # Heuristic filtering based on local part (before @) - ADDED THIS BLOCK (same as before)
    business_email_keywords = [
        'info', 'contact', 'sales', 'booking', 'reservations', 'admin', 'management', 'office', 'support', 'help', 'guest', 'rental', 'property'
    ]

    valid_emails = [
        email for email in valid_emails
        if any(keyword in email.lower().split('@')[0] for keyword in business_email_keywords) or  # Check for keywords in local part
           re.match(r'^[a-zA-Z]+\.[a-zA-Z]+', email.split('@')[0]) or # Check for name-like pattern (e.g., john.doe)
           re.match(r'^[a-zA-Z]+$', email.split('@')[0]) # Check for single word name-like pattern (e.g., john)
    ]
    # End of ADDED BLOCK


    print(f"----- Validated Email Matches (using email-validator) -----")
    print(valid_emails)
    logger.debug(f"Validated email matches: {valid_emails}") # DEBUG LOG - VALIDATED EMAILS
    return valid_emails


def extract_phone_numbers(text_content):
    phone_numbers = []
    for match in re.findall(r'[\+\(]?[1-9][0-9 \-\(\)]{7,}[0-9]', text_content): # Basic regex for phone numbers, consider improving
        try:
            # FIX 6: Phone Number Parsing - Use phonenumbers library for validation
            parsed_number = phonenumbers.parse(match, "US") # Default region to US, adjust as needed
            if phonenumbers.is_valid_number(parsed_number):
                formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164) # Format to E.164
                normalized_number = normalize_phone(formatted_number) # 1. Add Phone Normalization - NORMALIZE PHONE NUMBER
                phone_numbers.append(normalized_number) # Append the normalized number
        except phonenumbers.phonenumberutil.NumberParseException:
            logger.debug(f"Phone number parsing failed for: {match}") # Debug log for parse failures
            pass # Ignore if parsing fails, not a valid phone number

    # 3. Add Deduplication Logic - Deduplicate phone numbers
    unique_phones = list(set(phone_numbers))
    logger.debug(f"Extracted and validated phone numbers: {unique_phones}") # Debug log for validated numbers
    return unique_phones


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


def is_valid_url(url):
    """Basic validation to reject obviously invalid URLs."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) # Check for scheme and netloc
    except: # ADDED Error handling for URL parsing
        return False

# --- ADD validate_request FUNCTION HERE ---
def validate_request(url):
    """Basic validation to reject obviously invalid or unwanted URLs."""
    if not is_valid_url(url): # Use is_valid_url here
        logger.warning(f"Rejected as invalid URL: {url}")
        logger.debug(f"Validate Request Filter - Invalid URL: {url}") # DEBUG LOG - INVALID URL
        return False
    try: # ADDED try-except block - Error Handling Fix
        parsed_url = urlparse(url) # URL parsing moved inside try block
        if BAD_PATH_PATTERN.search(parsed_url.path):
            logger.warning(f"Rejected bad path: {url}")
            logger.debug(f"Validate Request Filter - Bad Path: {url} - Path: {parsed_url.path}") # DEBUG LOG - BAD PATH FILTER
            return False
    except Exception as e: # Catch URL parsing errors
        logger.error(f"URL parsing error: {str(e)}")
        return False

    # --- Improved Generic Domain Check - EXACT MATCH ---
    domain = urlparse(url).netloc.lower() # Extract domain using urlparse
    normalized_domain = domain.replace("www.", "") # Normalize by removing "www."
    logger.debug(f"DEBUG - validate_request: Checking domain: {normalized_domain} for URL: {url}") # DEBUG LOG - DOMAIN BEING CHECKED
    if normalized_domain in GENERIC_DOMAINS: # Check for EXACT domain match in GENERIC_DOMAINS
        logger.warning(f"Rejected due to generic domain: {url}, domain: {normalized_domain}") # ADDED DOMAIN TO WARNING
        logger.debug(f"Validate Request Filter - Generic Domain: {url} - Domain: {normalized_domain}") # DEBUG LOG - GENERIC DOMAIN FILTER
        metrics['crawl_errors'].labels(type='generic_domain', proxy='no_proxy').inc()
        logger.debug(f"DEBUG - validate_request: Domain {normalized_domain} NOT found in GENERIC_DOMAINS. Accepting.") # ADDED DEBUG LOG - ACCEPTANCE REASON

    # --- End Improved Generic Domain Check --- # Keep generic domain check

    return True


async def proxy_health_check(proxy_pool):
    pass # --- PROXY DISABLED ---
    """Check proxy health by testing connection to httpbin.org/ip"""
    while True:
        try:
            if not should_use_proxies(proxy_pool): # Skip health check if proxies are disabled
                await asyncio.sleep(300) # Check every 5 minutes if disabled
                continue

            test_url = "https://httpbin.org/ip"
            session = await get_session()
            for proxy in proxy_pool:
                try:
                    async with session.get(test_url, proxy=f"http://{proxy}", timeout=15) as response:
                        if response.status == 200:
                            PROXY_SCORES[proxy] = min(100, PROXY_SCORES.get(proxy, 0) + PROXY_SCORE_CONFIG['success'])
                        else:
                            PROXY_SCORES[proxy] = max(0, PROXY_SCORES.get(proxy, 0) + PROXY_SCORE_CONFIG['failure']) # Penalize on exception
                except Exception as e:
                    logger.warning(f"Proxy health check failed for {proxy}: {e}")
                    PROXY_SCORES[proxy] = max(0, PROXY_SCORES.get(proxy, 0) + PROXY_SCORE_CONFIG['failure']) # Penalize on exception
                if PROXY_SCORES[proxy] < 0: # Ensure score doesn't go below 0
                    PROXY_SCORES[proxy] = 0
                logger.debug(f"Health check score for {proxy}: {PROXY_SCORES[proxy]}") # Debug log for proxy health score

            await asyncio.sleep(3600) # Check every hour
        except Exception as e:
            logger.error(f"Proxy health check loop error: {str(e)}")
            await asyncio.sleep(600)

# --- Configuration ---
# ... existing configuration ...

MAX_DEPTH = 3  # Maximum depth for internal link following

# --- Utility Functions MOVED FROM crawler.py ---

def normalize_phone(number): # 1. Add Phone Normalization - IMPLEMENT FUNCTION
    return re.sub(r'\D', '', number).lstrip('1')

# Implement HTML Entity Decoding
def decode_html_entities(text): # 2. Implement HTML Entity Decoding - IMPLEMENT FUNCTION
    return html.unescape(text)