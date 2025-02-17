# proxy_test_crawler.py - Corrected Proxy Functionality Test Script

import asyncio
import aiohttp
import os
from dotenv import load_dotenv
import logging
from collections import defaultdict
import random
from random import choices # ADDED: Explicitly import choices from random
from itertools import cycle
from utils import get_session, close_session, PROXY_SCORES, rotate_proxy, update_proxy_score # Import proxy related functions from utils

load_dotenv()

LOG_LEVEL = logging.DEBUG  # Set log level for testing
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

USE_PROXY = os.getenv("USE_PROXY", "False").lower() == "true"
MAX_PROXY_RETRIES = 3  # Number of times to retry with a new proxy

proxy_pool_to_use = None
proxy_cycle = None

# --- Copy get_proxy_pool() function from your utils.py ---
BRIGHTDATA_PORTS = [22225, 22226]
BRIGHTDATA_GEO = ["us-east", "us-west"]

def get_proxy_pool():
    proxies = []
    provider_counts = {
        'oxylabs': 0,
        'smartproxy': 0,
        'brightdata': 0
    }

    if os.getenv('OXLABS_USER') and os.getenv('OXLABS_PASS') and random.random() < 0.7:
        proxies.append(f"{os.getenv('OXLABS_USER')}:{os.getenv('OXLABS_PASS')}@pr.oxylabs.io:7777")
        provider_counts['oxylabs'] += 1

    if os.getenv('SMARTPROXY_USER') and os.getenv('SMARTPROXY_PASS') and random.random() < 0.3:
        proxies.append(f"{os.getenv('SMARTPROXY_USER')}:{os.getenv('SMARTPROXY_PASS')}@gate.smartproxy.com:7000")
        provider_counts['smartproxy'] += 1

    if os.getenv('BRIGHTDATA_USER') and os.getenv('BRIGHTDATA_PASS'):
        for geo in BRIGHTDATA_GEO:
            for port in BRIGHTDATA_PORTS:
                proxies.append(f"{os.getenv('BRIGHTDATA_USER')}:{os.getenv('BRIGHTDATA_PASS')}@brd.superproxy.io:{port}")
                provider_counts['brightdata'] += 1

    logger.info(f"Proxy rotation: {provider_counts}")
    healthy_proxies = [p for p, s in PROXY_SCORES.items() if s > 20]
    if not all([os.getenv('BRIGHTDATA_USER'), os.getenv('BRIGHTDATA_PASS')]):
        logger.error("Missing Bright Data credentials - check .env file")
    if healthy_proxies:
        logger.debug(f"Using healthy proxies, count: {len(healthy_proxies)}")
        return healthy_proxies
    else:
        logger.warning("No healthy proxies found, using refreshed pool.")
        logger.debug("No healthy proxies found, using fresh pool (limited size).")
        weights = [
            3 if "oxylabs" in p else
            2 if "brightdata" in p else
            1 for p in proxies
        ]
        return choices(proxies, weights=weights, k=min(len(proxies), 25))
# --- End copied get_proxy_pool() ---


async def crawl_site_via_proxy(session, proxy_pool):
    """Crawls a test site (ipify.org) through a proxy, with retries."""
    test_url = "https://api.ipify.org?format=json" # Simple IP address API

    for attempt in range(MAX_PROXY_RETRIES):
        current_proxy = rotate_proxy(proxy_pool)
        proxy_info = current_proxy if current_proxy else "No Proxy"
        logger.info(f"Attempt {attempt + 1}: Crawling {test_url} via proxy: {proxy_info}")

        try:
            if current_proxy:
                proxy_url = f"http://{current_proxy}"
            else:
                proxy_url = None

            async with session.get(test_url, proxy=proxy_url, timeout=15) as response:
                status_code = response.status
                response_json = await response.json()
                public_ip = response_json.get('ip', 'IP not found')

                if status_code == 200:
                    logger.info(f"  Success! Status: {status_code}, Proxy: {proxy_info}, IP: {public_ip}")
                    update_proxy_score(current_proxy, True) # Reward proxy
                    return True, proxy_info, public_ip
                else:
                    logger.warning(f"  Request failed. Status: {status_code}, Proxy: {proxy_info}")
                    update_proxy_score(current_proxy, False) # Penalize proxy
                    return False, proxy_info, status_code

        except aiohttp.ClientError as e:
            logger.error(f"  aiohttp ClientError: {e}, Proxy: {proxy_info}")
            update_proxy_score(current_proxy, False) # Penalize proxy
        except asyncio.TimeoutError:
            logger.error(f"  Asyncio Timeout for proxy: {proxy_info}")
            update_proxy_score(current_proxy, False) # Penalize proxy
        except Exception as e:
            logger.exception(f"  Unexpected error with proxy: {proxy_info}")
            update_proxy_score(current_proxy, False) # Penalize proxy

    logger.error(f"  Max retries reached, crawl failed.")
    return False, "All Proxies Failed", "N/A"


async def main():
    global proxy_pool_to_use, proxy_cycle

    print("--- Proxy Crawler Test Started ---")
    print(f"USE_PROXY from env: {USE_PROXY}")

    proxy_pool_to_use = get_proxy_pool() # Initialize proxy pool
    print(f"PROXY_POOL from utils: {proxy_pool_to_use}")
    if USE_PROXY and proxy_pool_to_use:
        proxy_cycle = cycle(proxy_pool_to_use) # Initialize proxy cycle only if proxies are used
        print("Proxy rotation enabled.")
    else:
        print("Proxy usage disabled or no proxies configured.")

    session = await get_session() # Get session from pool

    if USE_PROXY and proxy_pool_to_use:
        num_test_crawls = min(len(proxy_pool_to_use) * 2, 10) # Test crawl count - capped at 10
    else:
        num_test_crawls = 3 # Test crawl count if no proxy

    crawl_results = []
    for i in range(num_test_crawls):
        print(f"\n--- Test Crawl {i+1}/{num_test_crawls} ---")
        if USE_PROXY and proxy_pool_to_use:
            result, proxy_used, details = await crawl_site_via_proxy(session, proxy_pool_to_use)
        else:
            result, proxy_used, details = await crawl_site_via_proxy(session, None) # Pass None for no proxy
        crawl_results.append({'result': result, 'proxy': proxy_used, 'details': details})
        await asyncio.sleep(2) # Small delay between requests

    await close_session() # Close session pool

    print("\n--- Crawl Test Summary ---")
    for res in crawl_results:
        print(f"Result: {'Success' if res['result'] else 'Failure'}, Proxy: {res['proxy']}, Details: {res['details']}")

    print("\n--- Proxy Scores ---")
    print(PROXY_SCORES) # Print final proxy scores


if __name__ == "__main__":
    asyncio.run(main())