from serpapi import GoogleSearch
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()

async def test_serpapi():
    api_key = os.getenv("SERPAPI_API_KEY")
    params = {
        "engine": "google",
        "q": "Austin Vacation Rentals",
        "gl": "us",
        "hl": "en",
        "num": 10,
        "api_key": api_key,
        "async": True
    }
    search = GoogleSearch(params)
    initial_result = search.get_dict()
    
    if 'search_metadata' not in initial_result:
        print("Initial search failed:", initial_result)
        return False

    search_id = initial_result['search_metadata']['id']
    print(f"Search submitted. ID: {search_id}. Waiting 5 seconds...")
    await asyncio.sleep(5)  # Increased to 25 seconds
    
    # Retrieve with API key
    archived_search = GoogleSearch({"api_key": api_key}).get_search_archive(search_id)
    
    if 'error' in archived_search:
        print(f"Archive Error: {archived_search['error']}")
        return False
        
    if 'organic_results' in archived_search:
        print(f"Found {len(archived_search['organic_results'])} results")
        return True
        
    print("No organic results found")
    return False

async def main():
    await test_serpapi()

if __name__ == "__main__":
    asyncio.run(main())
