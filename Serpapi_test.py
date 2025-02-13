from serpapi import GoogleSearch
import os
from dotenv import load_dotenv
load_dotenv() # Load .env variables

def test_serpapi():
    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise EnvironmentError("SERPAPI_API_KEY environment variable not set in .env")

    params = {
        "engine": "google",
        "q": "Austin Vacation Rentals",
        "gl": "us",
        "hl": "en",
        "num": 10,
        "api_key": api_key
    }

    search = GoogleSearch(params)
    results = search.get_dict()

    if 'organic_results' in results and results['organic_results']:
        print("SerpAPI Direct Test: SUCCESS - Organic results found!")
        print(f"Number of organic results: {len(results['organic_results'])}")
        # Optionally print the first result to inspect
        # print(results['organic_results'][0])
        return True
    else:
        print("SerpAPI Direct Test: FAILURE - No organic results.")
        print("Full SerpAPI Response (for debugging):")
        print(results)
        return False

if __name__ == "__main__":
    test_serpapi()