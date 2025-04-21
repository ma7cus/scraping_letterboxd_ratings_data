import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed  # For parallel processing

def parse_rating(raw_rating):
    """
    Converts a raw rating string (e.g., '★★★½') to a numeric value (e.g., 3.5).
    
    Explanation:
    - Counts the number of '★' characters (full stars).
    - Checks if '½' exists in the string (adds 0.5 if present).
    - Returns None if no rating is found.
    """
    if raw_rating:
        full_stars = raw_rating.count("★")
        half_star = 0.5 if "½" in raw_rating else 0
        return full_stars + half_star
    return None  # No rating found

def scrape_user_ratings_page(session, url):
    """
    Parses a single Letterboxd 'watched films' page to extract film metadata.

    Expects HTML structure like:
    <li class="poster-container film-watched">
        <div class="film-poster" data-film-slug="nosferatu-2024" data-film-id="35995" ...>
        ...
        <p class="poster-viewingdata">
            <span class="rating rated-8">★★★★</span>
        </p>
    </li>

    Extracts:
    - film_slug (e.g. 'nosferatu-2024')
    - film_id (e.g. '35995')
    - rating (e.g. 4.0, converted from star symbols)

    Args:
        session (requests.Session): Persistent HTTP session
        url (str): URL of the film listing page

    Returns:
        list[list]: List of [film_slug, film_id, rating] triples
                    Returns None if the page is empty or invalid

    """

    response = session.get(url)

    # If the page is invalid (e.g., non-existent), return None
    if response.status_code != 200:
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    film_items = soup.find_all("li", class_="poster-container")

    # If no films are found on this page, return None (used to stop scraping)
    if not film_items:
        return None

    page_film_data = []  # Store extracted films on this page

    for film in film_items:
        film_poster_div = film.find("div", class_="film-poster")
        rating_container = film.find("p", class_="poster-viewingdata")

        if film_poster_div:
            # Extract film name (unique slug) and film ID
            film_name = film_poster_div.get("data-film-slug", "Unknown")
            film_id = film_poster_div.get("data-film-id", "Unknown")
            
            # Extract rating and convert to numeric value
            raw_rating = rating_container.text.strip() if rating_container else None
            numeric_rating = parse_rating(raw_rating) if raw_rating else None

            # Append extracted data to this page's list if there is a valid rating associated
            if numeric_rating is not None:
                page_film_data.append([film_name, film_id, numeric_rating])

    return page_film_data  # Return extracted films for this page

def scrape_user_ratings_pages_in_parallel(username, max_workers=5, batch_size=5):
    """
    Scrapes a Letterboxd user's watched films efficiently using parallel processing.

    Arguments:
    - username (str): The Letterboxd username whose films will be scraped.
    - max_workers (int): The number of parallel threads fetching pages simultaneously.
    - batch_size (int): The number of pages to request in parallel before waiting for results.

    How Parallel Processing Works:
    - Instead of fetching pages one by one, the function:
        1. **Submits multiple page requests at once** (first loop).
        2. **Processes pages as soon as they return results** (second loop).
        3. **Stops when multiple consecutive pages are empty** (pagination check).
        4. **Writes collected data to CSV efficiently in one go**.

    This speeds up scraping significantly compared to sequential execution.
    """

    start_time = time.time()  # Start timing for performance measurement
    base_url = f"https://letterboxd.com/{username}/films/by/date/"
    page_number = 1 
    film_data = []  # List to store all scraped films

    # Use a persistent session to reuse HTTP connections (reduces network overhead)
    session = requests.Session()

    # Create a ThreadPoolExecutor to manage multiple parallel requests
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:  # This loop continues requesting pages until no more films are found

            futures = {}  # Dictionary to track submitted requests

            # ===================== STAGE 1: SUBMITTING PAGE REQUESTS =====================
            # This loop submits up to `batch_size` requests at once
            for _ in range(batch_size):
                url = f"{base_url}page/{page_number}/"  # Construct the URL for the current page
                future = executor.submit(scrape_user_ratings_page, session, url)  # Submit request to run in parallel
                futures[future] = page_number  # Store the future along with its corresponding page number
                page_number += 1  # Move to the next page

            # ===================== STAGE 2: PROCESSING RETURNED RESULTS =====================
            empty_pages = 0  # Tracks consecutive pages returning no films

            # This loop processes pages as soon as they complete (instead of waiting for all in sequence)
            for future in as_completed(futures):
                page_result = future.result()  # Retrieve the data from the completed request

                if page_result:  # If the page contains films
                    film_data.extend(page_result)  # Append the films to the main dataset
                else:
                    empty_pages += 1  # Count empty pages to determine stopping condition

            # ===================== STAGE 3: CHECK FOR END OF PAGINATION =====================
            # Stop fetching if multiple consecutive pages are empty (indicates no more films exist)
            if empty_pages >= batch_size:
                break  # Exit the loop when pagination has ended

    # Close the session to free up resources
    session.close()

    df = pd.DataFrame(film_data, columns=["film_slug", "film_id", "rating"])

    # Print summary of execution time and number of films processed
    end_time = time.time()
    total_time = end_time - start_time
    print(f"\nProcessed {len(film_data)} films in {total_time:.2f} seconds using {max_workers} threads.")

    return df

   