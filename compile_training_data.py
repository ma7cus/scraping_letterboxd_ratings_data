import os
import time
import requests
import pandas as pd
import random
from bs4 import BeautifulSoup
from read_in_data import fetch_all_user_data
from load_save_and_translate_data import load_existing_users, load_user_update_log, save_final_data, save_user_update_log, load_existing_film_mappings
from requests.exceptions import RequestException
from config import (
    RAW_RATINGS_PATH,
    VERSIONING_TOGGLE,
    HEADERS
)


def get_url_with_retries(url, max_retries=5, base_delay=2.0):
    """
    Attempt to fetch a URL with exponential backoff and jitter on failure.

    Args:
        url (str): The target URL.
        max_retries (int): Maximum number of retries.
        base_delay (float): Base delay for exponential backoff.

    Returns:
        requests.Response: The successful response object.
    """
    for attempt in range(max_retries): #Try to get the URL up to max_retries times
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)  #Attempt to get the URL, waiting up to 10 seconds for a response
            if response.status_code == 200: #If the responsse is OK, return the response
                return response
            else:
                print(f"HTTP {response.status_code} for URL: {url}") #If the url was accessible but not OK for some reason, print the error code
        except RequestException as e:
            print(f"Request failed (attempt {attempt+1}/{max_retries}): {e}") #If the request failed, print the error message
        
        #After anything other than a 200 response, wait for a random time before retrying
        #The wait time is calculated using exponential backoff with jitter
        #This means that the wait time increases exponentially with each failed attempt, with some randomness added
        #This is to avoid overwhelming the server with requests
        sleep_time = random.uniform(0, base_delay * (2 ** attempt))
        print(f"Retrying in {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

    # If all retries fail, print an error message and exit the program    
    print("All retry attempts failed for URL. Aborting program.")
    exit(1)


def fetch_new_popular_users(existing_user_set, number_of_new_users, base_url="https://letterboxd.com/members/popular/"):
    """
    Scrape new usernames from Letterboxd's popular members pages.

    Args:
        existing_user_set (set): Already collected usernames.
        number_of_new_users (int): Desired number of new users.
        base_url (str): Base URL for popular members listing.

    Returns:
        list: Newly discovered usernames.
    """
    new_users = []
    page_number = 1

    while len(new_users) < number_of_new_users:
        # Construct URL: first page uses base_url, later pages use page/{n}/ format
        url = f"{base_url}page/{page_number}/" if page_number > 1 else base_url
        response = get_url_with_retries(url)

        if not response:
            print("Failed to fetch user list page. Aborting batch early.")
            break

        soup = BeautifulSoup(response.content, "html.parser")

        # === HTML Structure Inspection (as of April 2025) ===
        # Each page contains a <section> element with class "section col-17 col-main"
        # Inside this section is a <table> with class "person-table"
        # Each user is represented by a <tr> row inside the table's <tbody>
        # Within each <tr>, the username is located inside an <h3 class="title-3"> tag,
        # which itself contains an <a> tag with href="/username/"

        section = soup.select_one("section.section.col-17.col-main")  # Outer container
        table = section.select_one("table.person-table tbody") if section else None  # User listing table

        if not table:
            print("Could not find expected HTML structure.")
            break

        # Iterate over each <tr> (user row)
        for row in table.find_all("tr"):
            # Extract <a href="/username/"> from within <h3 class="title-3">
            tag = row.select_one("h3.title-3 a[href]")
            if tag:
                # Extract the actual username from the href value (e.g., "/alice/" → "alice")
                username = tag["href"].strip("/").split("/")[0]

                # Only add users we haven't seen before
                if username not in existing_user_set:
                    new_users.append(username)
                    existing_user_set.add(username)

                    # Stop early if we've collected enough new users
                    if len(new_users) >= number_of_new_users:
                        break

        # Go to next page of the popular members list
        page_number += 1

        # Wait a random short delay between pages to avoid hammering the server
        time.sleep(random.uniform(0.5, 1.5))

    return new_users


def run_full_batch_scraping_method(num_batches: int, batch_size: int, mode: str = "continue"):
    """
    Run the full scraping pipeline across multiple batches of users.

    Args:
        num_batches (int): Number of user batches to process.
        batch_size (int): Number of users per batch.
        mode (str): 'new' to start fresh; 'continue' to append to existing data.
    """
    # === STAGE 1: Load current state ===
    # Load previously saved user mappings and most recent numeric user ID
    existing_users, user_id_mapping, max_user_id = load_existing_users()
    
    # Load record of when each user was last updated
    update_log = load_user_update_log()

    # === STAGE 2: Initialise or resume dataset ===
    if mode == "new":
        print("Starting new dataset")
        user_id_mapping = {}  # Reset user ID mapping
        existing_users = set()  # Reset existing users
        max_user_id = 0  # Reset max user ID

        # Create a fresh, empty ratings DataFrame
        current_users_ratings_df = pd.DataFrame({
            "user_id": pd.Series(dtype="int"),
            "film_id": pd.Series(dtype="str"),
            "rating": pd.Series(dtype="float")
        })
        film_id_to_title = {}
    else:
        print(f"Resuming from max user ID {max_user_id}")
        # Load existing raw ratings if available, otherwise create an empty one
        current_users_ratings_df = pd.read_csv(RAW_RATINGS_PATH) if os.path.exists(RAW_RATINGS_PATH) else pd.DataFrame(columns=["user_id", "film_id", "rating"])
        film_id_to_title = load_existing_film_mappings()   

    # === STAGE 3: Process batches of new users ===
    for i in range(num_batches):
        print(f"\n[Batch {i+1}/{num_batches}] Fetching {batch_size} new users")

        # Pull new unique usernames from Letterboxd's 'Popular Members' pages
        new_users = fetch_new_popular_users(existing_users, batch_size)

        if not new_users:
            print("No more new users found. Stopping early.")
            break

        # Scrape user ratings and update mappings in parallel
        batch_ratings, batch_film_map, _, new_max_user_id = fetch_all_user_data(
            new_users, user_id_mapping, max_user_id
        )

        # Update global max user ID to reflect any newly assigned users
        max_user_id = new_max_user_id

        # === STAGE 4: Update tracking metadata ===
        film_id_to_title.update(batch_film_map)  # Extend film mapping with new films
        today = pd.Timestamp.now().normalize()
        for user in new_users:
            update_log[user] = today  # Log the update time for this user
        existing_users.update(new_users)  # Track user as now processed

        # Add batch ratings to the cumulative dataset
        if not batch_ratings.empty:
            current_users_ratings_df = pd.concat([current_users_ratings_df, batch_ratings], ignore_index=True)

        
        # Drop any duplicated rows from combined ratings
        current_users_ratings_df.drop_duplicates(subset=["user_id", "film_id"], inplace=True)

        # === STAGE 5: Overwrite existing 'latest' files ===
        # This saves the updated data, mappings, and logs to CSV
        save_final_data(current_users_ratings_df, film_id_to_title, user_id_mapping, VERSIONING_TOGGLE)
        save_user_update_log(update_log)

if __name__ == "__main__":
    mode = "new"  # Set to "new" to start from scratch
    run_full_batch_scraping_method(num_batches=2, batch_size=10, mode=mode)