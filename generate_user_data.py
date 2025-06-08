import threading
import pandas as pd

from read_in_data import scrape_and_encode_user_ratings
from load_save_and_translate_data import (
    load_existing_users,
    load_user_update_log,
    load_existing_film_mappings,
    save_user_update_log,
    translate_ratings_dataframe
)
from config import (
    USER_MAPPINGS_PATH,
    FILM_MAPPINGS_PATH,
    USER_UPDATE_LOG_PATH,
)

def generate_user_data(username):
    """
    Generates raw and translated ratings files for a single Letterboxd user.

    This function:
        1. Loads existing mappings and update logs.
        2. Scrapes the specified user's ratings.
        3. Assigns numeric IDs to the user and any new films.
        4. Saves a raw file (with numeric IDs) and a translated file (with readable names).
        5. Updates and saves mapping files and the update log.

    Args:
        username (str): The Letterboxd username to scrape and process.
    """

    print(f"=== Generating ratings file for Letterboxd user: {username} ===")

    # === Load existing mapping and log files ===
    # These track all previously scraped users and film title mappings.
    existing_users, user_id_mapping, max_user_id = load_existing_users()
    film_id_mapping = load_existing_film_mappings()
    update_log = load_user_update_log()

    # Use a thread-safe lock to protect the max_user_id if reused in threaded contexts
    max_user_id_lock = threading.Lock()

    # === Scrape and encode user ratings ===
    # This function returns:
    #   - Encoded ratings (numeric user ID, film ID, rating)
    #   - Any new film mappings discovered
    #   - Updated user ID mapping
    #   - The new maximum user ID
    df_encoded, new_film_mappings, user_id_mapping, max_user_id = scrape_and_encode_user_ratings(
        user_string=username,
        user_id_map=user_id_mapping,
        film_id_map=film_id_mapping,
        max_user_id_lock=max_user_id_lock
    )

    # If the user had no valid ratings, abort early
    if df_encoded is None or df_encoded.empty:
        print(f"No valid ratings found for user: {username}")
        return

    # === Save the raw ratings file ===
    # This file uses numeric IDs (for consistency and anonymisation)
    raw_path = f"user_{username}_raw.csv"
    df_encoded.drop_duplicates(subset=["user_id", "film_id"], inplace=True)
    df_encoded.to_csv(raw_path, index=False)
    print(f"Saved raw user ratings to {raw_path} ({len(df_encoded)} rows)")

    # === Update mappings and translate the raw file ===
    film_id_mapping.update(new_film_mappings)  # Add any new film mappings found during scraping
    translated_df = translate_ratings_dataframe(df_encoded, user_id_mapping, film_id_mapping) #Use the updated mappings to translate the numeric IDs back to readable names

    # Save the translated file
    translated_path = f"user_{username}_translated.csv"
    translated_df.to_csv(f"user_{username}_translated.csv", index=False)
    print(f"Saved translated user ratings as {translated_path}")

    # === Update log of user updates ===
    # Log today's date as the most recent update for this user
    today = pd.Timestamp.now().normalize()
    update_log[username] = today
    save_user_update_log(update_log)

    # Save updated user and film ID mappings
    pd.DataFrame([(k, v) for k, v in user_id_mapping.items()], columns=["username", "numeric_user_id"]).to_csv(USER_MAPPINGS_PATH, index=False)
    pd.DataFrame([(k, v) for k, v in film_id_mapping.items()], columns=["film_id", "film_title"]).to_csv(FILM_MAPPINGS_PATH, index=False)

    print("Mappings and update log saved successfully.")



if __name__ == "__main__":
    user_name = "ma7cus"
    generate_user_data(user_name) 