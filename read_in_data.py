import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from fetch_ratings import scrape_letterboxd_in_parallel
import os

def process_user_data(user_id, user_id_mapping, film_id_mapping):
    """
    Fetches a user's Letterboxd ratings and ensures consistent data formatting.

    Arguments:
    - user_id (str): The Letterboxd username.
    - user_id_mapping (dict): A dictionary tracking assigned numeric user IDs.
    - film_id_mapping (dict): A dictionary mapping film_id → film_title.

    Returns:
    - df_processed (DataFrame): A DataFrame containing (numeric_user_id, film_id, rating).
    - new_film_mappings (dict): A dictionary containing new film_id → film_title mappings found during this user’s processing.
    - updated_user_mapping (dict): Updated dictionary mapping username → numeric user_id.
    """

    # Fetch user ratings from Letterboxd
    df = scrape_letterboxd_in_parallel(user_id)  

    # Assign a unique numeric ID to the user if not already assigned
    if user_id not in user_id_mapping:
        user_id_mapping[user_id] = len(user_id_mapping) + 1  

    numeric_user_id = user_id_mapping[user_id]  # Retrieve assigned numeric ID

    # Local dictionary to store new film mappings (prevents thread collisions)
    new_film_mappings = {}

    # Process film mappings safely
    for _, row in df.iterrows():
        film_id, film_title = row["film_id"], row["film_title"]

        if film_id not in film_id_mapping:  # If film is not already mapped, store it
            new_film_mappings[film_id] = film_title

    # Prepare processed DataFrame with numeric user_id, film_id, and rating
    df_processed = df[["film_id", "rating"]].copy()
    df_processed.insert(0, "user_id", numeric_user_id)  # Add numeric user_id as first column

    return df_processed, new_film_mappings, user_id_mapping


def save_final_data(combined_ratings, film_id_to_title, user_id_mapping, output_dir="."):
    """
    Saves the final dataset in both translated and untranslated formats.

    Arguments:
    - combined_ratings (DataFrame): The DataFrame containing user_id, film_id, and ratings.
    - film_id_to_title (dict): Dictionary mapping film_id → film_title.
    - user_id_mapping (dict): Dictionary mapping username → numeric user_id.
    - output_dir (str): Directory to store the output files (default is current directory).
    """

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Save raw (untranslated) ratings data
    raw_path = os.path.join(output_dir, "raw_user_ratings.csv")
    combined_ratings.to_csv(raw_path, index=False)
    print(f"\nRaw user ratings saved to {os.path.abspath(raw_path)}")

    # Translate film IDs to film titles
    combined_ratings["film_title"] = combined_ratings["film_id"].map(film_id_to_title)

    # Reverse dictionary: Convert numeric user IDs back to usernames
    user_id_to_name_reversed = {v: k for k, v in user_id_mapping.items()}
    combined_ratings["username"] = combined_ratings["user_id"].map(user_id_to_name_reversed)

    # Save translated ratings data
    translated_path = os.path.join(output_dir, "translated_user_ratings.csv")
    translated_data = combined_ratings[["username", "film_title", "rating"]]
    translated_data.to_csv(translated_path, index=False)
    print(f"\nTranslated user ratings saved to {os.path.abspath(translated_path)}")

    # Save the mapping dictionaries
    film_mappings_path = os.path.join(output_dir, "film_mappings.csv")
    user_mappings_path = os.path.join(output_dir, "user_mappings.csv")

    pd.DataFrame(list(film_id_to_title.items()), columns=["film_id", "film_title"]).to_csv(film_mappings_path, index=False)
    pd.DataFrame(list(user_id_mapping.items()), columns=["username", "numeric_user_id"]).to_csv(user_mappings_path, index=False)

    print(f"\nFilm ID mappings saved to {os.path.abspath(film_mappings_path)}")
    print(f"\nUser ID mappings saved to {os.path.abspath(user_mappings_path)}")


def fetch_all_user_data(users):
    """
    Fetches and combines movie rating data for multiple users.

    Arguments:
    - users (list): A list of Letterboxd usernames to fetch data for.
    - parallel (bool): Whether to process users in parallel.

    Returns:
    - combined_ratings (DataFrame): A merged DataFrame with (user_id, film_id, rating).
    - film_id_to_title (dict): A dictionary mapping film_id → film_title.
    - user_id_mapping (dict): A dictionary mapping username → numeric user_id.
    """

    all_ratings = []  # Stores all users' ratings
    film_id_to_title = {}  # Maps film_id → film_title
    user_id_mapping = {}  # Maps username → numeric user_id

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_user_data, user, user_id_mapping, film_id_to_title): user for user in users}

        for future in as_completed(futures):
            user_id = futures[future]
            try:
                df, new_film_mappings, updated_user_mapping = future.result()
                
                if df is not None:
                    all_ratings.append(df)  # Append processed ratings DataFrame
                    
                    # Safely update global film mapping dictionary
                    film_id_to_title.update(new_film_mappings)
                    
                    # Update user mapping dictionary
                    user_id_mapping.update(updated_user_mapping)  
                    
            except Exception as e:
                print(f"Error processing {user_id}: {e}")

    # Combine all user rating data into a single DataFrame
    combined_ratings = pd.concat(all_ratings, ignore_index=True) if all_ratings else pd.DataFrame(columns=["user_id", "film_id", "rating"])

    print("\nFinal Combined Ratings DataFrame (First 5 Rows):")
    print(combined_ratings.head())

    end_time = time.time()
    print(f"\nProcessed {len(users)} users in {end_time - start_time:.2f} seconds.")

    save_final_data(combined_ratings, film_id_to_title, user_id_mapping)

    return combined_ratings, film_id_to_title, user_id_mapping





