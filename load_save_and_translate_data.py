import os
import pandas as pd
from datetime import datetime
from config import (
    RAW_RATINGS_PATH,
    TRANSLATED_RATINGS_PATH,
    USER_MAPPINGS_PATH,
    FILM_MAPPINGS_PATH,
    RAW_RATINGS_PREFIX,
    TRANSLATED_RATINGS_PREFIX,
    USER_MAPPINGS_PREFIX,
    FILM_MAPPINGS_PREFIX,
    USER_UPDATE_LOG_PATH,
)

def load_existing_users():
    """
    Load the current user mapping from disk.

    Returns:
        tuple: (set of usernames, username → user_id dict, max user_id)
    """
    #Try to read in the user mapping file
    input_path = USER_MAPPINGS_PATH
    
    #If the file doesn't exist, print an error message and return empty values
    if not os.path.exists(input_path):
        print("User mapping file not found.")
        return set(), {}, 0

    try:

        #Read in the user mapping file
        df_user_ids = pd.read_csv(input_path)

        #Check if the user mapping file is empty or missing required columns
        if df_user_ids.empty or "numeric_user_id" not in df_user_ids.columns or "username" not in df_user_ids.columns:
            print("User mapping file is empty or missing required columns.")
            return set(), {}, 0

        #Drop any rows with missing values in the username or numeric_user_id columns and convert the numeric_user_id column to integers
        df_user_ids = df_user_ids.dropna(subset=["username", "numeric_user_id"])
        df_user_ids["numeric_user_id"] = df_user_ids["numeric_user_id"].astype(int)

        #Convert the dataframe to a dictionary mapping usernames to numeric user IDs
        user_id_mapping = dict(zip(df_user_ids["username"], df_user_ids["numeric_user_id"]))
        
        #Define the maximum assigned user ID so a new assigning knows where to start from
        max_user_id = df_user_ids["numeric_user_id"].max()

        return set(user_id_mapping.keys()), user_id_mapping, max_user_id
    
    #Return empty values if there was an error reading the file
    except Exception as e:
        print(f"Error reading user mappings: {e}")
        return set(), {}, 0

def load_existing_film_mappings():
    if os.path.exists(FILM_MAPPINGS_PATH):
        df = pd.read_csv(FILM_MAPPINGS_PATH)
        return dict(zip(df["film_id"].astype(str), df["film_title"]))
    return {}

def load_user_update_log():
    """
    Load the user update log from disk.

    Returns:
        dict: Mapping from username → last update timestamp
    """
    if os.path.exists(USER_UPDATE_LOG_PATH):
        df = pd.read_csv(USER_UPDATE_LOG_PATH)
        return dict(zip(df["username"], pd.to_datetime(df["last_updated"])))
    return {}

def save_user_update_log(update_log):
    """
    Save the user update timestamps to disk.

    Args:
        update_log (dict): Mapping from username → last update timestamp
    """
    df = pd.DataFrame([
        {"username": u, "last_updated": d.strftime("%Y-%m-%d")} for u, d in update_log.items()
    ])
    df.to_csv(USER_UPDATE_LOG_PATH, index=False)

def translate_ratings_dataframe(df, user_id_mapping, film_id_mapping):
    """
    Translates numeric user IDs and film IDs in a ratings DataFrame into readable usernames and film titles.

    Args:
        df (pd.DataFrame): DataFrame with columns ['user_id', 'film_id', 'rating'].
        user_id_mapping (dict): Mapping from username (str) → numeric ID (int).
        film_id_mapping (dict): Mapping from film_id (str or int) → film_title (slug or string).

    Returns:
        pd.DataFrame: A new DataFrame with columns ['username', 'film_title', 'rating'].
    """
    # Invert the user_id_mapping: numeric ID → username
    user_id_to_name = {int(v): k for k, v in user_id_mapping.items()}

    # Extract the film_id mapping: film_id → film_title
    film_id_to_title = {str(k): v for k, v in film_id_mapping.items()}

    # Read in the existing data to the correct type
    translated_df = df.copy()
    translated_df["user_id"] = translated_df["user_id"].astype(int)
    translated_df["film_id"] = translated_df["film_id"].astype(str)

    # Map numeric IDs back to usernames and film titles
    translated_df["username"] = translated_df["user_id"].map(user_id_to_name)
    translated_df["film_title"] = translated_df["film_id"].map(film_id_to_title)

    # Return the translated view (reordering columns)
    return translated_df[["username", "film_title", "rating"]]

def save_csv_versions(df, latest_path, prefix, timestamp, versioned=True):
    """
    Saves the full batch of scraped and encoded data from the current session.

    This function outputs four types of files:
        - Raw user ratings: always saved, with an optional timestamped version if `versioned=True`.
        - Translated ratings (with usernames and film titles): always saved, overwriting the existing file.
        - User mappings (username → numeric user_id): always saved, overwriting the existing file.
        - Film mappings (film_id → title/slug): always saved, overwriting the existing file.

    Args:
        df_encoded_values (DataFrame): DataFrame with columns ['user_id', 'film_id', 'rating'].
        film_id_to_title (dict): Mapping from film_id to film_title (slug).
        user_id_mapping (dict): Mapping from username to numeric user_id.
        versioned (bool): If True, saves a timestamped copy of the raw user ratings file for historical reference.
                          All other files are always overwritten.
    """
    print(f"Saving latest version to {latest_path}...")
    df.to_csv(latest_path, index=False)

    if versioned:
        output_dir = os.path.dirname(latest_path)
        versioned_path = os.path.join(output_dir, f"{prefix}_{timestamp}.csv")
        print(f"Saving versioned file to {versioned_path}...")
        df.to_csv(versioned_path, index=False)

def save_final_data(df_encoded_values, film_id_to_title, user_id_mapping, versioned=True):
    """
    Saves the raw and translated user ratings dataset, along with lookup tables.

    Args:
        df_encoded_values (DataFrame): DataFrame with user_id, film_id, rating.
        film_id_to_title (dict): Mapping from film_id to film_title (slug).
        user_id_mapping (dict): Mapping from username to numeric user_id.
        versioned (bool): Whether to save timestamped versions.
    """
    print("Starting data saving routine...")
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    # === RAW RATINGS ===
    save_csv_versions(df_encoded_values, RAW_RATINGS_PATH, RAW_RATINGS_PREFIX, timestamp, versioned)

    # === TRANSLATED RATINGS ===
    translated_data = translate_ratings_dataframe(df_encoded_values, user_id_mapping, film_id_to_title)
    save_csv_versions(translated_data, TRANSLATED_RATINGS_PATH, TRANSLATED_RATINGS_PREFIX, timestamp, False)

    # === FILM MAPPINGS ===
    film_mappings_df = pd.DataFrame(
        [(str(fid), title) for fid, title in film_id_to_title.items()],
        columns=["film_id", "film_title"]
    )
    save_csv_versions(film_mappings_df, FILM_MAPPINGS_PATH, FILM_MAPPINGS_PREFIX, timestamp, False)

    # === USER MAPPINGS ===
    user_mappings_df = pd.DataFrame(
        [(username, uid) for username, uid in user_id_mapping.items()],
        columns=["username", "numeric_user_id"]
    )
    save_csv_versions(user_mappings_df, USER_MAPPINGS_PATH, USER_MAPPINGS_PREFIX, timestamp, False)

    print("All data saved successfully.")
