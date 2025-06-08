import os

# === DIRECTORY SETUP ===

DEFAULT_OUTPUT_DIR = "data_output" # This is where your main data gets saved — ratings, mappings, logs, everything.
TEST_OUTPUT_DIR = "data_output/test" # If you’re just trying something out and don’t want to risk overwriting good data, this is your sandbox.
TEST_MODE = False # Toggle this to True when testing new code. It switches everything (ratings, logs, mappings) to save to TEST_OUTPUT_DIR instead of overwriting in the main directory.
OUTPUT_DIR = TEST_OUTPUT_DIR if TEST_MODE else DEFAULT_OUTPUT_DIR


# === Subdirectories for functional separation ===
TRAINING_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "training")
USER_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "users")
MAPPINGS_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "mappings")

# === Ensure output folders exist ===
os.makedirs(TRAINING_OUTPUT_DIR, exist_ok=True)
os.makedirs(USER_OUTPUT_DIR, exist_ok=True)
os.makedirs(MAPPINGS_OUTPUT_DIR, exist_ok=True)


# === FILENAMES  ===
# These filenames define both the input and output paths.
# When a batch is run, the code will always overwrite these files.
# Only RAW_RATINGS_FILENAME can be versioned at the moment. If versioning is enabled, timestamped backup is saved each run as well as overwriting the latest version.

RAW_RATINGS_FILENAME = "latest_raw_user_ratings.csv"       # Ratings stored with numeric user and film IDs
TRANSLATED_RATINGS_FILENAME = "latest_translated_user_ratings.csv"  # Same ratings, but with readable usernames and titles
USER_MAPPINGS_FILENAME = "latest_user_mappings.csv"               # Links usernames → numeric IDs
FILM_MAPPINGS_FILENAME = "latest_film_mappings.csv"               # Links film IDs → titles/slugs
USER_UPDATE_LOG_FILENAME = "latest_user_updates.csv"         # Tracks when each user was last scraped

# === TRAINING DATA PATHS ===

# These point to the same locations as the "latest" files, but can be used for clearer intent in scripts
TRAINING_RAW_RATINGS_PATH = os.path.join(TRAINING_OUTPUT_DIR, RAW_RATINGS_FILENAME)
TRAINING_TRANSLATED_RATINGS_PATH = os.path.join(TRAINING_OUTPUT_DIR, TRANSLATED_RATINGS_FILENAME)

# === PREFIXES FOR VERSIONING ===

# These are used when saving timestamped backup files (e.g. raw_user_ratings_20250420-184012.csv).
# The ‘latest_’ files are overwritten each run, but these backups can help inspect the output of each batch

#Versioning toggle
VERSIONING_TOGGLE = True # Set to True to save timestamped versions of the raw ratings file.

#Currently versioned files
RAW_RATINGS_PREFIX = "raw_ratings"

#Not currently versioned files (you'd have to amend the code in save_final_data in load_save_and_translate_data.py to use these)
TRANSLATED_RATINGS_PREFIX = "translated_ratings"
USER_MAPPINGS_PREFIX = "user_mappings"
FILM_MAPPINGS_PREFIX = "film_mappings"

# === FULL PATHS FOR FILE READ/WRITE ===
RAW_RATINGS_PATH = os.path.join(TRAINING_OUTPUT_DIR, RAW_RATINGS_FILENAME)
TRANSLATED_RATINGS_PATH = os.path.join(TRAINING_OUTPUT_DIR, TRANSLATED_RATINGS_FILENAME)

USER_MAPPINGS_PATH = os.path.join(MAPPINGS_OUTPUT_DIR, USER_MAPPINGS_FILENAME)
USER_UPDATE_LOG_PATH = os.path.join(MAPPINGS_OUTPUT_DIR, USER_UPDATE_LOG_FILENAME)

FILM_MAPPINGS_PATH = os.path.join(MAPPINGS_OUTPUT_DIR, FILM_MAPPINGS_FILENAME)


# === HTTP HEADERS FOR SCRAPING ===

# These headers make your requests look like they're coming from a normal browser, not a script.
# Helps avoid being blocked or rate-limited by Letterboxd.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}
