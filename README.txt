########################################################################################
Letterboxd Ratings Scraper and Mapper
########################################################################################

########################################################################################
Project Description
########################################################################################
This project scrapes popular Letterboxd user ratings and builds a cumulative dataset that maps user IDs and film IDs to readable names. 
It generates both raw and translated data files, as well as lookup tables for users and films. 
The scraping is done in batches with threading for speed and resilience. 
The result is a ready-to-use dataset for analytics or recommendation systems.

########################################################################################
Important Note
########################################################################################
This script fetches data from Letterboxd’s web pages using scraping techniques. 
As such, it is susceptible to HTML structure changes or rate limiting. 
It uses retry logic with exponential backoff to avoid hitting the server too aggressively. 
Be mindful not to overload Letterboxd’s site or violate their terms of use.

########################################################################################
Features
########################################################################################

- Batch scraping of popular Letterboxd users
- Encoded user and film IDs for efficient storage
- Lookup tables for readable usernames and film titles
- Translated ratings output for human-readable exploration
- Optional versioning of raw data outputs
- Safe test mode for trial runs without overwriting production filesgit

########################################################################################
Installation Instructions
########################################################################################

Clone the repository:
git clone https://github.com/ma7cus/scraping_letterboxd_ratings_data.git

Navigate to the project directory:
cd pulling_training_data

Install the required dependencies:
pip install beautifulsoup4 pandas requests

########################################################################################
Usage Instructions
########################################################################################

To run the script and fetch user rating data:

- Open `compile_training_data.py`
- At the bottom, set the mode to `"new"` or `"continue"` depending on your goal:
  - `"new"` starts a fresh run and clears the output files
  - `"continue"` resumes from existing user mappings and ratings

Then run:
python compile_training_data.py

########################################################################################
Input Data
########################################################################################

This project requires no input from the user initially — it starts from scratch using Letterboxd’s publicly available pages of popular users.

However, it maintains and updates the following files:

- `latest_raw_user_ratings.csv`: Encoded user/film/rating values
- `latest_user_mappings.csv`: Maps usernames to numeric user IDs
- `latest_film_mappings.csv`: Maps film IDs to slugs/titles
- `latest_user_updates.csv`: Timestamp of last scrape per user

These are automatically read and updated on each run.

########################################################################################
Output Data
########################################################################################

The following CSV files are generated or updated after each batch:

- `latest_raw_user_ratings.csv`: Encoded ratings data
- `raw_user_ratings_<timestamp>.csv`: Timestamped backup of that batch
- `latest_translated_user_ratings.csv`: Ratings with usernames and film titles
- `latest_film_mappings.csv`: Film ID → slug/title
- `latest_user_mappings.csv`: Username → numeric ID
- `latest_user_updates.csv`: Last update timestamp per user

All files are saved to the output directory specified in `config.py`.

########################################################################################
Directory Structure
########################################################################################

.
├── compile_training_data.py               # Main orchestration script
├── config.py                              # Contains all file paths and flags
├── fetch_ratings.py                       # Handles HTML requests and retry logic
├── read_in_data.py                        # Handles scraping and parallel processing
├── load_save_and_translate_data.py        # Manages translations and saves
├── requirements.txt                       # Project dependencies

########################################################################################
Config Overview
########################################################################################

The `config.py` file holds the paths and file names used throughout the project.

KEY FLAGS:
- `TEST_MODE`: If True, all files are saved in the `test_output` directory

FILENAME DEFINITIONS:
- `RAW_RATINGS_FILENAME`: The latest encoded user ratings (overwritten every run)
- `TRANSLATED_RATINGS_FILENAME`: Ratings translated to usernames and film titles
- `USER_MAPPINGS_FILENAME`: Maps usernames to numeric IDs
- `FILM_MAPPINGS_FILENAME`: Maps film IDs to titles/slugs
- `USER_UPDATE_LOG_FILENAME`: Stores timestamps of last scraped users

These are combined with the output directory (`OUTPUT_DIR`) to define input/output paths. Changing a filename here will change which file the program reads from and writes to.

########################################################################################
Workflow Summary
########################################################################################

1. Scrape N new users from Letterboxd’s popular members page.
2. Scrape ratings for each user using multithreading (5 workers).
3. Assign each user a numeric ID (reused if already known).
4. For each film, extract its ID and slug. Save new ones to the film mapping.
5. Store all ratings in `latest_raw_user_ratings.csv` with optional versioning.
6. Translate these into human-readable ratings with usernames and titles.
7. Update `film_mappings.csv` and `user_mappings.csv` accordingly.
8. Save the last scraped date for each user in `user_last_updated.csv`.

All mappings and the translated ratings are overwritten with each batch — only the raw ratings file has optional versioning.

########################################################################################
Contact
########################################################################################

For questions or suggestions, contact Marcus at marcusb196@outlook.com
