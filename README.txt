Letterboxd Ratings Scraper and Mapper
=====================================

This project scrapes user rating data from Letterboxd and compiles it into a structured dataset. It supports incremental updates, numeric ID mapping, and translation into human-readable formats. It is suitable for use in recommendation systems, statistical analysis, or exploratory data work.

Overview
--------

The scraper targets popular Letterboxd users and retrieves their film rating history, saving the data in both raw ID-based and translated readable formats.

- Assigns consistent numeric IDs to users and films
- Maintains mapping tables and update logs to allow incremental refreshes
- Multi-threaded design for efficient batch scraping
- Modular structure for testing and reuse
- Scraping relies on the current HTML structure of Letterboxd pages. This is a known point of failure and may require updates if the structure changes.

Directory Structure
-------------------

Project files:

    .
    ├── compile_training_data.py             # Main batch scraping pipeline
    ├── generate_user_data.py                # Standalone script to scrape one user
    ├── config.py                            # Paths, filenames, switches
    ├── fetch_ratings.py                     # Core scraping logic
    ├── read_in_data.py                      # Batch scraping and encoding
    ├── load_save_and_translate_data.py      # Mappings, translation, file reading in/out
    ├── data_output/                         # Output folder for structured data
    └── .gitignore                           # Excludes generated data from version control

Output directory:

    data_output/
    ├── training/
    │   ├── latest_raw_user_ratings.csv
    │   └── latest_translated_user_ratings.csv
    ├── mappings/
    │   ├── latest_user_mappings.csv
    │   ├── latest_film_mappings.csv
    │   └── latest_user_updates.csv
    └── users/
        ├── user_<username>_raw.csv
        └── user_<username>_translated.csv

Process Description
-------------------

1. Scraping

    - Usernames are pulled from Letterboxd’s "Popular Members" pages.
    - Each user's film rating history is scraped using multi-threaded HTTP requests.
    - The following are extracted:
        - film_id (Letterboxd internal ID)
        - film_slug (URL-style title)
        - rating (converted from "★★½" format to a numeric float)

2. Encoding

    - Each username is assigned a unique numeric user ID.
    - Each film is similarly tracked by a film ID and mapped to a readable slug.
    - User and film mappings are validated to prevent ID collisions or inconsistencies.

3. Output

    - Saves raw and translated ratings to CSV
    - Updates mappings and update logs
    - Optionally creates timestamped backup versions of the raw data if versioning is enabled

Configuration
-------------

All configuration is handled through `config.py`.

Key options:

    TEST_MODE: When set to True, writes output to a sandboxed directory (`data_output/test/`)
    VERSIONING_TOGGLE: If True, raw ratings are saved with a timestamp as well as overwriting the latest version

Running the Code
----------------

A. Batch Scraping (compile_training_data.py)

Use this for large-scale data collection.

    python compile_training_data.py

You can configure batch size and number of batches within the script:

    mode = "new"  # or "continue"
    run_full_batch_scraping_method(num_batches=2, batch_size=10, mode=mode)

- Use `mode="new"` to start fresh (clears previous user IDs and mappings)
- Use `mode="continue"` to append to the existing dataset

B. Single User Scraping (generate_user_data.py)

Use this for targeted scraping or testing output structure.

    python generate_user_data.py

Edit the following line to change the target user:

    user_name = "ma7cus"

Limitations
-----------

- The scraper depends on the HTML structure of Letterboxd's website. If the structure changes, the scraping logic may fail and will need updating.
- There is no public Letterboxd API for this data; scraping is used as a fallback.
- Although backoff logic is implemented, repeated access may result in throttling or temporary blocks.
- Films are identified by `data-film-id`, which must remain consistent to ensure data integrity.

Output Examples
---------------

Raw (numeric IDs):

    user_id,film_id,rating
    1,104387,4.0
    1,118156,3.5

Translated (readable labels):

    username,film_title,rating
    ma7cus,interstellar,4.0
    ma7cus,arrival,3.5


Legal and Ethical Use
----------------------

This project is for research and educational purposes only. Respect the Letterboxd [terms of service](https://letterboxd.com/legal/terms/) and avoid excessive automated traffic.

