from datetime import datetime  
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from fetch_ratings import scrape_user_ratings_pages_in_parallel
import os
import time
import threading

def scrape_and_encode_user_ratings(user_string, user_id_map, film_id_map, max_user_id_lock):
    """
    Fetches a user's Letterboxd ratings and outputs a data frame with the film names and user name assigned unique numeric IDs.
    The mappings for these IDs are also updated in the user_id_mapping and film_id_mapping dictionaries.

    Arguments:
    - user_string (str): The Letterboxd username.
    - user_id_mapping (dict): Dictionary mapping username → numeric user_id.
    - film_id_mapping (dict): Dictionary mapping film_id → film_title.
    - max_user_id_lock (threading.Lock): Thread lock to safely update max_user_id.

    Returns:
    - df_id_assigned_values (DataFrame): DataFrame containing (numeric_user_id, film_id, rating).
    - new_film_mappings (dict): Dictionary containing new film_id → film_title mappings.
    - updated_user_mapping (dict): Updated dictionary mapping username → numeric user_id.
    - max_user_id (int): Updated maximum numeric user ID.
    """

    # Fetch user ratings from Letterboxd
    # This is the data with the current user's film_slug, film_id and rating
    df = scrape_user_ratings_pages_in_parallel(user_string)  

    if df.empty:
        print(f"No ratings found for {user_string}. Skipping.")
        return None, {}, user_id_map, max_user_id

    # Update the user_id_mapping with the current user_id
    with max_user_id_lock: # This is just a variable which ensures that user_ids are mapped correctly even with concurrent threads
        if user_string not in user_id_map:
            max_user_id = max(user_id_map.values(), default=0) + 1 
            user_id_map[user_string] = max_user_id #Assign the new user_id as one more than the current max user_id
            print(f"DEBUG: Assigned user {user_string} -> ID {max_user_id}")

    #Extract the assigned user_id 
    numeric_user_id = user_id_map[user_string] # (This is here in case the user_id was already in the mapping in which case the max_user_id variable above wouldn't be assigned)

    # Create a dataframe with id values instead of strings
    df_encoded_values = df[["film_id", "rating"]].copy()
    df_encoded_values.insert(0, "user_id", numeric_user_id)

    # Update the film_id_mapping with any new film_id and film_slug pairs
    new_film_mappings = {}
    for _, row in df[["film_id", "film_slug"]].drop_duplicates().iterrows(): #Iterate through all unique film_ids of the current user
        film_id = row["film_id"]
        slug = row["film_slug"]
        if film_id not in film_id_map: #If there is a new film_id, assign the id/slug pair to the mapping file
            new_film_mappings[film_id] = slug
    
    return df_encoded_values, new_film_mappings, user_id_map, max_user_id

def fetch_all_user_data(users, user_id_mapping, max_user_id):
    """
    Processes the ratings data of a specified set of users in parallel.
    All usernames and film IDs are assigned unique numeric IDs, and the mappings for the current batch of users are also outputted with the encoded ratings data.

    Args:
        users (list[str]): List of Letterboxd usernames to process.
        user_id_mapping (dict[str, int]): Existing dictionary mapping username → numeric user ID. 
                                            (Required so new user ids aren't assigned already existing user ids)
                                            (The existing film_id map isn't required as we're pulling the already assigned ids from letterboxd)
        max_user_id (int): Current highest numeric user ID assigned so far.

    Returns:
        combined_user_ratings_df (pd.DataFrame): Concatenated DataFrame containing all user ratings from the batch with columns ['user_id', 'film_id', 'rating'].
        film_id_to_title (dict[str, str]): Dictionary mapping film_id → film_title (slug) for all films encountered in the batch.
        user_id_mapping (dict[str, int]): Updated mapping of usernames to numeric user IDs, reflecting any new users added.
        max_user_id (int): Updated maximum numeric user ID after processing the batch.
    """

    # === SETUP ===
    all_user_ratings_set = []  # Stores a set of all individual user ratings
    film_id_to_title = {}  # Maps film_id → film_title. Start empty, this will build up the full map for the current users
    max_user_id_lock = threading.Lock()  # Thread lock for safe ID increment

    start_time = time.time()

    # === SCRAPE USER DATA IN PARALLEL ===

    with ThreadPoolExecutor(max_workers=5) as executor: #Parallel processing with 5 worker threads
        futures_dict = {} #This maintains a map between submitted tasks and the user_ids associated with them.

        # === SUBMIT USER SCRAPING TASKS ===
        for user in users:

            # Submit a function for the current user to start running in the background
            future = executor.submit(scrape_and_encode_user_ratings,user,user_id_mapping,film_id_to_title,max_user_id_lock)
            
            #Store this future object in the futures dictionary so we know what data it corresponds to whenever it finishes
            futures_dict[future] = user  

        # === PROCESS COMPLETED TASKS ===
        #As each submitted task finishes, process its result
        for future in as_completed(futures_dict):
            user_id = futures_dict[future] #Pull the associated user_id for the latest completed task
            try:
                current_user_df, new_film_mappings, updated_user_mapping, new_max_user_id = future.result() #Try extract the results from the scraping task
                
                if current_user_df is not None:
                    
                    # Append processed ratings to the set of processed ratings
                    all_user_ratings_set.append(current_user_df) 

                    # Update mappings for this batch of users 
                    film_id_to_title.update(new_film_mappings)
                    user_id_mapping.update(updated_user_mapping)

                    # Update max_user_id globally
                    max_user_id = max(max_user_id, new_max_user_id)

            except Exception as e:
                print(f"Error processing {user_id}: {e}")

    # === OUTPUTTING CORRECT DATA ===

    #Combining the individual user ratings into a single DataFrame
    combined_user_ratings_df = pd.concat(all_user_ratings_set, ignore_index=True) if all_user_ratings_set else pd.DataFrame(columns=["user_id", "film_id", "rating"])

    #Printing the number of users processed and the time taken
    end_time = time.time()
    print(f"\nProcessed {len(users)} users in {end_time - start_time:.2f} seconds.")

    return combined_user_ratings_df, film_id_to_title, user_id_mapping, max_user_id  
