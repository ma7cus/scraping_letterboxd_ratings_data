import requests
from bs4 import BeautifulSoup
from read_in_data import fetch_all_user_data

def fetch_new_users(user_set, number_of_new_users, base_url="https://letterboxd.com/members/popular/"):
    """
    Fetches 'n' new users from Letterboxd's popular members pages, following the page architecture.

    Page Structure:
    <body class="popular-peopl logged-in">
      <div id="content" class="site-body">
        <div class="content-wrap">
          <div class="cols-2">
            <section class="section col-17 col-main">
              <table class="person-table">
                <tbody>
                  <tr>  # Loop through these rows
                    <td class="table-person">
                      <div class="person-summary">
                        <h3 class="title-3">
                          <a href="/USERNAME/">USERNAME</a>  # Extract href for username
                        </h3>
                      </div>
                    </td>
                  </tr>

    Arguments:
    - user_set (set): A set of already known usernames.
    - number_of_new_users (int): The number of new users to fetch.
    - base_url (str): Base URL for Letterboxd's popular members page.

    Returns:
    - list: A list of new usernames added to the existing user set.
    """

    new_users = []  # Store new users found
    page_number = 1  # Start on the first page

    while len(new_users) < number_of_new_users:
        url = f"{base_url}page/{page_number}/" if page_number > 1 else base_url
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Failed to fetch page {page_number}.")
            break  # Stop if the page request fails

        soup = BeautifulSoup(response.content, "html.parser")

        # Navigate the exact architecture described
        people_section = soup.select_one("section.section.col-17.col-main")
        if not people_section:
            print(f"No valid section found on page {page_number}.")
            break

        person_table = people_section.select_one("table.person-table tbody")
        if not person_table:
            print(f"No valid person table found on page {page_number}.")
            break

        all_users = []  # Store extracted usernames from this page

        # Find all <tr> rows (each user entry)
        for row in person_table.find_all("tr"):
            person_cell = row.select_one("td.table-person")
            if not person_cell:
                continue

            person_summary = person_cell.select_one("div.person-summary")
            if not person_summary:
                continue

            name_tag = person_summary.select_one("h3.title-3 a[href]")
            if name_tag:
                username = name_tag["href"].strip("/").split("/")[0]  # Extract username from href
                all_users.append(username)

        # Filter out existing users
        for user in all_users:
            if user not in user_set and len(new_users) < number_of_new_users:
                new_users.append(user)
                user_set.add(user)  # Add to the existing set

        # Move to the next page if needed
        if len(new_users) < number_of_new_users:
            page_number += 1
        else:
            break  # Stop when enough users have been collected

    if new_users:
        print(f"Added {len(new_users)} new users: {new_users}")
    else:
        print("No new users found.")

    return new_users

# Example usage
users = {"ma7cus", "kitcecilia", "helenbendell", "amyhowarthxx", "themparr", "jrmortz", "edmundored"}
new_users = fetch_new_users(users, 10)

print("\nFinal User List:", users)

fetch_all_user_data(users)
