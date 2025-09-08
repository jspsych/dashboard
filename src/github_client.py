"""
GitHub API client module
Handles fetching data from GitHub API with pagination support
"""

import httpx

def fetch_api(endpoint, github_token):
    url = f"https://api.github.com/repos/jspsych/jsPsych/{endpoint}"
    final_data = []
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f'token {github_token}'
    }
    while url:
        try:
            response = httpx.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()
            for item in data:
                final_data.append(item)
            if 'next' in response.links:
                url = response.links['next']['url']
            else:
                break
        except httpx.RequestError as e:
            print(f"Error fetching data from {url}: {e}")
            return None
        except httpx.HTTPStatusError as e:
            print(f"HTTP error occurred: {e}")
            return None

    return final_data