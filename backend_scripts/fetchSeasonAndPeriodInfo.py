from contextlib import closing
import requests
import json
import os
import databaseConnector

# List of Blizzard API regions to process
regions = [
    'us',
    'eu',
    'kr',
    'tw'
]

databaseConnector.init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    os.environ.get("DATABASE_PORT"),
    1,
)

# Base template URLs
season_index_url = "https://{region}.api.blizzard.com/data/wow/mythic-keystone/season/index"
season_details_url = "https://{region}.api.blizzard.com/data/wow/mythic-keystone/season/{season_id}"
period_details_url = "https://{region}.api.blizzard.com/data/wow/mythic-keystone/period/{period_id}"

CLIENT_ID = os.getenv('BLIZ_CLIENT_ID')
CLIENT_SECRET = os.getenv('BLIZ_CLIENT_SECRET')

# Obtain an access token
def get_access_token():
    auth_url = 'https://oauth.battle.net/token'
    data = {'grant_type': 'client_credentials'}
    response = requests.post(auth_url, data=data, auth=(CLIENT_ID, CLIENT_SECRET))
    response.raise_for_status()
    return response.json()['access_token']

# Common function to perform GET requests with token
def blizzard_get(url, params=None, token=None):
    headers = {'Authorization': f'Bearer {token}'}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


def main():
    token = get_access_token()
    all_regions_data = {}

    for region in regions:
        print(f"Fetching data for region: {region}")
        namespace = f"dynamic-{region}"
        # Get current season index
        idx_resp = blizzard_get(
            season_index_url.format(region=region),
            params={'namespace': namespace, 'locale': 'en_US'},
            token=token
        )
        season_id = idx_resp['current_season']['id']

        # Get season details to extract period IDs
        season_resp = blizzard_get(
            season_details_url.format(region=region, season_id=season_id),
            params={'namespace': namespace, 'locale': 'en_US'},
            token=token
        )
        periods = season_resp.get('periods', [])

        # For each period, fetch start and end timestamps
        region_periods = []
        with closing(databaseConnector.get_connection()) as conn:
            cursor = conn.cursor()
            for p in periods:
                print(f"Processing period ID: {p['id']}")
                pid = p['id']
                per_resp = blizzard_get(
                    period_details_url.format(region=region, period_id=pid),
                    params={'namespace': namespace, 'locale': 'en_US'},
                    token=token
                )
                region_periods.append({
                    'id': per_resp['id'],
                    'start_timestamp': per_resp['start_timestamp'],
                    'end_timestamp': per_resp['end_timestamp']
                })
                databaseConnector.insert_season_periods(conn, cursor, region, pid, per_resp['start_timestamp'], per_resp['end_timestamp'], season_id)
            databaseConnector.commit_changes(conn)
        all_regions_data[region] = {
            'season_id': season_id,
            'periods': region_periods
        }


    # Write to JSON file
    output_path = os.path.join('data', 'static')
    os.makedirs(output_path, exist_ok=True)
    with open(os.path.join(output_path, 'periods.json'), 'w') as f:
        json.dump(all_regions_data, f, indent=2)

    print(f"Generated periods.json for regions: {', '.join(regions)}")

if __name__ == '__main__':
    main()
