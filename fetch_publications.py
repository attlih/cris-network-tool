import requests
import json
import csv
from typing import Dict, List
import time
import sys
import pandas as pd


def filter_publication_data(publication: Dict, local_unit_id: str) -> Dict:
    """
    Filter and extract relevant fields from a publication (bundled with local_unit_id)
    """
    return {
        "titleOfPublication": publication["data"].get("titleOfPublication", {}),
        "authorsOfThePublication": publication["data"].get("authorsOfThePublication", {}),
        "yearOfPublication": publication["data"].get("detailedPublicationInformation", {}).get("yearOfPublication", {}),
        "localUnitId": local_unit_id
    }


def fetch_publications(page: int = 1, skip: int = 0, start_year: str = "2020", end_year: str = "2023", local_unit_id: str = "") -> Dict:
    """
    Fetch publications from UEF CRIS API for a specific page, year range, and local unit ID
    """
    base_url = "https://uef.cris.fi/api/public-research/publications"
    params = {
        "searchType": "publications",
        "localUnitIds": local_unit_id,
        "lang": "en",
        "yearOfPublicationStart": start_year,
        "yearOfPublicationEnd": end_year,
        "order": "data.titleOfPublication.titleOfPublication ASC",
        "page": page,
        "skip": skip,
        "limit": 100
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Raise an exception for bad status codes
    return response.json()


def fetch_all_publications(start_year: str, end_year: str, local_unit_id: str) -> List[Dict]:
    """
    Fetch all publications by handling pagination for a given year range and local unit ID
    """
    all_publications = []
    page = 1
    limit = 100  # Items per page

    # Get initial data to determine total pages
    initial_data = fetch_publications(
        page=1, skip=0, start_year=start_year, end_year=end_year, local_unit_id=local_unit_id)
    total_pages = initial_data.get("meta", {}).get("pageCount", 0)
    total_publications = initial_data.get("meta", {}).get("totalCount", 0)
    print(f"Total publications to fetch: {total_publications}")

    # Filter initial data
    filtered_initial_data = [filter_publication_data(pub, local_unit_id)
                             for pub in initial_data.get("data", [])]
    all_publications.extend(filtered_initial_data)

    # If there's only one page, we're done
    if total_pages <= 1:
        return all_publications

    # Fetch remaining pages
    while page < total_pages:
        page += 1
        skip = (page - 1) * limit
        print(f"\rFetching page {page}/{total_pages} (skip: {skip})", end="")

        try:
            data = fetch_publications(
                page=page, skip=skip, start_year=start_year, end_year=end_year, local_unit_id=local_unit_id)
            filtered_data = [filter_publication_data(pub, local_unit_id)
                             for pub in data.get("data", [])]
            all_publications.extend(filtered_data)
            time.sleep(1)  # Add a small delay to be nice to the server

        except requests.exceptions.RequestException as e:
            print(f"\nError fetching page {page}: {e}")
            break

    print()  # New line after progress messages
    return all_publications


def save_publications(publications: List[Dict], filename: str):
    """
    Save publications to a CSV file
    """
    if not publications:
        print("No publications to save")
        return

    # Extract headers from the first publication
    headers = []
    for key, value in publications[0].items():
        if isinstance(value, dict):
            # Flatten nested dictionaries
            for nested_key in value.keys():
                headers.append(f"{nested_key}")
        else:
            headers.append(key)

    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for pub in publications:
            row = []
            for key, value in pub.items():
                if isinstance(value, dict):
                    # Flatten nested dictionaries
                    row.extend(str(v) for v in value.values())
                else:
                    row.append(str(value))
            writer.writerow(row)

    print(f"Saved {len(publications)} publications to {filename}")


def generate_edge_list(input_path, output_path) -> List[Dict]:
    """
    Generate an edge list from the publication data.
    Each edge represents a co-authorship between two authors.
    """

    data = pd.read_csv(input_path)
    # Split the authors column into separate rows
    data_long = data.assign(
        authors=data['authors'].str.split(';')).explode('authors')

    # Strip any leading/trailing whitespace from author names
    data_long['authors'] = data_long['authors'].str.strip()

    # Create an edge list with source and target columns
    edge_list = data_long.merge(
        data_long, on=['titleOfPublication', 'yearOfPublication'])
    edge_list = edge_list[edge_list['authors_x'] != edge_list['authors_y']]
    edge_list = edge_list[['authors_x', 'authors_y',
                           'titleOfPublication', 'yearOfPublication']].drop_duplicates()

    # Remove double connections by ensuring source is always lexicographically smaller than target
    edge_list['source'], edge_list['target'] = (
        edge_list[['authors_x', 'authors_y']].min(axis=1),
        edge_list[['authors_x', 'authors_y']].max(axis=1)
    )
    edge_list = edge_list[['source', 'target',
                           'titleOfPublication', 'yearOfPublication']].drop_duplicates()
    edge_list.columns = ['source', 'target', 'publication', 'year']

    # Save the edge list to a CSV file
    edge_list.to_csv(output_path, index=False)
    print(f"Edge list saved to {output_path}")


def main():
    if len(sys.argv) < 4:
        print("Usage: python fetch_publications.py <start_year> <end_year> <local_unit_id>")
        sys.exit(1)

    start_year = sys.argv[1]
    end_year = sys.argv[2]
    local_unit_id = sys.argv[3]

    print(
        f"Fetching publications from {start_year} to {end_year} for local unit ID {local_unit_id}...")
    publications = fetch_all_publications(start_year, end_year, local_unit_id)
    print(f"Total publications fetched: {len(publications)}")

    # Save publications to a CSV file
    publications_filename = f"publications_{start_year}_{end_year}.csv"
    save_publications(publications, publications_filename)

    # Generate and save edge list
    print("Generating edge list...")
    edge_list_filename = f"edgelist_{start_year}_{end_year}.csv"
    generate_edge_list(publications_filename, edge_list_filename)


if __name__ == "__main__":
    main()