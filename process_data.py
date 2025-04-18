import requests
import pandas as pd
import time
import os
import re
from typing import Dict, List


def fetch_publication_details(publication_id: str) -> Dict:
    """
    Fetch detailed information about a specific publication using its ID
    """
    url = f"https://uef.cris.fi/api/public-research/publications/{publication_id}"
    params = {
        "lang": "en"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching publication {publication_id}: {e}")
        return None


def extract_author_info(publication_data: Dict) -> List[Dict]:
    """
    Extract only author ID and name information from publication data
    """
    if not publication_data or 'data' not in publication_data:
        return []

    publication_id = publication_data.get('id', 'unknown')

    authors_data = []
    authors = publication_data.get('data', {}).get(
        'authorsOfThePublication', {}).get('localAuthors', [])

    for author_entry in authors:
        author_data = author_entry.get('author', {})
        author_info = {
            'publication_id': publication_id,
            'author_id': author_data.get('id', ''),
            'author_name': f"{author_data.get('lastName', '')}, {author_data.get('firstName', '')}".strip()
        }

        # Only add if we have an author ID
        if author_info['author_id']:
            authors_data.append(author_info)

    return authors_data


def process_publications_csv(input_filename: str, output_filename: str):
    """
    Process a CSV file of publications, fetch details for each, and update with author IDs and names
    Always creates an edge list
    """
    # Check if input file exists
    if not os.path.exists(input_filename):
        print(f"Error: Input file '{input_filename}' not found")
        return

    # Read the CSV file
    try:
        df = pd.read_csv(input_filename)
        original_df = df.copy()  # Keep a copy of the original data
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Check if the ID column exists
    if 'id' not in df.columns:
        print("Error: CSV file must contain an 'id' column with publication IDs")
        return

    # Get publication IDs
    publication_ids = df['id'].unique().tolist()

    print(f"Found {len(publication_ids)} unique publications to process")

    # Create dictionaries to store publication ID to authors mapping
    publication_authors = {}
    publication_author_names = {}

    # Show progress bar
    total_publications = len(publication_ids)

    # Process each publication
    for i, pub_id in enumerate(publication_ids):
        # Calculate progress percentage
        progress = (i + 1) / total_publications * 100
        print(
            f"\rProcessing publication {i+1}/{total_publications}: ({progress:.1f}%)", end="")

        # Fetch publication details
        pub_details = fetch_publication_details(pub_id)

        # Extract author information
        if pub_details:
            authors = extract_author_info(pub_details)
            # Store authors for this publication
            if authors:
                # Create a string of author IDs for this publication
                author_ids = [author['author_id']
                              for author in authors if author['author_id']]
                publication_authors[pub_id] = ';'.join(author_ids)

                # Create a string of author names for this publication
                author_names = [author['author_name'].strip()
                                for author in authors if author['author_name']]
                publication_author_names[pub_id] = ';'.join(author_names)

        # Add a delay based on batch size to be nice to the server
        # Add longer delay every 10 requests
        if (i + 1) % 10 == 0:
            time.sleep(1.0)  # 1 second pause every 10 requests

    print("\nDone fetching publication details")

    # Update the dataframe with author IDs and names
    if publication_authors:
        # Create a new column for author IDs
        df['local_author_ids'] = df['id'].map(publication_authors)

        # Add a new column for local author names
        df['local_authors'] = df['id'].map(publication_author_names)

        # Save the updated dataframe to output file
        df.to_csv(output_filename, index=False)
        print(
            f"Successfully updated {len(publication_authors)} publications with author IDs and names in {output_filename}")

        # Always create edge list - pass the dataframe directly
        create_edge_list(df, output_filename)
    else:
        print("No author information found")


def create_edge_list(df, output_filename):
    """
    Create an edge list CSV from a publications dataframe or file with author information
    """
    print("\nGenerating edge list...")

    # Split the authors column into separate rows
    data_long = df.assign(
        local_authors=df['local_authors'].str.split(';')).explode('local_authors')

    # Strip any leading/trailing whitespace from author names
    data_long['local_authors'] = data_long['local_authors'].str.strip()

    # Find unique authors
    unique_authors = sorted(data_long['local_authors'].unique())
    print(f"Found {len(unique_authors)} unique authors")

    # Create an edge list with source and target columns
    edge_list = data_long.merge(
        data_long, on=['titleOfPublication', 'yearOfPublication'])
    edge_list = edge_list[edge_list['local_authors_x']
                          != edge_list['local_authors_y']]
    edge_list = edge_list[['local_authors_x', 'local_authors_y',
                           'titleOfPublication', 'yearOfPublication']].drop_duplicates()

    # Remove double connections by ensuring source is always lexicographically smaller than target
    edge_list['source'], edge_list['target'] = (
        edge_list[['local_authors_x', 'local_authors_y']].min(axis=1),
        edge_list[['local_authors_x', 'local_authors_y']].max(axis=1)
    )
    edge_list = edge_list[['source', 'target',
                           'titleOfPublication', 'yearOfPublication']].drop_duplicates()
    edge_list.columns = ['source', 'target', 'publication', 'year']

    # Save the edge list to a CSV file
    edge_list_file_path = f'edge_list_{output_filename}'
    edge_list.to_csv(edge_list_file_path, index=False)

    print(f"Edge list saved to {edge_list_file_path}")
    print(f"Total connections: {len(edge_list)}")

    return edge_list


def main():
    print("=== UEF CRIS Author Information & Network Tool ===")

    # Get input file path
    while True:
        input_file = input(
            "Enter the path to the CSV file containing publication IDs: ").strip()
        if os.path.exists(input_file):
            break
        else:
            print(
                f"Error: File '{input_file}' not found. Please enter a valid path.")

    # Get output file path
    output_file = input(
        f"Enter the output file name (default: updated_{os.path.basename(input_file)}): ").strip()
    if not output_file:
        output_file = f"updated_{os.path.basename(input_file)}"

    # Confirm operation
    print(f"\nWill process publications from: {input_file}")
    print(f"Results will be saved to: {output_file}")
    print(f"Edge list will be created automatically after processing")
    print("This may take some time for large datasets.")

    process_publications_csv(input_file, output_file)


if __name__ == "__main__":
    main()
