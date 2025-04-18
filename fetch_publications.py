import requests
import json
import csv
from typing import Dict, List
import time
import sys
import pandas as pd

UNIT_ID_TO_NAME = {
    # Philosophical Faculty
    "b5f6c35a-0bc3-4587-a31e-0a98c744eab7": "Administration of the Philosophical Faculty",
    "02ef482d-78d9-474d-bcb1-7a8c6f858a1d": "School of Humanities",
    "dc25550a-df97-4873-8cba-a0e141643ce9": "University Teacher Training School of University of Eastern Finland",
    "02bfe5c2-333f-44eb-b599-902bff03c2bb": "School of Educational Sciences and Psychology",
    "2d329a68-8abe-417c-8071-d9306940e5c7": "School of Applied Educational Science and Teacher Education",
    "de7fe9f9-69bd-4a6b-bb99-05ceb760b1dd": "School of Theology",

    # Faculty of Science, Forestry and Technology
    "8a54b7d0-e14d-4482-9957-fb2ed8ffce18": "Department of Physics and Mathematics",
    "5cec5645-aa02-4878-ab19-8d41a446b152": "Department of Chemistry and Sustainable Technology",
    "3733c177-e054-40df-9407-ac302b6add0d": "Administration of the Faculty of Science, Forestry and Technology",
    "3e7d0d85-4423-4b57-b532-922714578b6c": "School of Forest Sciences",
    "e4e9aee0-1c6a-4218-b5ce-89ad0a576a2e": "Department of Technical Physics",
    "88f6f7f1-e92b-4863-a485-848b130320a7": "School of Computing",
    "15db3714-ba67-4a03-af96-2bf6eafbd7b7": "Department of Environmental and Biological Sciences",

    # Service Centres
    "c91b0d6d-53bf-4063-a6c4-3980eb32ef35": "Pharmacy of University of Eastern Finland",
    "09a9fb8c-5d14-41d2-aafa-c3bfff59a568": "Centre for Continuous Learning of the University of Eastern Finland",
    "3df78c77-1795-4115-a84c-b080ee701f87": "Language Centre",
    "b8621176-27da-47a9-913f-6892f4297bd0": "Library",

    # Faculty of Health Sciences
    "0ee2a2bf-a0fd-4d19-be67-9832b043b1e9": "A.I. Virtanen Institute",
    "9b6ad0a5-2282-4f1f-806f-83b3d249cd70": "School of Pharmacy",
    "ba1f649a-cf6b-48fa-9b5c-c7d009052af3": "Department of Nursing Sciences",
    "0625aad6-f1d4-4821-aab9-2400b11b6f81": "School of Medicine",
    "35b85f11-b99d-410a-b5f4-3fd32b19a662": "Administration of the Faculty of Health Sciences",

    # Faculty of Social Sciences and Business Studies
    "d05c9db6-0c0e-4469-892a-6b6af55a2a75": "Department of Geographical and Historical Studies",
    "b013f81c-a354-4921-9294-56726424a9a5": "Karelian Institute",
    "1d1a7a1f-8ade-4b49-a741-3604bb1372f4": "Business School",
    "2b5d5783-bce1-457b-ad52-a3fd936601c1": "Law School",
    "8709d377-e6db-4736-a8f9-cc839570d108": "Department of Health and Social Management",
    "b1cd5940-91df-469b-a0dd-2c5c0e02158e": "Administration of the Faculty of Social Sciences and Business Studies",
    "2C12723657-8721-4dfd-8bd9-74294042a794": "Department of Social Sciences",

    # University leadership and services
    "ecdc1870-14cf-4d27-a3a0-837159a72174": "University of Eastern Finland, leadership",
    "3d3e782c-8ed7-4d07-af1f-b8f407ea5df9": "University Services"
}

def filter_publication_data(publication: Dict, local_unit_id: str) -> Dict:
    """
    Filter and extract relevant fields from a publication.
    Replace unit ID with department name if available.
    """
    return {
        "titleOfPublication": publication["data"].get("titleOfPublication", {}),
        "authorsOfThePublication": publication["data"].get("authorsOfThePublication", {}),
        "yearOfPublication": publication["data"].get("detailedPublicationInformation", {}).get("yearOfPublication", {}),
        "department": UNIT_ID_TO_NAME.get(local_unit_id, local_unit_id)  # fallback to ID if name not found
    }

def filter_publication_data(publication: Dict, local_unit_id: str) -> Dict:
    return {
        "titleOfPublication": publication["data"].get("titleOfPublication", {}),
        "authorsOfThePublication": publication["data"].get("authorsOfThePublication", {}),
        "yearOfPublication": publication["data"].get("detailedPublicationInformation", {}).get("yearOfPublication", {}),
        "localUnitId": local_unit_id,
        "department": UNIT_ID_TO_NAME.get(local_unit_id, "Unknown Department")
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

def generate_node_list(input_path, output_path):
    """
    Generate a node list from publication data.
    Each node is an author, assigned their most frequent department.
    """
    from collections import defaultdict, Counter

    data = pd.read_csv(input_path)
    if 'authors' not in data.columns or 'department' not in data.columns:
        raise ValueError("CSV must contain 'authors' and 'department' columns.")

    author_affiliations = defaultdict(list)

    for _, row in data.iterrows():
        authors = str(row['authors']).split(';')
        department = row['department']
        for author in authors:
            clean_author = author.strip()
            if clean_author:
                author_affiliations[clean_author].append(department)

    nodes = []
    for author, departments in author_affiliations.items():
        most_common_dept = Counter(departments).most_common(1)[0][0]
        nodes.append({
            "id": author,
            "label": author,
            "department": most_common_dept
        })

    df_nodes = pd.DataFrame(nodes)
    df_nodes.to_csv(output_path, index=False)
    print(f"Node list saved to {output_path}")



def main():
    print("Select mode:")
    print("1 - Fetch publications for a single local unit")
    print("2 - Generate edge and node lists from existing publication file")
    choice = input("Enter 1 or 2: ").strip()

    if choice == "1":
        # Ask user for inputs instead of relying on command-line arguments
        start_year = input("Enter start year (e.g. 2025): ").strip()
        end_year = input("Enter end year (e.g. 2025): ").strip()
        local_unit_id = input("Enter local unit ID (e.g. 88f6f7f1-e92b-4863-a485-848b130320a7): ").strip()

        print(f"\nFetching publications from {start_year} to {end_year} for local unit ID {local_unit_id}...")
        publications = fetch_all_publications(start_year, end_year, local_unit_id)
        print(f"Total publications fetched: {len(publications)}")

        department_name = UNIT_ID_TO_NAME.get(local_unit_id, "UnknownDepartment").replace(" ", "_")
        publications_filename = f"publications_{department_name}_{start_year}_{end_year}.csv"
        save_publications(publications, publications_filename)

    elif choice == "2":
        publications_filename = "publications_all.csv"

        print(f"Using publication file: {publications_filename}")

        print("Generating edge list...")
        edge_list_filename = "edgelist_all.csv"
        generate_edge_list(publications_filename, edge_list_filename)

        print("Generating node list...")
        node_list_filename = "nodelist_all.csv"
        generate_node_list(publications_filename, node_list_filename)

    else:
        print("Invalid choice. Please enter 1 or 2.")


if __name__ == "__main__":
    main()