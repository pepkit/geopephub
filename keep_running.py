import requests
from datetime import datetime
from pathlib import Path

FILE_PATH = Path("geo_projects_number.txt")


def add_to_file(
    number_of_projects: int,
) -> None:
    """
    Add content to the file

    :param number_of_projects: number of files
    :return: None
    """

    data = f"{datetime.now().strftime('%Y-%m-%d')},{number_of_projects}\n"

    if FILE_PATH.exists():

        with open(FILE_PATH, "a") as f:
            f.write(data)
        print(f"Added {number_of_projects} to {FILE_PATH}")

    else:
        print(f"File {FILE_PATH} does not exist. Creating a new file.")
        with open(FILE_PATH, "w") as f:
            f.write("date,number_of_projects\n")
            f.write(data)

        print(f"Created {FILE_PATH} and added {number_of_projects}")


def save_number_of_projects():
    """
    Save the number in bedbase to path of files to the file
    :return: None
    """

    # Get the number of files from the server
    response = requests.get("https://pephub-api.databio.org/api/v1/namespaces/geo/")
    if response.status_code == 200:
        number_of_projects = response.json()["number_of_projects"]
        add_to_file(number_of_projects)
    else:
        print(f"Error: {response.status_code}")


if __name__ == "__main__":

    save_number_of_projects()
