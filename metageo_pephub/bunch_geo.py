# This script aims to provide a simple way to get a list of all the PEPs in a
# PEPhub from GEO namespace, download them and zip into a single file.
from pephubclient import PEPHubClient
from pepdbagent import PEPDatabaseAgent
from datetime import datetime


def main(host, password, user, port=5432, db="pephub"):
    agent = PEPDatabaseAgent(
        host=host, port=port, database=db, user=user, password=password
    )
    client = PEPHubClient()

    list_of_all_geo = agent.annotation.get(
        namespace="geo", limit=2000, order_by="update_date"
    )
    for geo in list_of_all_geo.results:
        prj = agent.project.get(namespace="geo", name=geo.name, tag=geo.tag, raw=True)
        client._save_raw_pep(
            f"{geo.namespace}/{geo.name}:{geo.tag}",
            prj,
            just_name=True,
            project_path="/home/alex/databio/repos/metageo_pephub/kinga_folder",
            force=True,
        )
        # print(prj)


if __name__ == "__main__":
    start_time = datetime.now()
    # run main here!
    end_time = datetime.now()
    duration = end_time - start_time

    # Print the duration in a user-friendly format
    print(f"Time elapsed: {duration}")
