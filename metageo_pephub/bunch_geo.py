# This script aims to provide a simple way to get a list of all the PEPs in a
# PEPhub from GEO namespace, download them and zip into a single file.
from pephubclient.helpers import save_pep
from datetime import datetime
from utils import get_agent


def main():
    agent = get_agent()
    # client = PEPHubClient()

    list_of_all_geo = agent.annotation.get_projects_list(
        namespace="geo", limit=10000, order_by="update_date"
    )

    for geo in list_of_all_geo:
        prj = agent.project.get(namespace="geo", name=geo.name, tag=geo.tag, raw=True)
        save_pep(
            prj,
            project_path="/home/alex/databio/repos/metageo_pephub/kinga_folder",
            force=True,
            zip=True,
        )
        # print(prj)


if __name__ == "__main__":
    start_time = datetime.now()
    main()
    end_time = datetime.now()
    duration = end_time - start_time

    # Print the duration in a user-friendly format
    print(f"Time elapsed: {duration}")
