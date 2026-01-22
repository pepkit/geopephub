from geopephub.metageo_pephub import (
    upload_queued_projects,
    run_upload_checker,
    check_by_date,
)


def run_queuer():
    # add_to_queue_by_period(
    #     target="bedbase",
    #     start_period="2023/10/22", # Oct 23, 2023
    #     end_period="2023/10/24", # Oct 24, 2023
    #     level="project",
    #     tag="default",
    # )
    # upload_queued_projects(
    #     target="bedbase",
    #     tag="project",
    # )
    # check_by_date(
    #     target="bedbase",
    #     start_period="2023/10/22", # Oct 23, 2023
    #     end_period="2023/10/25", # Oct 24, 2023
    #     tag="projects",
    # )
    check_by_date(
        target="bedbase",
        start_period="2021/09/22",
        end_period="2021/09/25",
        tag="projects",
    )


if __name__ == "__main__":
    run_queuer()
