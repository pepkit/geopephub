from geopephub.utils import get_agent
from pepdbagent.db_utils import BedBaseStats
from sqlalchemy.orm import Session
from sqlalchemy import select, and_
from rich.progress import track


def main():
    agent = get_agent()

    all_projects_annot = agent.annotation.get(namespace="bedbase", limit=100000).results

    with Session(agent.pep_db_engine.engine) as session:

        for project_annot in track(all_projects_annot):
            project = agent.project.get(
                namespace=project_annot.namespace,
                name=project_annot.name,
                tag=project_annot.tag,
                raw=False,
            )
            for sample in project.samples:

                result = session.scalar(
                    select(BedBaseStats).where(
                        and_(
                            BedBaseStats.gsm == sample.sample_geo_accession.lower(),
                            BedBaseStats.gse == sample.gse.lower(),
                        )
                    )
                )
                if not result:
                    session.add(
                        BedBaseStats(
                            gse=sample.gse.lower(),
                            gsm=sample.sample_geo_accession.lower(),
                            sample_name=sample.sample_name,
                            genome=sample.get("ref_genome", "").strip(),
                            last_update_date=sample.sample_submission_date,
                            submission_date=sample.sample_submission_date,
                        )
                    )
                    session.commit()
    print("Finished adding bedbase stats with success")
