"""A DAG that proves the pipeline works, and nothing else.

Seeded once, when colors created this repository. Delete it as soon as you have
watched it run — it is here to answer one question on a fresh install, which is
whether a commit reaches the scheduler, and it has no other purpose.

Nothing reconciles this file. A later `create` will not restore it if you remove
it, and will not overwrite it if you edit it: the seed runs only for a
repository colors itself created, because a converging seed would replace real
DAGs with this one.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="hello_world",
    # No schedule. A machine that has just been provisioned should not start
    # running things on its own; trigger this by hand from the UI.
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["colors", "example"],
    doc_md=__doc__,
)
def hello_world():
    @task
    def greet() -> str:
        message = "hello from <{ airflow-host }>"
        print(message)
        return message

    @task
    def confirm(message: str) -> None:
        # Two tasks rather than one, so the run also demonstrates that XCom
        # works — which is the part that depends on the metadata database being
        # reachable and the Fernet key being right, rather than merely on the
        # scheduler being up.
        print(f"the previous task said: {message}")

    confirm(greet())


hello_world()
