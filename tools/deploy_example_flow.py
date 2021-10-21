"""
Uploads the "execpropstest" folder as a project to Azkaban at localhost.
Then executes the flow "job3" with some runtime property overrides.

For this to work, Azkaban must be running locally.
"""
import shutil
from pathlib import Path
from typing import Set

from requests import Session

PROJECT_NAME = "execpropstest"

BASE_URL = "http://localhost:8081"
INDEX_URL = f"{BASE_URL}/index"
MANAGER_URL = f"{BASE_URL}/manager"
EXECUTOR_URL = f"{BASE_URL}/executor"


def create_zip() -> str:
    source_folder = Path("test") / "execution-test-data" / "execpropstest"
    target_base_name = Path("temp") / "execpropstest"
    zip_path: str = shutil.make_archive(target_base_name, "zip", source_folder)
    print(f"Created zip file {zip_path}")
    return zip_path


def login():
    session = Session()
    res = session.post(BASE_URL, data=dict(action='login', username='azkaban', password="azkaban"))
    res.raise_for_status()
    assert res.json()["status"] == "success"
    return session


def get_project_names() -> Set[str]:
    res = session.get(INDEX_URL, params=dict(ajax="fetchallprojects"))
    res.raise_for_status()
    return {p["projectName"] for p in res.json()["projects"]}


def create_project_if_not_exists(session):
    if PROJECT_NAME not in get_project_names():
        res = session.post(MANAGER_URL, data=dict(
            action="create", name=PROJECT_NAME, description="Created by deploy_example_flow.py"))
        res.raise_for_status()
        json = res.json()
        print(f"Create project response: {json}")
        assert json["status"] == "success"


def upload_zip(session):
    res = session.post(MANAGER_URL,
                       files={"file": ("project.zip", open(zip_path, "rb"), "application/zip")},
                       data=dict(ajax="upload", project=PROJECT_NAME))
    res.raise_for_status()
    json = res.json()
    print(f"Upload zip response: {json}")
    assert json["projectId"]


def execute_flow(session):
    res = session.post(EXECUTOR_URL,
                       data={"ajax": "executeFlow", "project": PROJECT_NAME, "flow": "job3",
                             "runtimeProperty[ROOT][my_prop]": "my_ROOT_val",
                             "runtimeProperty[innerflow][my_prop]": "my_subflow_val",
                             "runtimeProperty[innerflow:job1][my_prop]": "my_inner_job_val",
                             })
    res.raise_for_status()
    json = res.json()
    print(f"Execute flow response: {json}")
    assert json["execid"]
    print(f"Execution link: {EXECUTOR_URL}?execid={json['execid']}#jobslist")


if __name__ == '__main__':
    zip_path: str = create_zip()
    session: Session = login()
    create_project_if_not_exists(session)
    upload_zip(session)
    print(f"Done. Project URL: {MANAGER_URL}?project={PROJECT_NAME}")
    execute_flow(session)
