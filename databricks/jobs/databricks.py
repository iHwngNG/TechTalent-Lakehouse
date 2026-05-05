# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


TechTalen_Lakehouse_Pipeline = Job.from_dict(
    {
        "name": "TechTalen-Lakehouse-Pipeline",
        "email_notifications": {
            "on_start": [
                "hwng.nvq@gmail.com",
            ],
            "on_success": [
                "hwng.nvq@gmail.com",
            ],
            "on_failure": [
                "hwng.nvq@gmail.com",
            ],
        },
        "schedule": {
            "quartz_cron_expression": "0 0 7 * * ?",
            "timezone_id": "Asia/Ho_Chi_Minh",
            "pause_status": "UNPAUSED",
        },
        "tasks": [
            {
                "task_key": "setup_catalog",
                "spark_python_task": {
                    "python_file": "/Workspace/Users/hwng.nvq@gmail.com/TechTalent-Lakehouse/src/utils/catalogSetup.py",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "scrapeITViec",
                "depends_on": [
                    {
                        "task_key": "setup_catalog",
                    },
                ],
                "spark_python_task": {
                    "python_file": "/Workspace/Users/hwng.nvq@gmail.com/TechTalent-Lakehouse/src/ingestion/job_scrape_itviec.py",
                },
                "environment_key": "TechTalent-Lakehouse-env",
            },
            {
                "task_key": "scrapeTopdev",
                "depends_on": [
                    {
                        "task_key": "setup_catalog",
                    },
                ],
                "spark_python_task": {
                    "python_file": "/Workspace/Users/hwng.nvq@gmail.com/TechTalent-Lakehouse/src/ingestion/job_scrape_topdev.py",
                },
                "environment_key": "TechTalent-Lakehouse-env",
            },
            {
                "task_key": "Transform_jobs",
                "depends_on": [
                    {
                        "task_key": "scrapeTopdev",
                    },
                    {
                        "task_key": "scrapeITViec",
                    },
                ],
                "spark_python_task": {
                    "python_file": "/Workspace/Users/hwng.nvq@gmail.com/TechTalent-Lakehouse/src/transformation/bronzeToSilver/01_transform_jobs.py",
                },
                "environment_key": "Default",
            },
        ],
        "queue": {
            "enabled": True,
        },
        "environments": [
            {
                "environment_key": "Default",
                "spec": {
                    "environment_version": "5",
                },
            },
            {
                "environment_key": "TechTalent-Lakehouse-env",
                "spec": {
                    "dependencies": [
                        "-r /Workspace/Users/hwng.nvq@gmail.com/TechTalent-Lakehouse/databricks/conf/dependencies.txt",
                    ],
                    "environment_version": "5",
                },
            },
        ],
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=TechTalen_Lakehouse_Pipeline, job_id=211771276672039)
# or create a new job using: w.jobs.create(**TechTalen_Lakehouse_Pipeline.as_shallow_dict())
