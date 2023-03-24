import asyncio
from dataclasses import dataclass
from datetime import timedelta
from time import sleep
from typing import Final

import temporalio
from temporalio import workflow, activity
from temporalio.common import RetryPolicy
from temporalio.workflow import execute_activity

WORKFLOW_NAME: Final = "temporal-sdk-python-issue-300"
ACTIVITY_NAME: Final = "temporal-sdk-python-issue-300"
ONE_MINUTE = timedelta(minutes=1)
TEN_MINUTES: Final = timedelta(minutes=10)
NO_RETRY: Final = RetryPolicy(maximum_attempts=1)


@dataclass
class TemporalPythonSdkIssue300Input:
    text: str
    timeout: float
    sub_workflows: int
    sub_activities: int


@dataclass
class TemporalPythonSdkIssue300Output:
    text: str


@activity.defn(name=ACTIVITY_NAME)
def temporal_python_sdk_issue_300(data: TemporalPythonSdkIssue300Input) -> float:
    print(f"sleeping again for {data.timeout}s")
    sleep(data.timeout)
    return 1000 * data.timeout


@workflow.defn(name=WORKFLOW_NAME)
class TemporalPythonSdkIssue300:
    @workflow.run
    async def run(
        self, data: TemporalPythonSdkIssue300Input
    ) -> TemporalPythonSdkIssue300Output:

        print(
            f"going to sleep for {data.timeout}s with the following message: {data.text}"
        )
        await asyncio.sleep(data.timeout)
        print(f"waking up to: {data.text} after {data.timeout}s")

        workflows = []
        workflows += [
            execute_activity(
                WORKFLOW_NAME, data, start_to_close_timeout=timedelta(minutes=10)
            )
            for i in range(data.sub_activities)
        ]

        workflows += [
            temporalio.workflow.execute_child_workflow(
                workflow=WORKFLOW_NAME,
                arg=TemporalPythonSdkIssue300Input(
                    sub_workflows=0,
                    sub_activities=0,
                    timeout=data.timeout,
                    text=f"{data.text}-{i}",
                ),
                execution_timeout=ONE_MINUTE,
                run_timeout=ONE_MINUTE,
                retry_policy=NO_RETRY,
            )
            for i in range(data.sub_workflows)
        ]

        results = await asyncio.gather(*workflows)
        print("result:", results)

        return TemporalPythonSdkIssue300Output(text=f"{data.timeout}s later...")
