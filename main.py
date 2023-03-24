import argparse
import asyncio
import logging
import multiprocessing
import tracemalloc
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Thread
from time import time_ns, sleep
from typing import Callable, Awaitable, TypeVar

import temporalio
from pydantic import BaseSettings
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.service import TLSConfig
from temporalio.worker import Worker, SharedStateManager

from definitions import (
    WORKFLOW_NAME,
    TemporalPythonSdkIssue300Input,
    TemporalPythonSdkIssue300Output,
    TemporalPythonSdkIssue300,
    TEN_MINUTES,
    NO_RETRY,
)
from definitions import temporal_python_sdk_issue_300

logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("temporal")
logging.getLogger("temporalio.worker.workflow_sandbox._restrictions").setLevel(
    logging.CRITICAL
)

T = TypeVar("T", contravariant=True)
S = TypeVar("S", covariant=True)


class TemporalCredentials(BaseSettings):
    class Config:
        env_file = ".env"

    uri: str
    namespace: str
    queue_name: str
    task_prefix: str
    ca_pem_path: Path
    ca_key_path: Path


class SentryCredentials(BaseSettings):
    class Config:
        env_file = ".env"

    sentry_dsn: str


async def temporal_client(credentials: TemporalCredentials) -> Client:
    return await Client.connect(
        credentials.uri,
        namespace=credentials.namespace,
        tls=TLSConfig(
            client_cert=credentials.ca_pem_path.read_bytes(),
            client_private_key=credentials.ca_key_path.read_bytes(),
        ),
    )


async def execute_temporal_python_sdk_issue_300_input(
    client: temporalio.client.Client,
    data: TemporalPythonSdkIssue300Input,
    task_queue: str,
    task_id_prefix: str,
    execution_timeout: timedelta,
    retry_policy: temporalio.common.RetryPolicy,
) -> TemporalPythonSdkIssue300Output:
    result = await client.execute_workflow(
        workflow=WORKFLOW_NAME,
        arg=data,
        id=f"{task_id_prefix}-{WORKFLOW_NAME}-{time_ns()}",
        task_queue=task_queue,
        execution_timeout=execution_timeout,
        retry_policy=retry_policy,
    )
    return TemporalPythonSdkIssue300Output(**result)


def execute_workflow(
    executor: Callable[[Client, T, str, str, timedelta, RetryPolicy], Awaitable[S]],
    data: T,
    credentials: TemporalCredentials,
    execution_timeout: timedelta,
    retry_policy: RetryPolicy,
) -> S:
    async def execute_workflow_inner() -> S:
        client = await temporal_client(credentials)
        return await executor(
            client,
            data,
            credentials.queue_name,
            credentials.task_prefix,
            execution_timeout,
            retry_policy,
        )

    return asyncio.run(execute_workflow_inner())


def trigger(timeout: float, sub_workflows: int, sub_activities: int) -> None:
    temporal_credentials = TemporalCredentials()
    result = execute_workflow(
        execute_temporal_python_sdk_issue_300_input,
        data=TemporalPythonSdkIssue300Input(
            text="hello world",
            timeout=timeout,
            sub_workflows=sub_workflows,
            sub_activities=sub_activities,
        ),
        execution_timeout=TEN_MINUTES,
        retry_policy=NO_RETRY,
        credentials=temporal_credentials,
    )
    print(result)


async def log_error(exception: BaseException) -> None:
    # sentry_sdk.capture_exception(exception)
    logger.exception(f"temporal exception: {exception}")


def log_memory_usage() -> None:
    while True:
        # Take a snapshot of the memory allocations
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")

        # Print the memory allocation statistics
        print("Memory allocation statistics:")
        for stat in top_stats[:25]:
            print(stat)

        # Sleep for 30s
        sleep(30)


def memory_tracing() -> None:
    tracemalloc.start()
    logging_thread = Thread(target=log_memory_usage)
    logging_thread.start()


async def worker() -> None:
    memory_tracing()
    temporal_credentials = TemporalCredentials()
    client = await temporal_client(temporal_credentials)

    await Worker(
        client,
        task_queue=temporal_credentials.queue_name,
        workflows=[TemporalPythonSdkIssue300],
        activities=[temporal_python_sdk_issue_300],
        activity_executor=ProcessPoolExecutor(1),
        shared_state_manager=SharedStateManager.create_from_multiprocessing(
            multiprocessing.Manager()
        ),
        # interceptors=[SentryInterceptor()],
        on_fatal_error=log_error,
    ).run()


def main():
    # sentry_sdk.init(dsn=SentryCredentials().sentry_dsn, environment="local")

    parser = argparse.ArgumentParser(
        description="Trigger or receive a temporal workflow"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    trigger_parser = subparsers.add_parser("trigger", help="Trigger workflow")
    trigger_parser.add_argument(
        "--timeout", help="workflow waits for x seconds", type=float, default=5.0
    )
    trigger_parser.add_argument(
        "--sub-workflows", help="# of sub workflows", type=int, default=0
    )
    trigger_parser.add_argument(
        "--sub-activities", help="# of sub activities", type=int, default=1
    )
    subparsers.add_parser("worker", help="Worker")
    args = parser.parse_args()
    if args.command == "trigger":
        trigger(
            args.timeout,
            sub_workflows=args.sub_workflows,
            sub_activities=args.sub_activities,
        )
    elif args.command == "worker":
        asyncio.run(worker())
    else:
        raise ValueError(f"unknown command {args.command}")


if __name__ == "__main__":
    main()
