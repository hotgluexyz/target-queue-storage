#!/usr/bin/env python3
import asyncio
import os
import json
import argparse
import logging
import time
import jsonlines

from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
from azure.storage.queue.aio import QueueClient

logger = logging.getLogger("target-queue-storage")
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

def load_json(path):
    with open(path) as f:
        return json.load(f)


def parse_args():
    '''Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    return args

MAX_RETRIES = 3

_queue_client = None

# Concurrency semaphore (initialized in upload)
inflight_semaphore = None

def get_queue_client(connect_string, q_name):
    global _queue_client
    if _queue_client is None:
        _queue_client = QueueClient.from_connection_string(connect_string, q_name)
    return _queue_client

def _http_error_summary(e: HttpResponseError) -> str:
    """Return 'HTTP {status} [{error_code}] ({reason})', omitting error_code when absent."""
    raw = getattr(e, "error_code", None)
    error_code = f" {getattr(raw, 'value', raw)}" if raw else ""
    return f"HTTP {e.status_code}{error_code} ({e.reason})"

async def send_one(p, msg_key, file, client, retry_count=0):
    async with inflight_semaphore:
        try:
            payload_obj = {'key': msg_key, 'name': file, 'payload': p}
            payload_str = json.dumps(payload_obj)
            await client.send_message(payload_str)
            return True
        except Exception as e:
            summary = _http_error_summary(e) if isinstance(e, HttpResponseError) else str(e)
            if isinstance(e, ClientAuthenticationError):
                logger.error(f"Authentication error for {file} ({summary}): check your connection string")
                return False
            if retry_count >= MAX_RETRIES:
                logger.error(f"Failed to send message from {file} after {retry_count + 1} attempts: {summary}")
                return False
            logger.warning(f"Transient error sending message from {file}, attempt {retry_count + 1}/{MAX_RETRIES + 1}: {summary}, retrying...")

    await asyncio.sleep(2 ** retry_count)
    return await send_one(p, msg_key, file, client, retry_count + 1)


def _collect_export_files(local_path):
    """Return sorted (file_path, file_name, kind) tuples for .part and .json inputs."""
    entries = []
    for root, _, files in os.walk(local_path):
        for name in files:
            if name.endswith(".part"):
                entries.append((os.path.join(root, name), name, "part"))
            elif name.endswith(".json"):
                entries.append((os.path.join(root, name), name, "json"))
    entries.sort(key=lambda item: item[0])
    return entries


def _count_task_results(done_tasks):
    """Return (successes, failures) tallies from completed asyncio tasks."""
    successes = failures = 0
    for task in done_tasks:
        if task.result():
            successes += 1
        else:
            failures += 1
    return successes, failures


async def export_records(record_iter, msg_key, file_name, client, max_concurrency, stats=None):
    """Send each record to the queue, keeping at most max_concurrency sends active at once."""
    pending = set()
    successes = failures = 0

    def record_results(done_tasks):
        nonlocal successes, failures
        s, f = _count_task_results(done_tasks)
        successes += s
        failures += f
        if stats is not None:
            stats["successes"] += s
            stats["failures"] += f

    for record in record_iter:
        pending.add(asyncio.create_task(send_one(record, msg_key, file_name, client)))
        if len(pending) >= max_concurrency:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            record_results(done)

    while pending:
        done, pending = await asyncio.wait(pending)
        record_results(done)

    return successes, failures


async def export_part_file(file_path, file_name, msg_key, client, max_concurrency, stats=None):
    """Stream a JSONL .part file to the queue."""
    with jsonlines.open(file_path) as reader:
        return await export_records(reader, msg_key, file_name, client, max_concurrency, stats)


async def export_json_file(file_path, file_name, msg_key, client, max_concurrency, stats=None):
    """Load a JSON array file and send each element to the queue."""
    with open(file_path, "r") as f:
        data = json.loads(f.read())
    if not isinstance(data, list):
        logger.warning(f"JSON file {file_name} does not contain a list, skipping...")
        return 0, 0
    return await export_records(iter(data), msg_key, file_name, client, max_concurrency, stats)


async def monitor_send_progress(stats, stop_event):
    """Log aggregate send counts every 2 seconds while export is running."""
    start_time = time.perf_counter()
    while not stop_event.is_set():
        await asyncio.sleep(2)
        if stop_event.is_set():
            break
        sent = stats["successes"]
        failed = stats["failures"]
        elapsed = time.perf_counter() - start_time
        if sent > 0 and elapsed > 0:
            rate = sent / elapsed
            logger.info(
                f"Progress: {sent} sent, {failed} failed "
                f"({rate:.0f} msg/s over {elapsed:.0f}s)"
            )
        else:
            logger.info(f"Progress: {sent} sent, {failed} failed")


async def upload(args):
    start_time = time.perf_counter()
    start_process_time = time.process_time()

    logger.info(f"Exporting data...")
    config = args.config
    q_name = config['queue']
    local_path = config['input_path']
    connect_string = config['connect_string']
    msg_key = config['path_prefix']

    export_files = _collect_export_files(local_path)
    if not export_files:
        logger.info("No files to process")
        end_time = time.perf_counter()
        end_process_time = time.process_time()
        return end_time - start_time, end_process_time - start_process_time

    file_count = len(export_files)
    logger.info(f"Found {file_count} file(s) to export under {local_path}")

    client = get_queue_client(connect_string, q_name)
    async with client:
        global inflight_semaphore
        max_concurrency = int(config.get('max_concurrency', 1000))
        inflight_semaphore = asyncio.Semaphore(max_concurrency)

        stats = {"successes": 0, "failures": 0}
        stop_event = asyncio.Event()
        progress_task = asyncio.create_task(monitor_send_progress(stats, stop_event))

        try:
            for index, (file_path, file_name, kind) in enumerate(export_files, start=1):
                logger.info(f"Exporting file {index}/{file_count}: {file_name}")
                if kind == "part":
                    sent, failed = await export_part_file(
                        file_path, file_name, msg_key, client, max_concurrency, stats
                    )
                else:
                    sent, failed = await export_json_file(
                        file_path, file_name, msg_key, client, max_concurrency, stats
                    )
                logger.info(f"Finished {file_name}: {sent} sent, {failed} failed")
        finally:
            stop_event.set()
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass

        logger.info(
            f"All processing completed: {stats['successes']} messages sent, "
            f"{stats['failures']} failed"
        )
        if stats["failures"] > 0:
            raise RuntimeError(
                f"Export failed: {stats['failures']} messages could not be sent to the queue"
            )

    end_time = time.perf_counter()
    end_process_time = time.process_time()

    total_wall_time = end_time - start_time
    total_process_time = end_process_time - start_process_time

    logger.info(f"Data exported.")
    logger.info(f"Total upload time - Wall time: {total_wall_time:.2f} seconds, Process time: {total_process_time:.2f} seconds")

    return total_wall_time, total_process_time

def main():
    # Parse command line arguments
    args = parse_args()

    # Upload the data
    asyncio.run(upload(args))

if __name__ == "__main__":
    main()
