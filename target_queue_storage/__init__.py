#!/usr/bin/env python3
import asyncio
import os
import json
import argparse
import logging
import threading
import time
import jsonlines

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

_queue_client = None
_lock = threading.Lock()

# Concurrency semaphore (initialized in upload)
inflight_semaphore = None

def get_queue_client(connect_string, q_name):
    global _queue_client
    if _queue_client is None:
        _queue_client = QueueClient.from_connection_string(connect_string, q_name)
    return _queue_client

async def send_one(p, msg_key, file, client):
    async with inflight_semaphore:
        try:
            payload_obj = {'key': msg_key, 'name': file, 'payload': p}
            payload_str = json.dumps(payload_obj)
            await client.send_message(payload_str)
            return True
        except Exception as e:
            logger.error(f"Error sending message from {file}: {e}")
            return False

def process_part(args):
    # Unwrap tuple args
    msg_key, root, file, client = args
    file_path = os.path.join(root, file)
    # Upload all data in input_path to Azure Queue Storage

    logger.info(f"Exporting {file_path}")

    # Upload the file
    with jsonlines.open(file_path) as reader:
        logger.info(f"Queueing {file_path} messages...")
        # Convert reader to list to get count and create tasks for all messages
        data = list(reader)
        total_messages = len(data)
        logger.info(f"Creating {total_messages} tasks for {file}")
        
        # Create tasks for all messages
        tasks = [asyncio.create_task(send_one(p, msg_key, file, client)) for p in data]
        return tasks


def process_json(args):
    # Unwrap tuple args
    msg_key, root, file, client = args
    file_path = os.path.join(root, file)
    # Upload all data in input_path to Azure Queue Storage

    logger.info(f"Exporting {file_path}")

    # Upload the file
    with open(file_path, "r") as f:
        # Read the JSON file
        data = json.loads(f.read())

        # Verify the data is a list
        if isinstance(data, list):
            # Queue each message individually
            total_messages = len(data)
            logger.info(f"Creating {total_messages} tasks for {file}")
            
            # Create tasks for all messages
            tasks = [asyncio.create_task(send_one(p, msg_key, file, client)) for p in data]
            return tasks
        else:
            logger.warning(f"JSON file {file} does not contain a list, skipping...")
            return []


async def monitor_progress(completed_counter, total_tasks):
    """Monitor progress of tasks and log every 2 seconds"""
    start_time = time.perf_counter()
    
    while completed_counter['count'] < total_tasks:
        await asyncio.sleep(2)
        
        completed = completed_counter['count']
        
        if completed < total_tasks:
            elapsed = time.perf_counter() - start_time
            progress = (completed / total_tasks) * 100
            remaining = total_tasks - completed
            
            # Estimate remaining time
            if completed > 0:
                rate = completed / elapsed
                eta = remaining / rate if rate > 0 else 0
                logger.info(f"Progress: {completed}/{total_tasks} ({progress:.1f}%) - {remaining} remaining - ETA: {eta:.1f}s")
            else:
                logger.info(f"Progress: {completed}/{total_tasks} ({progress:.1f}%) - {remaining} remaining")


async def upload(args):
    start_time = time.perf_counter()
    start_process_time = time.process_time()
    
    logger.info(f"Exporting data...")
    config = args.config
    q_name = config['queue']
    local_path = config['input_path']
    connect_string = config['connect_string']
    msg_key = config['path_prefix']

    client = get_queue_client(connect_string, q_name)
    async with client:
        global inflight_semaphore
        max_concurrency = int(config.get('max_concurrency', 1000))
        inflight_semaphore = asyncio.Semaphore(max_concurrency)

        all_tasks = []
        for root, _, files in os.walk(local_path):
            # Handle queuing .part and .json files concurrently
            part_files = [f for f in files if f.endswith(".part")]
            json_files = [f for f in files if f.endswith(".json")]
            
            # Collect tasks from all files
            for f in part_files:
                tasks = process_part((msg_key, root, f, client))
                all_tasks.extend(tasks)
            
            for f in json_files:
                tasks = process_json((msg_key, root, f, client))
                all_tasks.extend(tasks)
        
        if all_tasks:
            total_tasks = len(all_tasks)
            logger.info(f"Starting to send {total_tasks} total messages...")
            
            # Create a shared counter for progress monitoring
            completed_counter = {'count': 0}
            
            # Start progress monitoring task
            progress_task = asyncio.create_task(monitor_progress(completed_counter, total_tasks))
            
            # Execute all tasks with progress tracking
            results = []
            for coro in asyncio.as_completed(all_tasks):
                result = await coro
                results.append(result)
                completed_counter['count'] += 1
            
            # Cancel progress monitoring
            progress_task.cancel()
            
            # Count total successes and errors
            total_successes = sum(1 for result in results if result)
            total_errors = sum(1 for result in results if not result)
            
            logger.info(f"All processing completed: {total_successes} messages sent, {total_errors} failed")
        else:
            logger.info("No files to process")

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
    # print(timeit.repeat(lambda: asyncio.run(upload(args)), repeat=3, number=3))
    asyncio.run(upload(args))

if __name__ == "__main__":
    main()
