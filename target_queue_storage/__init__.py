#!/usr/bin/env python3
import asyncio
import os
import json
import argparse
import logging
import threading
import time
import jsonlines
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed

from azure.storage.queue.aio import QueueClient

import timeit

from async_token_bucket import AsyncTokenBucket

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

def get_queue_client(connect_string, q_name):
    global _queue_client
    if _queue_client is None:
        with _lock:
            if _queue_client is None:
                logger.info("Making client")
                _queue_client = QueueClient.from_connection_string(connect_string, q_name)
    return _queue_client

limiter = AsyncTokenBucket(rate=20000, capacity=20000)

async def send_all(payloads, conn_str, q_name, msg_key, file):
    time_start = time.perf_counter(), time.process_time()
    logger.info(f"Sending {len(payloads)} messages...")
    client = QueueClient.from_connection_string(conn_str, q_name)
    successes = 0
    errors = 0

    async with client:
        async def send_one(p):
            nonlocal successes, errors
            msg = json.dumps({'key': msg_key, 'name': file, 'payload': p})
            await limiter.acquire()
            try:
                await client.send_message(msg)
                successes += 1
            except Exception:
                errors += 1

        tasks = [asyncio.create_task(send_one(p)) for p in payloads]

        for fut in asyncio.as_completed(tasks):
            await fut

    time_end = time.perf_counter(), time.process_time()
    logger.info(f"Time taken: {time_end[0] - time_start[0]} seconds, {time_end[1] - time_start[1]} seconds")

    return successes, errors

async def batch_queue(data, msg_key, connect_string, q_name, file, chunk=512):
    acc_payload = []
    for payload in data:
        acc_payload.append(payload)
        if len(acc_payload)>=chunk:
            await send_all(acc_payload, connect_string, q_name, msg_key, file)            
            acc_payload=[]

    await send_all(acc_payload, connect_string, q_name, msg_key, file)

async def process_part(args):
    # Unwrap tuple args
    msg_key, connect_string, q_name, root, file = args
    file_path = os.path.join(root, file)
    # Upload all data in input_path to Azure Queue Storage

    logger.info(f"Exporting {file_path}")

    # Upload the file
    with jsonlines.open(file_path) as reader:
        logger.info(f"Queueing {file_path} messages...")
        await batch_queue(reader, msg_key, connect_string, q_name, file)


async def upload(args):
    start_time = time.perf_counter()
    start_process_time = time.process_time()
    
    logger.info(f"Exporting data...")
    config = args.config
    q_name = config['queue']
    local_path = config['input_path']
    connect_string = config['connect_string']
    msg_key = config['path_prefix']


    for root, dirs, files in os.walk(local_path):
        # Handle queuing .part files
        await asyncio.gather(*[process_part((msg_key, connect_string, q_name, root, f)) for f in files if f.endswith(".part")])

        # Process JSON files
        for file in [f for f in files if f.endswith(".json")]:
            logger.info(f"Exporting {file}")
            file_path = os.path.join(root, file)

            # Upload the file
            with open(file_path, "r") as f:
                # Read the JSON file
                data = json.loads(f.read())

                # Verify the data is a list
                if isinstance(data, list):
                    # Queue each message individually
                    logger.info(f"Queueing {len(data)} messages...")
                    # async queue messages
                    await batch_queue(data, msg_key, connect_string, q_name, file)

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
