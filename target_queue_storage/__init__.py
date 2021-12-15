#!/usr/bin/env python3
import os
import json
import argparse
import logging
import jsonlines
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed

from azure.storage.queue import QueueClient


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


def queue_message(msg_key, acc_payload, connect_string, q_name, file):
    # Create queue client
    queue_client = QueueClient.from_connection_string(connect_string, q_name)
    
    payloads_sent = 0
    for payload in acc_payload:
        message = json.dumps({
            'key': msg_key,
            'name': file,
            'payload': payload
        })

        try:
            status = queue_client.send_message(message)
            payloads_sent = payloads_sent + 1
        except Exception as ex:
            if "maximum permissible limit." in str(ex):
                logger.warn("Skipping message because of size limits.")
                pass
            else:
                raise ex
    return payloads_sent


def batch_queue(data, msg_key, connect_string, q_name, file, chunk=512):
    threads= []
    queues_count = 0
    with ThreadPoolExecutor(16) as executor:
        acc_payload = []
        for payload in data:
            acc_payload.append(payload)
            if len(acc_payload)>=chunk:
                exc = executor.submit(queue_message, msg_key, acc_payload, connect_string, q_name, file)
                threads.append(exc)
                acc_payload=[]
        exc = executor.submit(queue_message, msg_key, acc_payload, connect_string, q_name, file)
        threads.append(exc)
        for task in as_completed(threads):
            queues_count += task.result()
    logger.info(f"{queues_count} Queues sent to {q_name} from {file}")



def process_part(args):
    # Unwrap tuple args
    msg_key, connect_string, q_name, root, file = args
    file_path = os.path.join(root, file)
    # Upload all data in input_path to Azure Queue Storage

    logger.info(f"Exporting {file_path}")

    # Upload the file
    with jsonlines.open(file_path) as reader:
        logger.info(f"Queueing {file_path} messages...")
        batch_queue(reader, msg_key, connect_string, q_name, file)


def upload(args):
    logger.info(f"Exporting data...")
    config = args.config
    q_name = config['queue']
    local_path = config['input_path']
    connect_string = config['connect_string']
    msg_key = config['path_prefix']

    # Create multiprocessing pool
    pool = mp.Pool(mp.cpu_count())

    for root, dirs, files in os.walk(local_path):
        # Handle queuing .part files
        pool.map_async(process_part, [(msg_key, connect_string, q_name, root, f) for f in files if f.endswith(".part")]).get()

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
                    batch_queue(data, msg_key, connect_string, q_name, file)

    # Close the processing pool
    pool.close()

    logger.info(f"Data exported.")


def main():
    # Parse command line arguments
    args = parse_args()

    # Upload the data
    upload(args)


if __name__ == "__main__":
    main()
