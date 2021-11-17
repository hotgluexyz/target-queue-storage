#!/usr/bin/env python3
import os
import json
import argparse
import logging
import jsonlines
import multiprocessing as mp

from azure.storage.queue import QueueClient

logger = logging.getLogger("target-queue-storage")
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path):
    with open(path) as f:
        return json.load(f)


def parse_args():
    '''Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
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


def process_part(args):
    # Unwrap tuple args
    msg_key, connect_string, q_name, root, file = args
    file_path = os.path.join(root, file)
    # Upload all data in input_path to Azure Queue Storage
    queue_client = QueueClient.from_connection_string(connect_string, q_name)

    logger.info(f"Exporting {file_path}")

    # Upload the file
    with jsonlines.open(file_path) as reader:
        logger.debug(f"Queueing {file_path} messages...")

        for payload in reader:
            # Queue each message individually
            message = json.dumps({
                'key': msg_key,
                'name': file,
                'payload': payload
            })

            queue_client.send_message(message)


def queue_message(args):
    # Unwrap tuple args
    msg_key, payload, connect_string, q_name, file = args
    # Create queue client
    queue_client = QueueClient.from_connection_string(connect_string, q_name)

    message = json.dumps({
        'key': msg_key,
        'name': file,
        'payload': payload
    })

    try:
        queue_client.send_message(message)
    except Exception as ex:
        if "maximum permissible limit." in str(ex):
            logger.warn("Skipping message because of size limits.")
            pass
        else:
            raise ex


def upload(args):
    logger.info(f"Exporting data...")
    config = args.config
    q_name = config['queue']
    local_path = config['input_path']
    connect_string = config['connect_string']
    msg_key = config['path_prefix']

    # Upload all data in input_path to Azure Queue Storage
    queue_client = QueueClient.from_connection_string(connect_string, q_name)

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
                    logger.debug(f"Queueing {len(data)} messages...")

                    # async queue messages
                    pool.map_async(queue_message, [(msg_key, payload, connect_string, q_name, file) for payload in data]).get()

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
