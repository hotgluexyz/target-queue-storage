#!/usr/bin/env python3
import os
import json
import argparse
import logging
import jsonlines

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


def upload(args):
    logger.info(f"Exporting data...")
    config = args.config
    q_name = config['queue']
    local_path = config['input_path']
    connect_string = config['connect_string']
    msg_key = config['path_prefix']

    # Upload all data in input_path to Azure Queue Storage
    queue_client = QueueClient.from_connection_string(connect_string, q_name)

    for root, dirs, files in os.walk(local_path):
        for file in [f for f in files if f.endswith(".part")]:
            logger.info(f"Exporting {file}")
            file_path = os.path.join(root, file)

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

    logger.info(f"Data exported.")


def main():
    # Parse command line arguments
    args = parse_args()

    # Upload the data
    upload(args)


if __name__ == "__main__":
    main()
