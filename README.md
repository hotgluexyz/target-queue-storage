# target-queue-storage

`target-queue-storage` is a Singer target for [Azure Queue Storage](https://learn.microsoft.com/en-us/azure/storage/queues/). It reads local `.part` (JSONL) and `.json` files produced by a Singer tap and publishes each record as a message to an Azure queue.

## Installation

```bash
pip install target-queue-storage
```

Or install from source:

```bash
pip install git+https://github.com/hotgluexyz/target-queue-storage.git
```

## Configuration

### Accepted Config Options

| Field | Required | Description |
|---|---|---|
| `queue` | Yes | Name of the Azure Storage queue to send messages to |
| `connect_string` | Yes | Azure Storage connection string (from the portal under "Access keys") |
| `input_path` | Yes | Local directory containing the `.part` or `.json` files to enqueue |
| `path_prefix` | Yes | Value set as the `key` field in every enqueued message payload |
| `max_concurrency` | No | Max number of in-flight send operations at once (default: `1000`) |

Example `config.json`:

```json
{
  "queue": "my-queue",
  "connect_string": "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=abc123==;EndpointSuffix=core.windows.net",
  "input_path": "/path/to/singer/output",
  "path_prefix": "exports/contacts",
  "max_concurrency": 500
}
```

### Message format

Each message sent to the queue is a JSON string with this shape:

```json
{
  "key": "<path_prefix>",
  "name": "<source filename>",
  "payload": { "<original record>" }
}
```

### Authentication

Credentials come from the Azure Storage connection string. You can find it in the [Azure Portal](https://portal.azure.com) under your Storage Account > **Security + networking** > **Access keys**.

For local development, you can use [Azurite](https://github.com/Azure/Azurite) and set `connect_string` to `UseDevelopmentStorage=true`.

## Usage

### Run directly

```bash
target-queue-storage --help
target-queue-storage -c config.json
```

### Pipe from a Singer tap

```bash
tap-something -c tap_config.json | target-queue-storage -c config.json
```

The target expects its input data to already be on disk at `input_path` — Singer messages piped via stdin are not processed; the tap should write its output files to `input_path` first.

## Developer setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Run the target against a local data directory:

```bash
target-queue-storage -c .secrets/config.json
```
