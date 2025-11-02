# Openrelik worker kusto ingestor
## Description
This worker ingests .csv files into a kusto cluster using streaming.

Creates a new table using the name of the .csv file.

Streaming supports 10MB per chunk, .csv files larger than 10MB are split up into chunks and ingested one-by-one.

Supports an `.openrelik-config` files.

# TODO:
Supply an `.openrelik-config` file to this worker with an `openrelik-kusto-cluster-uri:` argument to use for the kusto cluster location. If you're running an extract from an archive task before this, place your `.openrelik-config` file in an archive (eg. `openrelik-config.zip`) and add globs for it (`*.openrelik-config`) to your extract from archive task.

## Deploy
Update your `config.env` file to set `OPENRELIK_WORKER_KUSTOINGEST_VERSION` to the tagged release version you want to use.

Add the below configuration to the OpenRelik docker-compose.yml file, you may need to update the `image:` value to point to the container in a  registry.

```
openrelik-worker-kustoingest:
    container_name: openrelik-worker-kustoingest
    image: openrelik-worker-kustoingest:${OPENRELIK_WORKER_KUSTOINGEST_VERSION}
    restart: always
    environment:
      - REDIS_URL=redis://openrelik-redis:6379
    volumes:
      - ./data:/usr/share/openrelik/data
    command: "celery --app=src.app worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-kustoingest"
    
    # ports:
      # - 5678:5678 # For debugging purposes.
```
