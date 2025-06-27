# RO-Crate Validation Service

This project presents a Flask-based API for validating RO-Crates.

## Project Structure

```
app/
├── ro_crates/
│   ├── routes/
│   │   ├── __init__.py         # Registers blueprints
│   │   └── post_routes.py      # POST API routes
│   └── __init__.py             
├── services/
│   ├── logging_service.py      # Centralised logging
│   └── validation_service.py   # Queue RO-Crates for validation
├── tasks/
│   └── validation_tasks.py     # Validate RO-Crates
├── utils/
│   ├── config.py               # Configuration
│   ├── minio_utils.py          # Methods for interacting with MinIO
│   └── webhook_utils.py        # Methods for sending webhooks
```

## Setting up the project

### Prerequisites

- Docker with Docker Compose

### Installation

1. Clone the repository:
    ```bash
   git clone https://github.com/eScienceLab/Cratey-Validator.git
   cd crate-validation-service
   ```

2. Create the `.env` file for shared environment information. An example environment file is included (`example.env`), which can be copied for this purpose. But make sure to change any security settings (username and passwords).

3. Build and start the services using Docker Compose:
    ```bash
   docker compose up --build
   ```

4. Set up the MinIO bucket
   1. Open the MinIO web interface at `http://localhost:9000`.  
   2. Log in with your MinIO credentials.  
   3. Create a new bucket named `ro-crates`.  
   4. **Enable versioning** for the `ro-crates` bucket — this is important for tracking unique object versions.

      ![Ensure MinIO versioning is enabled](docs/assets/minio-versioning-enabled.webp "Ensure MinIO versioning is enabled")

   5. Upload your RO-Crate files to the `ro-crates` bucket.  
   6. To verify that versioning is enabled:
      - Select the uploaded RO-Crate object in the `ro-crates` bucket.
      - Navigate to the **Actions** panel on the right.
      - The **Display Object Versions** option should be clickable.

      ![Validate MinIO versioning is enabled](docs/assets/validate-minio-versioning-enabled.webp "Validate MinIO versioning is enabled")


## Development

For standard usage the Docker Compose script uses prebuilt containers.
For testing locally developed containers use the alternate Docker Compose file:
```bash
   docker compose --file docker-compose-develop.yml up --build
``` 

## Example Usage

Validation of RO-Crate with the ID of `1`. No webhook is used here: 
```bash
curl -X 'POST' \
  'http://localhost:5001/ro_crates/validate_by_id_no_webhook' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "crate_id": "1"
}'
```

Retrieval of validation result for RO-Crate `1`:
```bash
curl -X 'GET' \
  'http://localhost:5001/ro_crates/get_validation_by_id' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "crate_id": "1"
}'
```
