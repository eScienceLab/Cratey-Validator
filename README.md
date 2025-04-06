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

2. Build and start the services using Docker Compose:
    ```bash
   docker compose build
   docker compose up
   ```

## Example Usage

```
curl -X POST http://localhost:5001/ro_crates/post/validate_by_id_no_webhook -d "id=1"
```
