"""Entry point for the Flask application."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from app import create_app
from app.services.logging_service import setup_logging

app = create_app()
setup_logging()

if __name__ == "__main__":
    # Run the Flask development server:
    app.run(host="0.0.0.0", debug=True)
