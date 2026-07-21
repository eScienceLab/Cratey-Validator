"""Entry point for the Flask application."""

from app import create_app
from app.services.logging_service import setup_logging

app = create_app()
setup_logging(app.config["SETTINGS"])

if __name__ == "__main__":
    # Run the Flask development server:
    app.run(host="0.0.0.0", debug=True)
