"""Defines main Blueprint and registers sub-Blueprints for organising related routes."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

#from flask import Flask

#from app.ro_crates.routes.post_routes import validate_ro_crate_from_id_no_webhook_bp, validate_ro_crate_from_id_bp
#from app.ro_crates.routes.post_routes import post_routes_bp
from app.ro_crates.routes.post_routes import api

#ro_crates_bp = Flask(__name__)

#ro_crates_bp.register_blueprint(validate_ro_crate_from_id_bp, url_prefix="/api/ro-crates")
#ro_crates_bp.register_blueprint(validate_ro_crate_from_id_no_webhook_bp, url_prefix="/api/ro-crates")
#ro_crates_bp.register_blueprint(post_routes_bp, url_prefix="/api")