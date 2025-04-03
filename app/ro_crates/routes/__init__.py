"""Defines main Blueprint and registers sub-Blueprints for organising related routes."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from flask import Blueprint
from flask_restx import Api

from app.ro_crates.routes.post_routes import post_routes_bp

ro_crates_bp = Blueprint("ro_crates", __name__)
api = Api(ro_crates_bp, doc='/doc/')

ro_crates_bp.register_blueprint(post_routes_bp, url_prefix="/post")
