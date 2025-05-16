"""Defines main Blueprint and registers sub-Blueprints for organising related routes."""

# Author: Alexander Hambley
# License: MIT
# Copyright (c) 2025 eScience Lab, The University of Manchester

from apiflask import APIBlueprint

from app.ro_crates.routes.post_routes import post_routes_bp
from app.ro_crates.routes.get_routes import get_routes_bp

#ro_crates_bp = APIBlueprint("ro_crates", __name__)

#ro_crates_bp.register_blueprint(post_routes_bp, url_prefix="/post")

ro_crates_post_bp = post_routes_bp
ro_crates_get_bp = get_routes_bp