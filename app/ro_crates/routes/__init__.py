"""Defines main Blueprint and registers sub-Blueprints for organising related routes."""

from app.ro_crates.routes.post_routes import post_routes_bp, minio_post_routes_bp
from app.ro_crates.routes.get_routes import get_routes_bp

# Always registered:
v1_post_bp = post_routes_bp

# Registered only when object storage is enabled:
v1_minio_post_bp = minio_post_routes_bp
v1_minio_get_bp = get_routes_bp
