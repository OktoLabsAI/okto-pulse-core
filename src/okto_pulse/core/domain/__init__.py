"""Pure domain policy/predicate leaves (no DB/session/SQLAlchemy imports).

Modules here are safe to import from models, services and the bug-regression
resolver/gate without pulling heavy database dependencies.
"""
