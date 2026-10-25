"""Shared service namespace, including TV5's existing member-local API package."""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
