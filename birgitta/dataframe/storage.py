"""Utility functions finding which storage type is used."""

from birgitta import context

__all__ = ['stored_in']


def stored_in(t):
    """Returns true if storage type equals t"""
    storage_type = context.get("BIRGITTA_DATASET_STORAGE")
    return t == storage_type
