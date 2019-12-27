"""The RowConf class. Configures a fixture row.
"""
from birgitta.schema.fixtures import ExampleVal

__all__ = ['RowConf']


class RowConf:
    def __init__(self):
        """Constructs an empty RowConf.
        """
        self.vals = {}

    def set_field(self, field, val):
        """Set the value of one of the fields of the row"""
        if callable(val):
            raise ValueError("Val %s cannot be a function" % repr(val))
        self.vals[field] = val
        return self

    def get_field(self, field):
        """Get the value of one of the fields of the row.
        If it exists."""
        if not self.has_field(field):
            return None
        return ExampleVal(self.vals.get(field))

    def has_field(self, field):
        return field in self.vals
