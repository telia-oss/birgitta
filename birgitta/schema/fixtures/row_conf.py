"""The RowConf class. Configures a fixture row.
"""
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
        return self.vals.get(field)
