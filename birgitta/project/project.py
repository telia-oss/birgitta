"""The Project class. Defines data set name and schema.
"""

__all__ = ['Project']


class Project:
    def __init__(self, name):
        """Constructs a project object.

        The name is important to give flexibility with regards to
        the underlying project naming.

        Args:
            name (str): The actual table name.
        """
        self.name = name
