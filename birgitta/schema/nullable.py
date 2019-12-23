"""ExampleVal class. The purpose is to simplify
conditional logic when evaluating schema DSLs."""

__all__ = ['Nullable']


class Nullable:
    def __init__(self, val):
        """
        Args:
            val (val): Nullable bool, True or False
        """
        self.val = val
