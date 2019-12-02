"""ExampleVal class. The purpose is to simplify
conditional logic when evaluating schema DSLs."""

__all__ = ['ExampleVal']


class ExampleVal:
    def __init__(self, val):
        """
        Args:
            val (val): The example value
        """
        self.val = val
