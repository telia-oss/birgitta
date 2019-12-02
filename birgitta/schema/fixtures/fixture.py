"""The Fixture class. Defines a fixture, including
a reference to the data set it creates data for.

A fixture is one of more rows of data to use as input for a test
or validation of the output.
"""
from .row_conf import RowConf
from .variants import RowConfsVariant, VariantBase

__all__ = ['Fixture']


class Fixture:
    def __init__(self,
                 dataset):
        """Constructs a Fixture object, focused on a specific dataset.

        Args:
            dataset (Dataset): the Dataset object of the fixture, used for
            creating fixture values. We use the schema and name defined there.
        """
        self.dataset = dataset
        self.variants = {}

    def df(self, variant_name, spark):
        """Constructs a fixtures dataframe with the variant.

        Args:
            variant_name (str): the name of the variant to be used
            spark (SparkSession): used for building the dataframes

        Returns:
            Dataframe of the fixtures.
        """
        variant = self.get_variant(variant_name)
        return variant.df(spark, self.dataset)

    def get_variant(self, variant_name):
        """
        Args:
            variant_name (str): the name of the variant to get

        Returns:
            VariantBase subclass object.

        If 'default' is not defined, then return default fallback."""
        if variant_name in self.variants:
            return self.variants[variant_name]
        if variant_name == 'default':
            return self.set_default_fallback()
        return None

    def set_default_fallback(self):
        """Set and return a fallback value for default.
        This fallback variant will, for every field, get
        the example value
        from the Catalog default of self.dataset.
        """
        variant = RowConfsVariant([RowConf()])
        self.variants['default'] = variant
        return variant

    def set_default_variant(self, variant=None):
        """Set the default variant.

        Args:
            variant (VariantBase): the variant for the fixture.
            If None, a single row RowConfsVariant fallback is added
            as default.
        """
        if not variant:
            self.set_default_fallback()
        else:
            self.set_variant('default', variant)

    def set_variant(self, name, variant):
        if not issubclass(type(variant), VariantBase):
            err_msg = 'variant (%s) must be a subclass of VariantType'\
                      % repr(variant)
            raise ValueError(err_msg)
        self.variants[name] = variant

    def add_variant(self, name, variant):
        """Adds a variant.

        Args:
            name (str): the variant name
            variant (VariantBase): the variant for the fixture
        """
        self.set_variant(name, variant)
