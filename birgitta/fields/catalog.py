"""Field / feature catalog class. This catalog is per project/domain.
It holds a list of fields with description and example values.
It does not hold field types. The field types themselves are defined
in specific data set schemas, for clarity and more intuitive schema APIs.
Additionally, by decoupling data type from field description and example_val,
The same example value e.gg. 2017-01-01 can be interpreted as date, datetime
or string, depending on the schema needs.

The purpose of the catalog is:

1. generate example fixtures for recipe tests in an efficient manner
2. reuse default values across data set fixtures for simplicity
3. promote reuse of field names across data sets
4. document all fields and eventually enable automatic publishing of these
comments in views and reports
"""
from birgitta.schema.fixtures import ExampleVal


__all__ = ['Catalog']


class Catalog:
    def __init__(self):
        """Creates an empty catalog.
        """
        self.field_confs = {}
        self.secondary_catalog = None

    def set_secondary(self, catalog):
        """Set secondary catalog to fall back on for missing fields.
        Args:
            secondary_catalog (Catalog): fall back Catalog
        """
        self.secondary_catalog = catalog

    def copy(self):
        cat = Catalog()
        cat.field_confs = self.field_confs
        cat.set_secondary(self.secondary_catalog)
        return cat

    def add_field(self,
                  name,
                  *,
                  example,
                  description):
        self.field_confs[name] = {
            "example": example,
            "description": description
        }

    def example_val(self,
                    field):
        """Get the example val for a field.

        Args:
            field (str): Field name.
            override_conf (dict): Overrides catalog fixture conf.

        Returns:
            example value
        """
        field_conf = self.get_field_conf(field)
        return ExampleVal(field_conf['example'])

    def get_field_conf(self, field):
        if field in self.field_confs:
            return self.field_confs[field]
        # If not in primary field_confs, look in secondary catalog
        if not self.secondary_catalog:
            raise ValueError("Missing field %s" % (field))
        return self.secondary_catalog.get_field_conf(field)
