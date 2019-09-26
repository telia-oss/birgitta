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
from birgitta.schema.fixtures.values import dtvals

__all__ = ['Catalog']


class Catalog:
    def __init__(self, field_confs, secondary_field_confs=None):
        """
        Args:
            field_confs (dict): holds all fields with
            descriptions and example values.
            secondary_field_confs (dict): fall back dict enabling
            the combination of fields from separate domains.
        """
        self.field_confs = field_confs
        self.secondary_field_confs = secondary_field_confs

    def example_val(self,
                    field,
                    override_conf=None):
        """Get the example val for a field.

        Args:
            field (str): Field name.
            override_conf (dict): Overrides catalog fixture conf.

        Returns:
            example value
        """
        example_conf = self.get_example_conf(field, override_conf)
        return self.val_from_conf(field, example_conf)

    def get_example_conf(self, field, override):
        """Get theh example conf for a field.

        These are the possible sources of example confs:
        Default is the conf from the catalog.
        If there is an override, it is used instead.
        In this way, a more specific value can be used for specific cases.
        If none is found, then fall back to default date time val.
        This is useful for building date dimension tables.
        """
        conf = None
        if override:  # overrides all
            conf = override
        else:  # Look for a conf in the catalog
            conf = self.catalog_conf(field)
        if conf:
            return conf["example"]
        # Interpret field as generic datetime value if no conf found,
        # enabling writing schemas faster for date dimension tables.
        return {"dtval": field}

    def val_from_conf(self, field, example_conf):
        # If dtval, get standard time based value from dtvals lib.
        if "dtval" in example_conf:  # Use dtvals
            return dtval(example_conf)
        # If fn val, invoke the value generating function
        if "fn" in example_conf:
            return fnval(example_conf)
        # If static val, return it
        if "static" in example_conf:
            return staticval(field, example_conf)

    def catalog_conf(self, field):
        if field in self.field_confs:
            return self.field_confs[field]
        # If not in primary field_confs, test secondary_field_confs
        elif (self.secondary_field_confs and
              (field in self.secondary_field_confs)):
            return self.secondary_field_confs[field]
        return None


def dtval(example_conf):
    """Get a standard timebased value from the dtvals lib."""
    dt_field = example_conf['dtval']
    if "params" in example_conf:
        return dtvals.field_val(dt_field, example_conf["params"]["dt"])
    else:
        return dtvals.default_val(dt_field)


def fnval(example_conf):
    fn = example_conf["fn"]
    if "params" in example_conf:
        return fn(*list_if_not(example_conf["params"]))
    else:
        return fn()


def staticval(field, example_conf):
    val = example_conf["static"]
    if callable(val):
        raise ValueError(
            '"static" field %s cannot be callable, use "fn": %s' % (
                field, repr(val)))
    return example_conf["static"]


def list_if_not(params):
    if isinstance(params, list):
        return params
    else:
        return [params]
