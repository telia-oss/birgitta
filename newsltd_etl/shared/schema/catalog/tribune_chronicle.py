from birgitta.fields.catalog import Catalog
from ..catalog.chronicle import catalog as chronicle_catalog  # noqa 501
from ..catalog.tribune import catalog as tribune_catalog

catalog = Catalog(tribune_catalog.field_confs, chronicle_catalog.field_confs)
