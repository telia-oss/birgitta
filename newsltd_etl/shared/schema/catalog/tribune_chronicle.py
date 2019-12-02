from ..catalog.chronicle import catalog as chronicle_catalog  # noqa 501
from ..catalog.tribune import catalog as tribune_catalog

catalog = chronicle_catalog.copy()
catalog.set_secondary(tribune_catalog)
