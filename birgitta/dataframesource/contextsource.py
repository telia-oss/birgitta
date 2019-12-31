"""The context_source module. Helper function to obtain the data frame source
from the context.
"""
from birgitta import context

__all__ = ['get', 'set']


def get():
    source = context.get('BIRGITTA_DATAFRAMESOURCE')
    if source:
        return source
    source = derive_source()
    set(source)
    return source


def set(dataframe_source):
    context.set('BIRGITTA_DATAFRAMESOURCE', dataframe_source)


def derive_source():
    try:
        import dataiku
        try:
            # Ensure we have the actual dataiku.spark mod and not a mock
            if (
                    hasattr(dataiku, 'spark') and
                    'attach_mock' not in dir(dataiku.spark)
            ):
                from birgitta.dataframesource.sources.dataikusource import DataikuSource  # noqa E402
                return DataikuSource()
        except AttributeError:
            return None
    except ModuleNotFoundError:
        return None
