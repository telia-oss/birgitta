"""The context_source module. Helper function to obtain the data frame source
from the context.
"""
from birgitta import context
from birgitta.dataiku import platform as dkuplatform

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
    if dkuplatform.is_current_platform():
        from birgitta.dataframesource.sources.dataikusource import DataikuSource  # noqa E402
        return DataikuSource()
    return None
