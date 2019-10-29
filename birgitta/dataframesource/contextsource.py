"""The context_source module. Helper function to obtain the data frame source
from the context.
"""
from birgitta import context


__all__ = ['get', 'set']


def get():
    return context.get('BIRGITTA_DATAFRAMESOURCE')


def set(dataframe_source):
    return context.set('BIRGITTA_DATAFRAMESOURCE', dataframe_source)
