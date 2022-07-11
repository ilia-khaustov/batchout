try:
    import jsonpath_rw
except ImportError:
    pass
else:
    from .extractors import JsonpathExtractor
