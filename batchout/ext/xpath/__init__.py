try:
    import lxml
except ImportError:
    pass
else:
    from .extractors import XPathExtractor
