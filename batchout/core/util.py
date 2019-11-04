

def to_iter(obj):
    try:
        return iter(obj)
    except TypeError:
        return obj,
