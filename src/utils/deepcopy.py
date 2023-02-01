import pickle


def deepcopy(item):
    """
    deep copy an item using pickle library or copier attached to that (specified by __deepcopy__)

    Args:
        item: the object to be deep copied

    Returns:
        a new object deep copied from the object

    """
    copier = getattr(item, "__deepcopy__", None)
    if copier is not None:
        return copier(item)
    else:
        return pickle.loads(pickle.dumps(item))
