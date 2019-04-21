from functools import partial, update_wrapper
from importlib import import_module
from inspect import getfullargspec


class dependencies_func:
    """
    A decorator to provide the modules as arguments to functions, which provides a dual advantage:
    1-  Avoid cyclic dependencies by passing the required modules as argument and not populating the global sys module.
    2- Doesn't populate local namespace.
    """
    def __init__(self, *args):
        self.deps = []

        for dep in args:
            self.deps.append(dep)

    def __call__(self, fn):
        spec = getfullargspec(fn)
        has_self = True if spec.args[0] in ['cls', 'self'] else False
        if has_self:
            raise TypeError("Not supported for methods.")
        update_wrapper(self.wrapped_func, fn)
        partial_fn = partial(fn)
        for dep in self.deps:
            partial_fn = partial(partial_fn, import_module(dep))
        update_wrapper(partial_fn, fn)
        return partial_fn
