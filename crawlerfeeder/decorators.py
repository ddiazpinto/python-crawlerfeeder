"""
Methods and functions decorators
Most of them are actually useless, but are created for future purposes
"""
import logging


def crawler(f):
    def wrapper(self, *args, **kwargs):
        return f(self, *args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper


def parser(f):
    def wrapper(self, *args, **kwargs):
        return f(self, *args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper


def feeder(f):
    def wrapper(self, *args, **kwargs):
        return f(self, *args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper


def wet(f):
    """
    Defines a method as a wet method.
    If the CrawlerFeeder is running in dry-run the method will not be executed
    """
    def wrapper(self, *args, **kwargs):
        if self._args.safe_run:
            logging.info("Skipping '%s' process due is executed in dry-run mode." % f.__name__)
        else:
            return f(self, *args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper


def unsafe(f):
    """
    Defines a method as a unsafe method.
    If the CrawlerFeeder is running in safe-run and there at least a warning the method will not be executed
    """
    def wrapper(self, *args, **kwargs):
        if self._args.safe_run and (self._warnings or self._errors):
            logging.info("Skipping '%s' process due is executed in safe-run mode and there are warnings or errors." % f.__name__)
        else:
            return f(self, *args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper
