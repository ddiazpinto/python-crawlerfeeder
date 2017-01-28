"""
Cli functions and formatters
"""
import os
import logging
import argparse
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser
from datetime import datetime


class LoggingFormatter(logging.Formatter):
    FORMATS = {
        logging.DEBUG:"[%(asctime)s] DEBUG: %(module)s: %(lineno)d: %(message)s",
        logging.INFO: "[%(asctime)s] %(message)s",
        'DEFAULT': "[%(asctime)s] %(levelname)s: %(message)s"}

    def format(self, record):
        self._fmt = self.FORMATS.get(record.levelno, self.FORMATS['DEFAULT'])
        return logging.Formatter.format(self, record)


class FileExistsAction(argparse.Action):
    """
    Argparse action to validate an argument as a valid file, transforming the string path
    into an opened file descriptor. For inheritance purposes, the open file descriptor is returned
    into the __call__ function.
    """
    def __call__(self, parser, namespace, values, option_string=None):
        """
        Chech if the file exists

        Returns:
            a file descriptor
        """
        if not os.path.exists(values):
            parser.error("The file %s does not exist." % values)
        else:
            f = open(values, 'r')
            setattr(namespace, self.dest, f)
            return f


class ConfigFileAction(FileExistsAction):
    """
    Argparse action to validate an argument as a valid INI configuration file, transforming the string
    path into a ConfigParse object. __call__ function inherit from FileExists, recovering the opened file
    descriptor.
    """
    def __call__(self, parser, namespace, values, option_string=None):
        f = super(ConfigFileAction, self).__call__(parser, namespace, values, option_string=None)
        config = ConfigParser()
        config.readfp(f)
        setattr(namespace, self.dest, config)


class ValidDateAction(argparse.Action):
    """
    Argparse action to validate an argument as a valid date, transforming the string into a date.
    """
    def __call__(self, parser, namespace, values, option_string=None):
        try:
            values = datetime.strptime(values, '%Y-%m-%d')
        except ValueError:
            parser.error("The date in %s must be a valid date.")
        setattr(namespace, self.dest, values)
        return values


def get_argparser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Crawler & Feeder. Executes a list of tasks in a three steps process "
                    "to crawl and feed from data sources to data sources.")
    required = parser.add_argument_group('required arguments')
    required.add_argument(
        "--config", "-c", required=True, action=ConfigFileAction,
        help="Path to INI file that contains the process configuration.")
    required.add_argument(
        "--process-set", "-p", default='main',
        help="Specifies the set of tasks that will be process.")
    parser.add_argument(
        "--output-file",
        help="Saves pending changes into the given file, before make any change in database.")

    execution = parser.add_mutually_exclusive_group(required=False)
    execution.add_argument(
        "--dry-run", action="store_true",
        help="Executes all the processes except those that are in 'feed' step.")
    execution.add_argument(
        "--skip-unsafe", action="store_true",
        help="Skip executing feed step if there are errors or warnings.")
    execution.add_argument(
        "--safe-run", action="store_true",
        help="Executes all the processes except those are marked as unsafe if there are errors or warnings.")

    verbose = parser.add_mutually_exclusive_group(required=False)
    verbose.add_argument(
        '-d', '--debug', action="store_const", dest="loglevel", const=logging.DEBUG, default=logging.WARNING,
        help="Print lots of debugging statements")
    verbose.add_argument(
        '-v', '--verbose', action="store_const", dest="loglevel", const=logging.INFO,
        help="Be verbose")
    return parser
