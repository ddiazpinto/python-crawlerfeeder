"""
    Crawler & Feeder
    Executes a list of tasks in a three steps process to crawl and feed from data sources to data sources.

    @TODO: Use the same aproach than crawl() process to parse and feed in order to auto save returning data
    @TODO: Use threads for crawling processes
    @TODO: Use threads for feeding processes
"""
import sys
import logging
from abc import ABCMeta
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

import sources
from cli import LoggingFormatter


class CrawlerFeederBase(object):
    """
    Abstract class that controls all the steps of the process

    @TODO: _data_parsed must be ambiguos and serve to many different objects like _data_crawled
    @TODO: The class is too much dependant of cli. It would be great to decouple it.
    """
    __metaclass__ = ABCMeta

    _args = None

    _data_sources = {}

    _data_crawled = {}

    _data_parsed = {}

    _warnings = []

    _errors = []

    def __init__(self, args):
        # Save the args locally
        self._args = args

        # Configure logging
        logging_handler = logging.StreamHandler(sys.stderr)
        logging_handler.setFormatter(LoggingFormatter())
        logging.root.addHandler(logging_handler)
        logging.root.setLevel(args.loglevel)

    def set_data_source(self, name, value):
        self._data_sources[name] = value

    def get_data_source(self, name):
        return self._data_sources[name]

    def get_data_source_classname(self, name):
        return "%s%s" % ("".join([x.capitalize() for x in name.split("_")]), "DataSource")

    def get_process_set(self):
        process_set = "crawlerfeeder.%s" % self._args.process_set \
            if self._args.process_set != 'main' else "crawlerfeeder"
        if not self._args.config.has_section(process_set):
            raise IndexError(
                "The process set '%s' does not exist. Include a section called '%s' in the configuration file." % (
                self._args.process_set, process_set))
        return process_set

    def get_crawlers(self):
        return self._args.config.get(self.get_process_set(), 'crawlers').split(',')

    def get_parsers(self):
        return self._args.config.get(self.get_process_set(), 'parsers').split(',')

    def get_feeders(self):
        return self._args.config.get(self.get_process_set(), 'feeders').split(',')

    def get_data_sources(self):
        return self._args.config.get(self.get_process_set(), 'sources').split(',')

    def set_data_crawled(self, key, value):
        self._data_crawled[key] = value

    def get_data_crawled(self, key):
        return self._data_crawled[key]

    def add_warning(self, message):
        self._warnings.append(message)
        logging.warning(message)

    def run(self):
        # Instantiate data sources
        logging.info("## Instantiate data sources...")
        self.instantiate()

        # Step I. Crawler
        logging.info("## [1/3. Crawler]: Crawling data from data sources...")
        self.crawl()

        # Step II. Parser
        logging.info("## [2/3. Parser]: Parsing data...")
        self.parse()

        # Step III. Feeder
        logging.info("## [3/3. Feeder]: Feeding sources with parsed data...")
        if self._args.dry_run:
            logging.info("### Skipping changes because executed in dry-run mode.")
        elif self._args.skip_unsafe:
            logging.info("### Skipping changes because executed in skip-unsafe mode and there are "
                         "warnings or errors registered during the process.")
        else:
            self.feed()

    def instantiate(self):
        """Instantiate all the data sources provided in the configuration file"""
        for data_source_name in self.get_data_sources():
            logging.info("### Instantiating '%s' service..." % data_source_name)
            data_source = getattr(sources, self.get_data_source_classname(data_source_name))
            self.set_data_source(data_source_name, data_source(**dict(self._args.config.items(data_source_name))))

    def crawl(self):
        """Crawls data from data sources"""
        for crawler in self.get_crawlers():
            try:
                service_name, method_name = crawler.split('.')
                logging.info("### Crawling '%s' from '%s'..." % (method_name, service_name))
            except ValueError:
                service_name = None
                method_name = crawler
                logging.info("### Crawling '%s'..." % method_name)
            method = getattr(self, "crawl_%s" % method_name)
            if service_name:
                service = self.get_data_source(service_name)
                self.set_data_crawled(method_name, service.crawl(**method(**dict(self._args.config.items(service_name)))))
            else:
                self.set_data_crawled(method_name, method())

    def parse(self):
        """Parses data from data sources"""
        for parser in self.get_parsers():
            logging.info("### Parsing '%s'..." % parser)
            method = getattr(self, "parse_%s" % parser)
            method()

    def feed(self):
        """Feeds data to data sources"""
        for feeder in self.get_feeders():
            try:
                service_name, method_name = feeder.split('.')
                logging.info("### Feeding '%s' from '%s'..." % (method_name, service_name))
            except ValueError:
                service_name = None
                method_name = feeder
                logging.info("### Feeding '%s'..." % method_name)
            method = getattr(self, "feed_%s" % method_name)
            if service_name:
                service = self.get_data_source(service_name)
                service.feed(**method(**dict(self._args.config.items(service_name))))
            else:
                method()
