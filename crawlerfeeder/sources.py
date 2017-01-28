"""
Data sources
All the data sources must extend DataSource abstract class and define `crawl` and `feed` methods.
This methods are automatically called during the crawl and feed processes.
"""
import httplib2
from abc import ABCMeta, abstractmethod
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pymysql.cursors
from crawlerfeeder import logging


class DataSource(object):
    __metaclass__ = ABCMeta

    _service = None

    _data = {}

    @abstractmethod
    def crawl(self, **kwargs):
        pass

    @abstractmethod
    def feed(self, **kwargs):
        pass


class GoogleAnalyticsDataSource(DataSource):
    """
    Google Analytics V4 data source
    """
    _view_id = None

    def __init__(self, service_account_email, key_file_location, scopes, discovery_uri, view_id, **kwargs):
        credentials = ServiceAccountCredentials.from_p12_keyfile(
            service_account_email, key_file_location, scopes=scopes)
        http = credentials.authorize(httplib2.Http())
        self._service = build('analytics', 'v4', http=http, discoveryServiceUrl=discovery_uri)
        self._view_id = view_id

    def crawl(self, **kwargs):
        return self._service.reports().batchGet(body=kwargs['request']).execute()

    def feed(self, **kwargs):
        raise NotImplementedError("This method is not implemented yet.")


class MysqlDataSource(DataSource):
    """
    MySQL data source
    """
    def __init__(self, host, user, password, db, **kwargs):
        self._service = pymysql.connect(host, user, password, db, cursorclass=pymysql.cursors.DictCursor)

    def crawl(self, **kwargs):
        with self._service.cursor() as cursor:
            cursor.execute(**kwargs)
            logging.info("Affected rows: %s" % cursor.rowcount)
            return cursor.fetchall()

    def feed(self, **kwargs):
        with self._service.cursor() as cursor:
            if 'args' in kwargs:
                cursor.executemany(**kwargs)
            else:
                cursor.execute(**kwargs)
            logging.info("Affected rows: %s" % cursor.rowcount)
            return cursor
