#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import threading
import requests

import utils
from utils import get_module_logger


logger = get_module_logger(__name__)


class Scraper(threading.Thread):
    def __init__(self, url, result):
        super(Scraper, self).__init__()
        self.name = "thread-%s" % url
        self.url = url
        self.result = result

    def run(self):
        result = []
        try:
            s = requests.session()
            s.verify = utils.verify
            s.trust_env = utils.trust_env
            response = s.get(self.url, timeout=5)
        except Exception as e:
            logger.warning("Get {0} failed, error: {1}.".format(self.url, str(e)))
        else:
            if response.status_code != requests.codes.ok:
                logger.warning("Get {0} failed, response code is: {1}.".format(self.url, response.status_code))
            else:
                rlt = response.json()
                if rlt and "beans" in rlt:
                    result = rlt['beans']
                else:
                    logger.warning("No metrics get in the {0}.".format(self.url))
            s.close()
            if len(result) > 0:
                self.result.append(result)


class ScrapeMetrics(object):
    def __init__(self, urls):
        self.urls = urls

    def scrape(self):
        result = []
        tasks = [Scraper(url, result) for url in self.urls]
        for task in tasks:
            task.start()
        for task in tasks:
            task.join()
        return result
