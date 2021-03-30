# -*- coding: utf-8 -*-
import scrapy
from pydispatch import dispatcher
from scrapy import signals
from urlparse import urlparse, urljoin
import pandas as pd
import re
import json


class MainSpider(scrapy.Spider):
    concurrent = 128
    name = 'main'
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN': concurrent,
        'CONCURRENT_REQUESTS': concurrent,
        'CONCURRENT_REQUESTS_PER_IP': concurrent, }
    valid_proxies = None
    kwargs = {}

    def __init__(self, **kwargs):
        super(MainSpider, self).__init__(**kwargs)
        self.kwargs = kwargs
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        dispatcher.connect(self.spider_opened, signals.spider_opened)

    def spider_opened(self, spider):
        self.valid_proxies = pd.DataFrame(columns=['proxy', 'timeout'])

    def spider_closed(self, spider):
        self.valid_proxies.drop_duplicates(subset=['proxy']).to_csv('./proxies.csv', sep=';', index=False)

    def start_requests(self):
        yield scrapy.Request('https://free-proxy-list.net/', callback=self.parse, dont_filter=True)
        yield scrapy.Request('https://api.openproxy.space/list?skip=0&ts=1700000000000',
                             callback=self.parse2, dont_filter=True)

    def parse(self, response):
        textarea = response.xpath('//textarea[@class="form-control"]/text()').extract_first()
        nofiltered_proxies = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,6}', textarea)
        for i, p in enumerate(nofiltered_proxies):
            yield scrapy.Request(
                'https://www.google.com/',
                callback=self.procee,
                meta={'proxy': p, 'dont_redirect': True, 'handle_httpstatus_list': [200, 302, 301],
                      'dont_retry': True, 'download_timeout': 10},
                dont_filter=True, )

    def parse2(self, response):
        if not response.meta.get('follow'):
            json_data = json.loads(response.body)
            for i in range(3):
                yield scrapy.Request('https://openproxy.space/list/'+json_data[i]['code'],
                                     self.parse2, meta={'follow': True}, dont_filter=True)
            return
        json_data = re.search(r'(?<=data:\[).*(?=\])', response.body).group(0)
        nofiltered_proxies = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,6}', json_data)
        for i, p in enumerate(nofiltered_proxies):
            yield scrapy.Request(
                'https://www.google.com/',
                callback=self.procee,
                meta={'proxy': p, 'dont_redirect': True, 'handle_httpstatus_list': [200, 302, 301],
                      'dont_retry': True, 'download_timeout': 10},
                dont_filter=True, )

    def procee(self, response):
        proxy = re.search(r'\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{1,5}', response.meta.get('proxy')).group(0)
        download_latency = response.meta.get('download_latency')
        self.log('Got proxy: {0}, timeout: {1}sec'.format(proxy, download_latency))
        self.valid_proxies = self.valid_proxies.append(
            pd.Series({'proxy': proxy, 'timeout': download_latency}), ignore_index=True)
