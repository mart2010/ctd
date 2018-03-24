__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The CRT Project"

import datetime
import json
import logging

import ctd
import ctd.service as service
import luigi
import os
import requests

from crt.taskbase import BaseBulkLoadTask, BasePostgresTask, batch_name
from blockchain_parser.blockchain import Blockchain


logger = logging.getLogger(__name__)


class FetchExchangeData(luigi.Task):
    """Fetch data from min-api.cryptocompare.com/data/
    """
    from_dt = luigi.DateHourParameter()
    to_dt = luigi.DateHourParameter()
    
    def __init(self, *args, **kwargs):
        super(FetchExchangeData, self).__init__(*args, **kwargs)
        filename = "ExchangeHourCC_%s_%s.csv" % (self.from_dt.strftime(luigi.DateHourParameter.date_format,
                                                 self.to_dt.strftime(luigi.DateHourParameter.date_format)))
        self.dump_filepath = os.path.join(ctd.config.DATA_DOWNLOADED_DIR, filename)
        
    def output(self):
        return luigi.LocalTarget(self.dump_filepath)

    
    def run(self):
        # use the downlader code... to get json
        
        # dump the json into csv format...
        
        
        


class FetchNewBlockfiles(luigi.Task):
    blockfiles_path = "/path/to/block"    # luigi.Parameter()
    
    def output(self):
        return luigi.LocalTarget(self.blockfiles_path)

   
insert_block = \
"""
insert into block(hash, version, coinbase, price, size, difficulty, block_time, height, prev_hash_id)
values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""




class LoadBlockDataIntoStaging(BasePostgresTask):
    # blockfiles_path = luigi.Parameter()
            
    def requires(self):
        return FetchNewBlockfiles()
    
    def run(self):
        bc = Blockchain(self.input())
        for block in bc.get_unordered_blocks():
            row_tuple = (block.hash, block.header.version, 'coinbase', -1000, block.header.difficulty, 
                         block.header.timestamp, )
            vals = cursor.mogrify("%s", (x, )) for x in 
        
        

    def output(self):
        pass




