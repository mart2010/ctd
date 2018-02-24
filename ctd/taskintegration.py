__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The CRT Project"

import datetime
import json
import logging

import crt
import crt.service as service
import luigi
import os
from crt.taskbase import BaseBulkLoadTask, BasePostgresTask, batch_name
from blockchain_parser.blockchain import Blockchain


logger = logging.getLogger(__name__)



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




