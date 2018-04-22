# -*- coding: utf-8 -*-

import glob
import json
import shutil
import logging
import ctd.config as config
import ctd.elt as elt
import psycopg2
from psycopg2.extensions import AsIs
import datetime

import luigi
import os
import requests

from ctd.taskbase import BaseBulkLoadTask, BasePostgresTask, batch_name
from blockchain_parser.blockchain import Blockchain



########################################################################################
##### for loading exchange rate data into ctd DB 
########################################################################################

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
        
        

        
        
########################################################################################
##### for loading BlockChain raw data into DB (for now using btc)
########################################################################################

# on pourrait inclure le paramètre "nb_blk_files" seulement à ce niveau et lancer la tache root avec ce param pour cette Tache.. voir guide
class GetBLKFilesToLoad(luigi.Task):
    run_dts = luigi.DateMinuteParameter()
    nb_blk_files = luigi.IntParameter()
    
    # TODO: check if ok with path instead of file, check up-to which blk files ok to load (4-5 blocks to wait for confirmation)
    def output(self):
        output_path = '/tmp/blk_files_%s' % self.run_dts.strftime(luigi.DateMinuteParameter.date_format)
        return luigi.LocalTarget(output_path)

    def run(self):
        sql = "select max(height) from itg.block"
        n = elt.get_ro_connection().fetch_one(sql)
        if not n:
            next_fileid = 1
        else:
            next_fileid =  n + 1

        all_files = sorted(glob.glob(config.BLOCKCHAIN_DIR + 'blk*.dat'))
        files_toload = all_files[next_fileid:next_fileid+self.nb_blk_files] if self.nb_blk_files else all_files[next_fileid:]

        # TODO raise error if no blk files available to process
        # if ....:
        #    raise ctd.WorkflowError("No more blk files available, STOP PROCESSING!")
        
        for f in files_toload:
            shutil.copy2(f,self.output())
           

class LoadBLKFilesToStgBase(BasePostgresTask):
    run_dts = luigi.DateMinuteParameter()
    nb_blk_files = luigi.IntParameter()
    
    def __init__(self, *args, **kwargs):
        super(LoadBLKFilesToStgBase, self).__init__(*args, **kwargs)
        self.blockchain = Blockchain(self.input())
    
    def requires(self):
        return GetBLKFilesToLoad(self.run_dts, self.nb_blk_files)

    def execute_sql(self, cursor, audit_id):
        insert_sql = "insert into {}(%s) values %s".format(self.table)
        
        row_insert = {}
        row_insert['load_audit_id'] = audit_id
        row_insert['loading_dts'] = self.run_dts
        rowcount = 0
        for row in self.generate_rows(row_insert):
            cols = row.keys()
            vals = [row[c] for c in cols]
            # cursor.execute(insert_sql, (AsIs(','.join(cols)),tuple(vals)))
            print("the mogrify" + cursor.mogrify(insert_sql, (AsIs(','.join(cols)),tuple(vals))))
            rowcount += 1
        return rowcount
        
    def generate_rows(self, row):
        raise NotImplementedError
        
            
class LoadBLKFilesToStgTx(LoadBLKFilesToStgBase):
    table = 'stg.blk_data_tx'
   
    def generate_rows(self, row):        
        for block in self.blockchain.get_unordered_blocks():
            row['blk_chain'] = 'btc'
            row['blk_hash'] = block.hash
            row['blk_size'] = block.size
            row['blk_height'] = block.height                    
            row['blk_nb_tx'] = block.n_transactions
            row['blk_version'] = block.header.version
            row['blk_prev_hash'] = block.header.previous_block_hash
            row['blk_utc_timestmp'] = block.header.timestamp
            row['blk_difficulty'] = block.header.difficulty            
            for trans in block.transactions:
                row['tx_version'] = trans.version
                row['tx_locktime'] = trans.locktime
                row['tx_is_coinbase'] = trans.is_coinbase
                row['tx_use_rbf'] = trans.use_replace_by_fee
                row['tx_hash'] = trans.hash
                row['tx_nb_inputs'] = trans.n_inputs
                row['tx_nb_outputs'] = trans.n_outputs                
                # index/pos of output is deducted (assumption: output in list outputs ordered correctly)
                out_pos = 1
                for txout in trans.outputs:
                    row['txout_pos'] = out_pos
                    row['txout_script_type'] = txout.type
                    row['txout_addresses_base58'] = [a.address for a in txout.addresses]
                    row['txout_publickeys'] = [a.publickey for a in txout.addresses]
                    row['txout_value'] = txout.value
                    out_pos += 1
                    yield row
        
    
class LoadBLKFilesToStgTxin(LoadBLKFilesToStgBase):
    table = 'stg.blk_data_txin'
    
    def generate_rows(self, row):
        for block in self.blockchain.get_unordered_blocks():
            row['blk_chain'] = 'btc'
            for trans in block.transactions:
                row['tx_hash'] = trans.hash
                row['tx_nb_inputs'] = trans.n_inputs
                for txin in trans.inputs:
                    row['txin_txout_hash'] = txin.transaction_hash
                    row['txin_txout_pos'] = txin.transaction_index
                    row['txin_pos'] = txin.sequence_number                
                    yield row

                    
class IntegrateBlockData(BasePostgresTask):
    run_dts = luigi.DateMinuteParameter()
    nb_blk_files = luigi.IntParameter()
    table = 'itg.block'
    
    def requires(self):
        return LoadBLKFilesToStgTx(self.run_dts, self.nb_blk_files)

    def execute_sql(self, cursor, audit_id):
        insert_sql = \
        """        
        insert into {}(hash, version, difficulty, utc_time, height, load_audit_id)
        select  distinct blk_hash
                , blk_version
                , blk_difficulty
                , blk_utc_timestamp
                , blk_height
                , %(audit_id)s
        from stg.blk_data_tx
        """.format(self.table)
        
        cursor.execute(insert_sql, {'audit_id': audit_id})
        return cursor.rowcount

        
class IntegrateTxData(BasePostgresTask):  
    run_dts = luigi.DateMinuteParameter()
    nb_blk_files = luigi.IntParameter()
    table = 'itg.tx'
    
    def requires(self):
        return LoadBLKFilesToStgTx(self.run_dts, self.nb_blk_files)

    def execute_sql(self, cursor, audit_id):
        insert_sql = \
        """        
        insert into {}(hash, block_id, version, locktime, is_coinbase, size, nb_inputs, nb_outputs, load_audit_id)
        select  distinct tx_hash
                , b.block_id
                , tx_version
                , tx_locktime
                , tx_is_coinbase
                , tx_size
                , tx_nb_inputs
                , tx_nb_outputs
                , %(audit_id)s
        from stg.blk_data_tx t
        join itg.block b on (b.hash = t.block_hash)
        """.format(self.table)
        
        cursor.execute(insert_sql, {'audit_id': audit_id})
        return cursor.rowcount

        
class IntegrateAddressData(BasePostgresTask):
    run_dts = luigi.DateMinuteParameter()
    nb_blk_files = luigi.IntParameter()
    table = 'itg.address'
    
    def requires(self):
        return IntegrateTxData(self.run_dts, self.nb_blk_files)

    def execute_sql(self, cursor, audit_id):
        insert_sql = \
        """        
        insert into {}(adress_base58, load_audit_id )
        select distinct
            , unnest(txout_addresses_base58) as adr
            --, unnest(txout_publickeys)
            , %(audit_id)s    
        from stg.blk_data_tx new
        where not exists (select 1 from itg.address a where a.address_base58 = new.adr ) 
        """.format(self.table)
        
        cursor.execute(insert_sql, {'audit_id': audit_id})
        return cursor.rowcount

        
class IntegrateTxOutData(BasePostgresTask):
    run_dts = luigi.DateMinuteParameter()
    nb_blk_files = luigi.IntParameter()
    table = 'itg.txout'
    
    def requires(self):
        return IntegrateTxData(self.run_dts, self.nb_blk_files)

    def execute_sql(self, cursor, audit_id):
        insert_sql = \
        """        
        insert into {}(tx_id, tx_pos, value, script_type, load_audit_id)
        select i.tx_id
            , txout_pos
            , txout_value
            , txout_script_type
            , %(audit_id)s
        from stg.blk_tx_data s
        join itg.tx i on (i.hash = s.tx_hash)
        """.format(self.table)
        
        cursor.execute(insert_sql, {'audit_id': audit_id})
        return cursor.rowcount

# RENDU LA..................       
class IntegrateTxOutAddressData(BasePostgresTask):
    run_dts = luigi.DateMinuteParameter()
    nb_blk_files = luigi.IntParameter()
    table = 'itg.txout_address'
    
    def requires(self):
        return [IntegrateTxOutData(self.run_dts, self.nb_blk_files), IntegrateAddressData(self.run_dts, self.nb_blk_files)]

    def execute_sql(self, cursor, audit_id):
        insert_sql = \
        """        
        insert into {}(txout_id, address_id, load_audit_id)
        select tx_adr.txout_id
                , adr.address_id 
                , %(audit_id)s
        from 
            (select  itxo.txout_id
                    , unnest(txout_addresses_base58) as adrs
            from stg.blk_tx_data s
            join itg.tx itx on (itx.hash = s.tx_hash)
            join itg.txout itxo on (itx.tx_id = itxo.tx_id and s.txout_pos = itxo.pos) as tx_adr
        join itg.address adr on (tx_adr.adrs = adr.address_base58)        
        """.format(self.table)
        
        cursor.execute(insert_sql, {'audit_id': audit_id})
        return cursor.rowcount

        
# may have issue if executed with unordered blocks as the txout may not be present (not sure as txout is executed before.,, to validate)
# if that's the case, should have an initial load where all blocks/tx/txout are loaded first
class IntegrateTxInData(BasePostgresTask):  
    run_dts = luigi.DateMinuteParameter()
    nb_blk_files = luigi.IntParameter()
    table = 'itg.txin'
    
    def requires(self):
        return IntegrateTxOutData(self.run_dts, self.nb_blk_files)

    def execute_sql(self, cursor, audit_id):
        insert_sql = \
        """        
        insert into {}(tx_id, tx_pos, txout_id, load_audit_id)
        select  i.tx_id
                , s.txin_pos
                , txo.txout_id
                , %(audit_id)s
        from stg.blk_data_txin s
        join itg.tx i on (i.hash = t.tx_hash)
        join (select t2.hash, t1.pos, t1.txout_id
              from itg.txout t1 join itg.tx t2 on (t1.tx_id = t2.tx_id) 
             ) as txo on (txo.hash = t.txin_txout_hash and txo.pos = t.txin_txout_pos)
        """.format(self.table)
        
        cursor.execute(insert_sql, {'audit_id': audit_id})
        return cursor.rowcount


class BatchIntegrateData(luigi.Task):  
    run_dts = luigi.DateMinuteParameter()
    nb_blk_files = luigi.IntParameter()

    global batch_name
    batch_name = "Integrate-BlockChainData"  # for auditing

    def requires(self):
        return [IntegrateTxOutAddressData(self.run_dts, self.nb_blk_files), IntegrateTxInData(self.run_dts, self.nb_blk_files)]

                
    
