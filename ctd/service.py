# -*- coding: utf-8 -*-

import ctd.elt as elt



def fetch_last_fileid_loaded():

    sql = \
    """
    select max(height)
    from itg.block
    """
    return elt.get_ro_connection().fetch_one(sql)

