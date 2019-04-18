# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 11:08:17 2018

@author: d7loz9
"""

import hashlib
import bit

def sha256_fromfile(infile):
    hash_o = hashlib.sha256(open(infile,'rb').read())
    return hash_o.hexdigest()


def sha256(text_in, from_iter=0, to_iter=1):
    """Generate hashing of text_in at different iteration.
    return generator of tuple(hexdigest, iter_no)
    """
    t_in = text_in
    for i in range(from_iter, to_iter):
        hash_o = hashlib.sha256(t_in.encode('utf-8'))
        tuple_o = (hash_o.hexdigest(), i)
        t_in = tuple_o[0]
        yield tuple_o
    

def derivekey(sha256_in, compressed=True):
    """ From a sha256 digest code (in hex), derive key having either public key encoded in Base58CheckEncoding 
    WIF-compressed (result in prefix 'K' or 'L') or WIF uncompressed (result in prefix '5').
    Return --> bit.Key obj
    """
    key = bit.Key.from_hex(sha256_in)
    if not compressed:
        wif_uncompressed = bit.format.bytes_to_wif(key.to_bytes(), version= 'main', compressed=False)
        key = bit.Key(wif_uncompressed)
    return key

# TODO: decide how to do for the 2 different public address (could simply call it with the 2-version privatekey)
def trx_and_unspent(key):
    """ Call service API to fetch trx-type info. 
    Return number transactions, balance unspent in USD (as tuple) associated with the key
    """
    nb_trx = len(key.get_transactions())
    unspent = key.get_balance(currency='usd')
    return (nb_trx, unspent)
    
  
    
    




# import pybitcoin as btc

base58_alphabet = set('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz')
max_privatekey = 1.158e77  


def construct_privatekey(privatekey_char, compressed=False):
    """ Validate and instantiate a BitcoinPrivateKey object
    
        Input 256bit key encoding supported:
            1. Hexadecimal having 64-hex digits w or w/o '0x' prefix (ex. '0x432AF43E4327...')
            2. Base58 WIF with 51 digits and prefix '5' (Base58Check encoding)
                (i.e. private key from which uncompressed public keys are derived)
            3. Base58 WIF-compressed with 52 digits and prefix 'K' or 'L' (Base58Check using ox01 suffix before encoding)
                (i.e. private key from which compressed public keys are derived, typical of newer wallet)
                (last 2 formats must not be used interchangeably)
    """
    def chars_in_base58(input_chars):
        dif = set(input_chars).difference(base58_alphabet)
        return dif == 0
    # is hexadecimal
    if len(privatekey_char) in (64,66):
        assert int(privatekey_char, base=16) > 0
    # is Base58
    elif len(privatekey_char) in (51,52) and privatekey_char[0:1] in ('5','K','L'):
        assert chars_in_base58(privatekey_char)
        # only suitable for uncompressed publickey
        if privatekey_char[0:1] == '5':
            assert len(privatekey_char) == 51
            if compressed:
                raise ValueError("'{}' as in Base58 WIF uncompresed form, cannot instantiate with compress=True".format(privatekey_char))
        else:
            assert len(privatekey_char) == 52
            if not compressed:
                raise ValueError("'{}' as in Base58 WIF-compresed form, cannot instantiate with compress=False".format(privatekey_char))            
    else:
        raise ValueError("Input argument '{}' has unrecognized format".format(privatekey_char))
    
    # assert 0 < int(privatekey_char) < max_privatekey
    # return btc.BitcoinPrivateKey(privatekey_char, compressed=compressed)
    return 0
        
      
    

    