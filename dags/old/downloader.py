import requests
import datetime

CC_URL = "https://min-api.cryptocompare.com/data/"
MY_APP = "experiment(dataPFranc)"

def validate_time(t):
    if not (t.microsecond == t.second == t.minute == 0):
        raise ValueError("Exact hourly datetime expected")


def fetch_hourly_data(pair, from_dt, to_dt):
    """fetch hour pair (ex. EUR/USD) trade data from from_dt to to_dt (exclusive)
    """
    validate_time(from_dt)
    validate_time(to_dt)
    # substract one hour from t_dt (exclusive)
    to_dt = to_dt - datetime.timedelta(seconds=3600)
    
    # the exchange to obtain data (CCCAG is their aggregted data is default e = "CCCAGG")
    # number of data point to return data from
    limit = int ((to_dt - from_dt).total_seconds() / 3600)
    # last timestamp to return data for (unix epoch)
    to_ts = to_dt.timestamp()
    
    m = pair.index('/')
    # print("Load data from {} to {}".format(datetime.datetime.fromtimestamp(to_ts-limit*3600),datetime.datetime.fromtimestamp(to_ts)) )
    url = CC_URL + "histohour" 
    ret = http_get_data(url, as_json=True, fsym=pair[:m], tsym=pair[m+1:], limit=limit, toTs=to_ts, extraParams=MY_APP)
    return ret


       
    
    
def http_get_data(url, as_json, **kvargs):
    """ Generic HTTP GET call with params kvargs converted to well formed query string
    Return data in json or text
    """   
    print(kvargs)
    try:     
        r = requests.get(url, params=kvargs)
        # print(r.url)
        if as_json:
            return r.json()
        else:
            return r.text
    except requests.exceptions.RequestException as e:
        print(e)

        
        
