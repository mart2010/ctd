
import os
import numpy as np
import matplotlib.pyplot as plt

# header : ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
eur_usd_file = 'data_static/EURUSD_15m_BID_01.01.2010-31.12.2016.csv'

def load_fx_data():
    file_in = open(eur_usd_file)
    # ignore line with volume=0 (rates do not change)
    lines = [ line for line in file_in if line[-3:] != ',0\n' ]
    file_in.close()
    # skip header line and empty last line
    lines = lines[1:-1]
    float_data = np.zeros((len(lines), len(lines[0].split(','))-1))

    for i, line in enumerate(lines):
        # skip the timestmp (rely on sequential index) and
        values = [ float(x) for x in line.split(',')[1:]]
        float_data[i, :] = values
    return  float_data

# data as np array
fx_data = load_fx_data()


def show_value_fx(metric):
    rate_o  = fx_data[:,metric]
    plt.plot(range(len(rate_o)),rate_o)
    plt.show()

# to show all numpy array
# np.set_printoptions(threshold=100000, linewidth=100)
