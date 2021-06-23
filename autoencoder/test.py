"""
****************************************
 * @author: Chandler Qian
 * Date: 3/18/21
 * Project: the project this script belongs to
 * Purpose: the purpose of this script
 * Python version: python version
 * Project root: your project root
 * Environment name: your project environment package name
****************************************
"""
ratio_0 = row.speed_0
ratio_10 = row.speed_10
ratio_20 = row.speed_20
ratio_30 = row.speed_30
ratio_40 = row.speed_40
ratio_50 = row.speed_50
ratio_60 = row.speed_60
ratio_70 = row.speed_70
ratio_80 = row.speed_80
ratio_90 = row.speed_90
ratio_100 = row.speed_100
ratio_110 = row.speed_110

ratio_list = [ratio_0, ratio_10, ratio_20, ratio_30,
              ratio_40, ratio_50,
              ratio_60, ratio_70, ratio_80, ratio_90,
              ratio_100,
              ratio_110]

ratio_list.sort(reverse=True)

switcher = {
    ratio_0: 'Ratio_0',
    ratio_10: 'Ratio_10',
    ratio_20: 'Ratio_20',
    ratio_30: 'Ratio_30',
    ratio_40: 'Ratio_40',
    ratio_50: 'Ratio_50',
    ratio_60: 'Ratio_60',
    ratio_70: 'Ratio_70',
    ratio_80: 'Ratio_80',
    ratio_90: 'Ratio_90',
    ratio_100: 'Ratio_100',
    ratio_110: 'Ratio_110'
}

first = switcher.get(ratio_list[0])
second = switcher.get(ratio_list[1])
third = switcher.get(ratio_list[2])
fourth = switcher.get(ratio_list[3])

if arg == 1:
    return first
if arg == 2:
    return second
if arg == 3:
    return third
if arg == 4:
    return fourth
return 'null'

from scipy.stats.mstats import rankdata
import numpy as np
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
scaler.fit(pred_df.fillna(0))
scaled = scaler.transform(pred_df.fillna(0))

speed1 = np.histogram(scaled[:, 0], bins=np.array(range(-4, 20)))
speed2 = np.histogram(scaled[:, 1], bins=np.array(range(-4, 20)))
speed3 = np.histogram(scaled[:, 2], bins=np.array(range(-4, 20)))
import matplotlib.pyplot as plt
plt.hist(scaled[:, 2], bins = speed1[1] )
plt.show()

@_log
def _ranker(pred_df):
    """return the rank of the columns your provided

    Args:
        pred_df: dataframe with the columns you want to rank

    Return:
         the rank and targets dataframe
    """
    columns = pred_df.columns
    num_col = len(columns)
    index_dic = {}
    for i in range(len(columns)):
        index_dic[columns[i]] = i
    df_arr = np.array(pred_df)
    rank_arr = rankdata(df_arr, axis=1)
    rank_df = pd.DataFrame(data=(num_col + 1) - rank_arr, columns=columns)
    return rank_df


import matplotlib.pyplot as plt
plt.hist(z_score['speed_0'], bins=15)
plt.ylabel('counts')
plt.xlabel('zero speed Data');
plt.show()

import matplotlib.pyplot as plt
plt.hist(z_score['speed_90'], bins=35)
plt.ylabel('counts')
plt.xlabel('speed 90 Data');
plt.show()


test = {1: 'one', 2:'two', 3:'three'}

from autoencoder import fea_eng_enc

daily_df = fea_eng_enc.main(method = 'min_max', encoder_ratio = 0.01, save=True)
scaled_df['model_NPR___NPR_HD___NPR_XD']


class test:

    def __init__(self, a, b):
        self.a = a
        self.b = b

    def test(self):
        self.a+=1


class C:
    def __init__(self):
        self._x=None

    def g(self):
        return self._x

    def s(self, v):
        self._x = v

    def d(self):
        del self._x

    prop = property(g,s,d)

c = C()
c.x="a"
print(c.x)

def deco(func):

    def wrapper(*args):
        print('haha')

        return func(*args)
    return wrapper

@deco
def test(a):
    return a*2

def deco1(func):
    print('haha')
    return func

@deco1
def test2(b):
    return b*3

def myGeneratorList(n):
    for i in range(n):
        yield i