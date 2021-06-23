"""
****************************************
 * @author: Chandler Qian
 * Date: 3/9/21
 * Project: safety clustering
 * Purpose: feature engineering for autoencoders
 * Python version: 3.8.1
 * Project root: /home/usr/projects/safety-recommendation
 * Environment package: safety_rec on the remote
 * Copyright 2021 Geotab DNA. All Rights Reserved.
****************************************
"""

import pandas as pd
import numpy as np
import google.cloud.bigquery as bigquery
import sys, os
import yaml
from scipy.stats.mstats import rankdata
import re

with open("./autoencoder/config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

sys.path.insert(1, cfg['directory'][
    'root'])  # add root to the path so script can be called by cronjob
from src.query import query_fea_dict, query_imputed_fea
from src import util

"""
****************************************
Scaling methods for all the features. method 1 and 2 are applied to the features with 
scale_required = 1 within fea_dict.
        method 1 : min max: cons - the distribution is off, poor at handling anomaly. pros - ez
        method 2 : z-score: cons - more complex. pros - zero mean, good at handling anomaly.
        method 3 : keep the raw data, if the raw speed data at similar scale within 0-1.
****************************************
"""


class PreCls:
    """Preprocessing class for the project
    class for preprocess which has multiple preprocessing methods

    Attributes:
        fea_dict: the feature dictionary
        daily_df: dataframe for preprocessing
    """

    def __init__(self, fea_dict, daily_df):
        """ preprocessing class constructor for init
        """
        fea_dict = fea_dict[fea_dict['fea_status'] == 1]
        self.fea_list = fea_dict['fea_name']
        self.num_fea = fea_dict[fea_dict['fea_category'] == 'numerical']
        self.cat_fea = fea_dict[fea_dict['fea_category'] == 'categorical']
        self.id_fea = fea_dict[fea_dict['fea_category'] == 'id']
        daily_df.columns = daily_df.columns.str.lower()
        self.daily_df = self._fill_na(fea_dict, daily_df)
        self.length = len(daily_df)

    @staticmethod
    @util.logger_log(filepath=cfg['directory']['autoencoder_log'])
    def _fill_na(fea_dict, daily_df):
        """ pre processing high level function which preprocess the raw data before encoding and scaling
            please make sure the right imputation method is provided in the feature dictionary

        Args:
            fea_dict: feature dictionary dataframe
            daily_df: daily dataframe for pre processing

        Returns:
            preprocessed dataframe based on the scaling method
        """
        daily_df.columns = daily_df.columns.str.lower()
        zero_fil_fea = fea_dict[fea_dict['fillna_method'] == '0'][
            'fea_name']  # string 0
        for feature in zero_fil_fea:
            daily_df[feature].fillna(0, inplace=True)
        return daily_df

    @util.logger_log(filepath=cfg['directory']['autoencoder_log'])
    def _encoder(self, encoder_ratio):
        """ customized one hot encoding

        Args:
            ratio: frequency ratio threshold to keep the categorical features

        Returns:
            dataframe after encoding
        """
        cat_df = self.daily_df[self.cat_fea.fea_name].copy()
        cat_df = cat_df.applymap(
            str)  # convert whole cat_df as str in case of int values as categories
        cat_df = cat_df.applymap(lambda s: s.lower())  # lowercase all values
        for fea in self.cat_fea.fea_name:
            cat_df.loc[cat_df[fea].str.contains(
                'other'), fea] = 'other'  # replace #other, &other with other
            ratio_list = cat_df[fea].value_counts() < (
                encoder_ratio * self.length)
            cat_df[fea].replace(ratio_list.index[ratio_list], 'other',
                                inplace=True)  # list of categories below threshold
        return pd.get_dummies(cat_df)  # return the encoded cat feature df

    @util.logger_log(filepath=cfg['directory']['autoencoder_log'])
    def _min_max(self):
        """ return min max scaling based transformation

        Returns:
            min max transformed dataframe
        """
        from sklearn.preprocessing import MinMaxScaler
        scaler = MinMaxScaler()
        num_fea_name = self.num_fea[self.num_fea[
                                        'scale_required'] == 1].fea_name  # numerical features which required scale's name
        scaler.fit(self.daily_df[num_fea_name])
        return pd.DataFrame(
            data=scaler.transform(self.daily_df[num_fea_name]),
            columns=num_fea_name
        )

    @staticmethod
    @util.logger_log(filepath=cfg['directory']['autoencoder_log'])
    def _ranker(fea_df):
        """ function to return the min_max normalized rank of the columns your provided
            the function is not utilized based on our investigation on z-score + rank

        Args:
            fea_df: dataframe with the columns you want to rank

        Return:
             the rank and targets dataframe
        """
        columns = fea_df.columns
        num_col = len(columns)
        max_min = num_col - 1  # the range of value
        index_dic = {}
        for i in range(len(columns)):
            index_dic[columns[i]] = i
        df_arr = np.array(fea_df)
        rank_arr = rankdata(df_arr, axis=1)
        rank_df = pd.DataFrame(data=(num_col + 1) - rank_arr
                               , columns=columns)
        # rank_df = pd.DataFrame(data=(num_col - rank_arr)/max_min
        #                         , columns=columns)
        return rank_df

    @util.logger_log(filepath=cfg['directory']['autoencoder_log'])
    def _zscal_ranker(self):
        """ z score normalization with ranker padding.

        Args:
            rank: the list of features to rank across a single customer

        Returns:
            z normalization transformed dataset with the rank option
        """
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        num_fea_name = self.num_fea[self.num_fea[
                                        'scale_required'] == 1].fea_name  # numerical features which required scale's name
        scaler.fit(self.daily_df[num_fea_name])
        return pd.DataFrame(
            data=scaler.transform(self.daily_df[num_fea_name]),
            columns=num_fea_name
        )

    @staticmethod
    @util.logger_log(filepath=cfg['directory']['autoencoder_log'])
    def _rm_dup_col(df):
        """helper function to combine the duplicate column into 1 with sum()

        Args:
            df: the dataframe to check

        Returns:
            the dataframe without dup columns
        """
        cols = df.columns.to_list()
        # get the columns which have same name
        dup_cols = set([x for x in cols if cols.count(x) > 1])
        for col in dup_cols:
            # use sum since it's caused by typo of the categories
            df[col + '_agg'] = df[col].groupby(level=0, axis=1).sum()
            df.drop(col, axis=1, inplace=True)
        return df

    @util.logger_log(filepath=cfg['directory']['autoencoder_log'])
    def _pipeline(self, scale_method, encoder_ratio):
        """high level pipeline function to call the other functions and combine
           This function utilizes the other functions within the class and use
           regex to force the column name contain only alphabets, number, and '_'.

        Args:
            scale_method: the scaling method # 'min_max', 'z-score'

        Returns:
            concatenated dataframe
        """
        scale_methods = {
            'min_max': self._min_max
            , 'zscal_ranker': self._zscal_ranker
        }
        id_df = self.daily_df[self.id_fea.fea_name]
        cat_df = self._encoder(encoder_ratio)
        num_df_scale = scale_methods.get(scale_method)()
        num_df_no_scale = self.daily_df[
            self.num_fea[self.num_fea['scale_required'] == 0].fea_name]
        final_df = pd.concat([id_df, cat_df, num_df_scale, num_df_no_scale],
                             axis=1)
        # regex to replace [, ], <, (, ), -, $space, +, #, /, \, ', " with _
        # there is a slim chance this step generates duplicate columns example:
        # 'NPR / NPR HD / NPR-XD', and 'NPR / NPR-HD / NPR-XD', only happens during encoding
        regex = re.compile(r"\[|]|<|>|\(|\)|-|\s|\+|#|/|\\|\'|\"",
                           re.IGNORECASE)
        final_df.columns = [regex.sub("_", col) if any(
            x in str(col) for x in
            {'[', ']', '<', '>', '(', ')', '-', ' ', '+', '#', '/', '\\', '\'',
             '\"'}) else col for col in final_df.columns.values]
        # remove duplicate columns sum() which rarely happens
        final_df = self._rm_dup_col(final_df)
        return final_df


def main(method, encoder_ratio, date, save=False, bq_load=False):
    """ The main function to trigger the entire process
        steps to take:
            1. fillna based on the feature dictionary using customized function
            2. scaling based on the input using the PreCLs
            3. encoding based input ratio using the PreCLs

    Args
        method: the method we are applying for preprocessing
        encoder_ratio: the ratio for one-hot encoding
        date: the _partitiontime for query. exg: '2021-02-01'
        save: save on local or not
        bq_load: save to bq sharded table or not

    Returns
        preprocessed dataframe
    """
    client = bigquery.Client(project='geotab-bi')
    daily_df = util.date_query(query=query_imputed_fea, date=date, client = client)
    fea_dict = client.query(
        query_fea_dict).to_dataframe()  # make sure the fea_dict is up to date
    """
     create preprocessing object which can be reused multiple time
     1. Imputation step is completed during the class constructor
        replace null with 0, since mostly are missing because they should be 0
        use the static method within the preprocessing method
    """
    pre_pro = PreCls(fea_dict, daily_df)
    """
    normalization methods:
    method 1 : min max scaling only, method = 'min_max'
    method 2 : z-score,  method = 'zscal_ranker'
    """
    scaled_df = pre_pro._pipeline(scale_method=method
                                  , encoder_ratio=encoder_ratio)
    """
    add index columns for encoding method as well as encoder ratio, time stamps
    """
    scaled_df['scale_method'] = method
    scaled_df['encoder_ratio'] = encoder_ratio
    scaled_df['fea_eng_date'] = pd.Timestamp('today')
    # if save is True, we are going to save the preprocessed data into
    # directory = cfg['directory']['raw_data_dir']
    if save:
        if not os.path.exists(cfg['directory']['raw_data_dir']):
            os.mkdir(cfg['directory']['raw_data_dir'])

        scaled_df.to_csv(cfg['directory']['raw_data_dir'] + method + '_' + str(
            encoder_ratio) + '.csv'
                         , index=False)
    # if bq_load is True, we are going to append data into
    # table = cfg['tables']['fea_eng'] with method, encoder ratio and date.
    if bq_load:
        table_dest = cfg['fea_eng_tables']['fea_eng'] + '_' + method + '_' \
                     + str(encoder_ratio).replace('.', '_') + '_' \
                     + date.replace('-', '')  # end with "_20210514 for sharded table"
        job = client.load_table_from_dataframe(scaled_df, table_dest)
        job.result()
    return scaled_df


if __name__ == '__main__':
    """
    The only time this script is executed as main is when we need to save the 
    preprocessed data. Therefore save = True.
    Examples code to run in cronjob: autoencoder/fea_eng_enc.py "'min_max'" 0.05
    """
    method = sys.argv[1]  # first argument after the script
    encoder_ratio = float(sys.argv[2])  # second argument, convert to numerical
    date = sys.argv[3]
    sys.exit(main(method=method,
                  encoder_ratio=encoder_ratio,
                  date=date,
                  save=True,
                  bq_load=True))
