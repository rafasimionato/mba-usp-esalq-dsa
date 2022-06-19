"""
This file is part of the conclusion work of my MBA studies on Data Science and
Analytics. Intention of this is to automate download of structured data from CVM
web site.

It produces a dataframe filtered on field 'CNPJ_FUNDO' when it is equals to
'36.178.569/0001-99' (which matches to 'BOLSA AMERICANA'), limited by the experiment
date ('2022-05-20 00:00:00') and also indexed by date.

@author: Rafael Simionato
"""

import os
import os.path
from os.path import isfile, join
from os import listdir

import shutil
from urllib import request
import zipfile

import pandas as pd

import matplotlib.pyplot as plt

import pickle

raw_data = './data/raw/'
raw_data_unzipped = './data/raw/unzipped/'

remote_url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'

# To donwload any other specific file and use it in the analysis, add its name
# to the following list. Please attention to the files organization in the remote
# server as it may change overtime.
files_zipped = [
    'HIST/inf_diario_fi_2020.zip',
    # 2021
    'inf_diario_fi_202101.zip', 'inf_diario_fi_202102.zip', 'inf_diario_fi_202103.zip',
    'inf_diario_fi_202104.zip', 'inf_diario_fi_202105.zip', 'inf_diario_fi_202106.zip',
    'inf_diario_fi_202107.zip', 'inf_diario_fi_202108.zip', 'inf_diario_fi_202109.zip',
    'inf_diario_fi_202110.zip', 'inf_diario_fi_202111.zip', 'inf_diario_fi_202112.zip',
    # 2022
    'inf_diario_fi_202201.zip', 'inf_diario_fi_202202.zip', 'inf_diario_fi_202203.zip',
    'inf_diario_fi_202204.zip', 'inf_diario_fi_202205.zip'
]

try:
    print ("Removing unzipped folder files.")
    shutil.rmtree(raw_data_unzipped)
except OSError as e:
    print("Error: %s - %s." % (e.filename, e.strerror))

for file in files_zipped:

    # First, we may check if we have saved the file locally
    if os.path.isfile(raw_data + file.removeprefix("HIST/")):
        print ("File " + file.removeprefix("HIST/") + " already exists locally.")
    else:
        # We do not have it. So download remote file and save locally
        print ("File " + file.removeprefix("HIST/") + " does not exist locally. Downloading ...")
        request.urlretrieve(remote_url + file, raw_data + file.removeprefix("HIST/"))

    print ("Unziping file " + file.removeprefix("HIST/") + " from the raw ...")
    with zipfile.ZipFile(raw_data + file.removeprefix("HIST/"), 'r') as zip_ref:
        zip_ref.extractall(raw_data_unzipped)

df_cvm_filtered = pd.DataFrame()

onlyfiles = [f for f in listdir(raw_data_unzipped) \
                 if isfile(join(raw_data_unzipped, f))]

for file in onlyfiles:
     print ("Processing file " + file + " ...")
     fname = os.path.join(raw_data_unzipped, file)
     df_aux = pd.read_csv(fname, sep=';')
     df_aux = df_aux[df_aux['CNPJ_FUNDO'] == '36.178.569/0001-99'] # BOLSA AMERICANA
     df_cvm_filtered = pd.concat([df_cvm_filtered, df_aux])

del df_aux

df_cvm_filtered = df_cvm_filtered.reset_index(drop=True).drop(index=[0, 1])
df_cvm_filtered['DT_COMPTC'] = pd.to_datetime(df_cvm_filtered['DT_COMPTC'])

# Defining the limit date for the experiments
df_cvm_filtered = df_cvm_filtered[df_cvm_filtered['DT_COMPTC'] <= '2022-05-20 00:00:00']

df_cvm_filtered = df_cvm_filtered.sort_values(by='DT_COMPTC').reset_index(drop=True)
df_cvm_filtered['DT_COMPTC_YMD'] = df_cvm_filtered['DT_COMPTC'].dt.strftime('%Y%m%d')
df_cvm_filtered.drop(['CNPJ_FUNDO', 'TP_FUNDO'], inplace=True, axis=1)
df_cvm_filtered.set_index("DT_COMPTC", inplace = True)

# Ensure setting VL_QUOTA as the first one column in the dataframe
cols = ['VL_QUOTA', 'VL_TOTAL', 'VL_PATRIM_LIQ', 'CAPTC_DIA', 'RESG_DIA', 'NR_COTST', 'DT_COMPTC_YMD']
df_cvm_filtered = df_cvm_filtered[cols]

print(df_cvm_filtered.head())
print(df_cvm_filtered.shape)
print(df_cvm_filtered.dtypes)

plt.figure(num = 0, figsize=(9, 5))
plt.title('Evolução do valor da quota')
plt.plot(df_cvm_filtered[['VL_QUOTA']])

# Saving dataframe to a pickle file
output_file_path = './data/filtered_dataset/df_filtered_cvm.pkl'
os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
with open(output_file_path, 'wb') as f:
    pickle.dump(df_cvm_filtered, f)