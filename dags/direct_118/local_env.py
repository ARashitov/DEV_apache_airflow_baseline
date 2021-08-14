"""
    Author: Adil Rashitov (adil.rashitov.98@gmail.com)
    Created at: 14.08.2021

    About: Given file contains environment variables
           required for direct 118.
"""
import os


POSTGRES_URI = os.environ['AIRFLOW_CONN_POSTGRES_CONTACT_DETAILS']

STAGE_TABLE_1 = os.environ['I_DIRECT_118_STAGE_TABLE_1']
STAGE_TABLE_2 = os.environ['I_DIRECT_118_STAGE_TABLE_2']
STAGE_TABLE_3 = os.environ['I_DIRECT_118_STAGE_TABLE_3']
STAGE_TABLE_4 = os.environ['I_DIRECT_118_STAGE_TABLE_4']
OUTPUT_TABLE = os.environ['I_DIRECT_118_OUTPUT_TABLE']

SEARCH_ENDPOINT = os.environ['AIRFLOW_CONN_WWW_DIRECT_118']
