#!/bin/python3

import os
import sys
from azure.cosmosdb.table.tableservice import TableService
from puppetconfig_functions import *

# Check that required environment variables are set

try:  
   os.environ["PUPPETCONFIG_SA_ACCOUNT_NAME"]
except KeyError: 
   print("Please set the environment variable PUPPETCONFIG_SA_ACCOUNT_NAME")
   sys.exit(1)

try:  
   os.environ["PUPPETCONFIG_TABLE_NAME"]
except KeyError: 
   print("Please set the environment variable PUPPETCONFIG_TABLE_NAME")
   sys.exit(1)
   
try:  
   os.environ["PUPPETCONFIG_SAS_TOKEN"]
except KeyError: 
   print("Please set the environment variable PUPPETCONFIG_SAS_TOKEN")
   sys.exit(1)

# Initialise Variables
sa_account_name = os.environ.get('PUPPETCONFIG_SA_ACCOUNT_NAME')
table_name = os.environ.get('PUPPETCONFIG_TABLE_NAME')
sas_token = os.environ.get('PUPPETCONFIG_SAS_TOKEN')
table_service = TableService(account_name=sa_account_name, sas_token=sas_token)

def main():

    # Perform function here
    print("this is main")
    do_list(table_service, table_name)

    # Done
    sys.exit(0)

if __name__ == '__main__':
	main()
