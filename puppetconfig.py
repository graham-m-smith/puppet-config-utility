#!/bin/python3

import os
import sys
import argparse
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

    valid_actions = ['list-machines', 'show-machine']
    parser = argparse.ArgumentParser()

    parser.add_argument('action', help='action to perform', type=str, choices=valid_actions)

    args = parser.parse_args()

    print(args.action)

    # Perform function here
    
    #do_list(table_service, table_name)
    #do_show_machine(table_service, table_name, 'puppetserver')
    #do_set_fact(table_service, table_name, 'puppetserver', 'fact1', 'value2')
    #do_delete_fact(table_service, table_name, 'puppetserver', 'fact1')
    #do_add_machine
    #do_delete_machine

    # Done
    sys.exit(0)

if __name__ == '__main__':
	main()
