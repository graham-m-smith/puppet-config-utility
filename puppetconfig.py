#!/bin/python3

import os
import sys
import argparse
import yaml
from azure.cosmosdb.table.tableservice import TableService
from puppetconfig_functions import *

# Check that required environment variables are set

#try:  
#   os.environ["PUPPETCONFIG_SA_ACCOUNT_NAME"]
#except KeyError: 
#   print("Please set the environment variable PUPPETCONFIG_SA_ACCOUNT_NAME")
#   sys.exit(1)

#try:  
#   os.environ["PUPPETCONFIG_TABLE_NAME"]
#except KeyError: 
#   print("Please set the environment variable PUPPETCONFIG_TABLE_NAME")
#   sys.exit(1)
   
#try:  
#   os.environ["PUPPETCONFIG_SAS_TOKEN"]
#except KeyError: 
#   print("Please set the environment variable PUPPETCONFIG_SAS_TOKEN")
#   sys.exit(1)

# Initialise Variables
#table_name = os.environ.get('PUPPETCONFIG_TABLE_NAME')
#sa_account_name = os.environ.get('PUPPETCONFIG_SA_ACCOUNT_NAME')
#sas_token = os.environ.get('PUPPETCONFIG_SAS_TOKEN')
#table_service = TableService(account_name=sa_account_name, sas_token=sas_token)

def main():

    # Parse command line options

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='commands')

    # list-machines command
    list_machines_parser = subparsers.add_parser('list-machines', help='List Machines')
    list_machines_parser.set_defaults(command_type='list-machines')

    # show-machines command
    show_machine_parser = subparsers.add_parser('show-machine', help='Show Machine Detail')
    show_machine_parser.set_defaults(command_type='show-machine')
    show_machine_parser.add_argument('machine', action='store', help='Machine Name')

    # set-fact command
    set_fact_parser = subparsers.add_parser('set-fact', help='Set fact value for a machine')
    set_fact_parser.set_defaults(command_type='set-fact')
    set_fact_parser.add_argument('machine', action='store', help='Machine Name')
    set_fact_parser.add_argument('fact', action='store', help='Fact Name')
    set_fact_parser.add_argument('value', action='store', help='Fact Value')

    # delete-fact command
    delete_fact_parser = subparsers.add_parser('delete-fact', help='Delete fact for a machine')
    delete_fact_parser.set_defaults(command_type='delete-fact')
    delete_fact_parser.add_argument('machine', action='store', help='Machine Name')
    delete_fact_parser.add_argument('fact', action='store', help='Fact Name')

    args = parser.parse_args()

    config_file = '/etc/puppetconfig.yml'
    with open(config_file, "r") as configyml:
        cfg = yaml.load(configyml)

    sa_account_name = cfg['puppetconfig']['sa_account_name']
    table_name = cfg['puppetconfig']['table_name']
    sas_token = cfg['puppetconfig']['sas_token']
    table_service = TableService(account_name=sa_account_name, sas_token=sas_token)

    print("sa_account_name:", sa_account_name)
    print("table_name:", table_name)
    print("sas_token:", sas_token)

    # Perform function here

    if args.command_type == 'list-machines':
        do_list(table_service, table_name)

    elif args.command_type == 'show-machine':
        do_show_machine(table_service, table_name, args.machine)

    elif args.command_type == 'set-fact':
        do_set_fact(table_service, table_name, args.machine, args.fact, args.value)

    elif args.command_type == 'delete-fact':
        do_delete_fact(table_service, table_name, args.machine, args.fact)
    
    #do_add_machine
    #do_delete_machine
    #do_generate_yaml

    # Done
    sys.exit(0)

if __name__ == '__main__':
	main()
