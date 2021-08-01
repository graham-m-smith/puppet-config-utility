#!/bin/python3

# To Do:
# move config file to function
# lock file for generate
# proxy setting?
# logging
# debugging

import os
import sys
import argparse
import yaml
from azure.data.tables import TableServiceClient
from azure.core.credentials import AzureSasCredential
from puppetconfig_functions import *
from puppetconfig_generate import *

def main():

    # Parse command line options

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='Available Commands - puppetconfig <command> -h for more detail')

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

    # add-machine command
    add_machine_parser = subparsers.add_parser('add-machine', help='Add a new machine')
    add_machine_parser.set_defaults(command_type='add-machine')
    add_machine_parser.add_argument('machine', action='store', help='Machine Name')

    # delete-machine command
    delete_machine_parser = subparsers.add_parser('delete-machine', help='Delete a machine')
    delete_machine_parser.set_defaults(command_type='delete-machine')
    delete_machine_parser.add_argument('machine', action='store', help='Machine Name')

    # generate command
    generate_parser = subparsers.add_parser('generate', help='generate facts.yaml file')
    generate_parser.set_defaults(command_type='generate')

    parser.add_argument('--debug', action='store_true', dest='debug')

    # Parse arguments
    args = parser.parse_args()

    # Load settings from config file

    config_file = '/etc/puppetconfig.yml'
    if not os.path.exists(config_file):
        print("Config file", config_file, "does not exist")
        sys.exit(2)

    with open(config_file, "r") as configyml:
        cfg = yaml.safe_load(configyml)

    # Initialise Variables

    table_name = cfg['puppetconfig']['table_name']
    sas_token = cfg['puppetconfig']['sas_token']
    endpoint = cfg['puppetconfig']['endpoint']

    table_service_client = TableServiceClient(endpoint=endpoint, credential=AzureSasCredential(sas_token))
    table_client = table_service_client.get_table_client(table_name=table_name)

    # Perform function here

    if args.command_type == 'list-machines':
        do_list(table_client)

    elif args.command_type == 'show-machine':
        do_show_machine(table_client, args.machine)

    elif args.command_type == 'set-fact':
        do_set_fact(table_client, args.machine, args.fact, args.value)

    elif args.command_type == 'delete-fact':
        do_delete_fact(table_client, args.machine, args.fact)

    elif args.command_type == 'add-machine':
        do_add_machine(table_client, args.machine)

    elif args.command_type == 'delete-machine':
        do_delete_machine(table_client, args.machine)

    elif args.command_type == 'generate':
        do_generate(table_client)
    
    else:
        print("Invalid command")
        sys.exit(1)
    
    # Done
    sys.exit(0)

if __name__ == '__main__':
	main()
