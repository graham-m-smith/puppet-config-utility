#!/usr/bin/env python3

# -----------------------------------------------------------------------------
# Required Pip Modules
# -----------------------------------------------------------------------------
# azure.data.tables >= 12.1.0
# azure.core >= 1.16.0
# PyYAML >= 5.4.1
# prettytable >= 2.1.0
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# To Do:
# -----------------------------------------------------------------------------
# lock file for generate
# logging
# debugging
# validated list of facts & values - quality check = store in same table, different partition
# additional commands
#   list-machines-with-fact <fact> --show-value
#   list-machines --table --columns=3
#   add-valid-fact <fact>
#   delete-valid-fact <fact>
#   list-valid-fact
#   add-valid-fact-value <fact> <value>
#   delete-valid-fact-value <fact> <value>
#   list-valid-fact-value <fact>
# check machine facts against valid fact list
# check machine fact value against valid fact value list
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Import Modules
# -----------------------------------------------------------------------------
import os
import sys
import argparse
from puppetconfig_functions import *
from puppetconfig_generate import *
from puppetconfig_constants import DEFAULT_CONFIG_FILE

# External Modules

try:
    from azure.data.tables import TableServiceClient
except ModuleNotFoundError:
    print("Module azure.data.tables not instaled [pip3 install azure.data.tables]")
    sys.exit(2)

try:
    from azure.core.credentials import AzureSasCredential
except ModuleNotFoundError:
    print("Module azure.core not instaled [pip3 install azure.core]")
    sys.exit(2)


# -----------------------------------------------------------------------------
# Main Function
# -----------------------------------------------------------------------------
def main():

    # Parse command line options

    parser = argparse.ArgumentParser(description='Utility to manage puppet fact configuration')
    parser.add_argument('--debug', action='store_true', dest='debug_flag')
    parser.add_argument('--verbose', action='store_true', dest='verbose_flag')
    parser.add_argument('--config-file', action='store', dest='config_file')
    parser.set_defaults(config_file=DEFAULT_CONFIG_FILE)

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

    # add-valid-fact command
    avf_parser = subparsers.add_parser('add-valid-fact', help='Add a new valid fact')
    avf_parser.set_defaults(command_type='add-valid-fact')
    avf_parser.add_argument('fact', action='store', help='Fact Name')

    # list-valid-fact command
    lvf_parser = subparsers.add_parser('list-valid-fact', help='List valid facts')
    lvf_parser.set_defaults(command_type='list-valid-fact')

    # delete-valid-fact command
    dvf_parser = subparsers.add_parser('delete-valid-fact', help='Delete a valid fact')
    dvf_parser.set_defaults(command_type='delete-valid-fact')
    dvf_parser.add_argument('fact', action='store', help='Fact Name')

    # Parse arguments
    args = parser.parse_args()
    if 'command_type' not in args:
        print("No command specified")
        parser.print_help()
        sys.exit(2)

    # Load settings from config file
    cfg = get_config(args.config_file)

    # Set proxy if required
    proxy = cfg['azure']['proxy']
    if proxy != 'none':
        os.environ['https_proxy'] = proxy

    # Initialise Variables

    table_name = cfg['azure']['table_name']
    sas_token = cfg['azure']['sas_token']
    endpoint = cfg['azure']['endpoint']

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
        do_generate(table_client, args.config_file)

    elif args.command_type == 'add-valid-fact':
        do_add_valid_fact(table_client, args.fact)
    
    else:
        print("Invalid command")
        parser.print_help()
        sys.exit(1)
    
    # Done
    sys.exit(0)

if __name__ == '__main__':
	main()
