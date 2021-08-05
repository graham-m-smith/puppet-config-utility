import sys
import os
import prettytable
import yaml
from azure.data.tables import UpdateMode
from azure.core.exceptions import ResourceExistsError, HttpResponseError
from puppetconfig_constants import PUPPETCFG_PK
from prettytable import PrettyTable

# -----------------------------------------------------------------------------
# Function to load configuration from yaml file
# -----------------------------------------------------------------------------
def get_config(config_file):
    if not os.path.exists(config_file):
        print("Config file", config_file, "does not exist")
        sys.exit(2)
        
    with open(config_file, "r") as configyml:
        cfg = yaml.safe_load(configyml)

    return cfg

# -----------------------------------------------------------------------------
# Function to list the machines in the Azure table
# -----------------------------------------------------------------------------
def do_list(table_client):

    # Get data from Azure Table
    query = f"PartitionKey eq '{PUPPETCFG_PK}'"

    try:
        data = table_client.query_entities(query)
    except HttpResponseError as err:
        print("Error getting list of machines")
        print(err)
        sys.exit(2)

    for record in data:
        machine = record['RowKey']
        print(machine)

# -----------------------------------------------------------------------------
# Function to list the facts for a specific machine
# -----------------------------------------------------------------------------
def do_show_machine(table_client, machine):

    # Initialise Variables
    excluded_keys = ['PartitionKey', 'Timestanp', 'etag', 'RowKey']

    # Get data for this machine from Azure Table
    try:
        record = table_client.get_entity(PUPPETCFG_PK, machine)
    except HttpResponseError as err:
        print("Machine", machine, "does not exist")
        sys.exit(1)

    print("")
    print("Facts for machine", machine)
    print("")

    table = PrettyTable()
    table.field_names = ['Fact', 'Value']
    table.align = 'l'
    table.set_style(prettytable.DEFAULT)

    for key in record.keys():
        #if key == 'PartitionKey' or key == 'Timestamp' or key == 'etag'or key == 'RowKey':
        if key in excluded_keys:
            continue

        value = record[key]
        table.add_row([key, value])

    print(table)

# -----------------------------------------------------------------------------
# Function to add/set a fact for a machine
# -----------------------------------------------------------------------------
def do_set_fact(table_client, machine, fact, value):

    # Get existing data for this machine
    try:
        record = table_client.get_entity(PUPPETCFG_PK, machine)
    except HttpResponseError:
        print("Machine", machine, "does not exist")
        sys.exit(1)

    # Add the fact to the record
    record[fact] = value

    # Update the table
    try:
        table_client.update_entity(mode=UpdateMode.REPLACE, entity=record)
    except HttpResponseError as err:
        print("Error updating record for", machine)
        print(err)
        sys.exit(2)

    print("Added fact", fact, "value", value, "to machine", machine)

# -----------------------------------------------------------------------------
# Function to delete a fact for a machine
# -----------------------------------------------------------------------------
def do_delete_fact(table_client, machine, fact):

    # Get existing data for this machine
    try:
        record = table_client.get_entity(PUPPETCFG_PK, machine)
    except HttpResponseError:
        print("Machine", machine, "does not exist")
        sys.exit(1)

    # Check if fact exists
    if not fact in record:
        print("fact", fact, "does not exist for", machine)
        sys.exit(1)

    # Delete fact from record
    del record[fact]

    # Update table
    try:
        table_client.update_entity(mode=UpdateMode.REPLACE, entity=record)
    except HttpResponseError as err:
        print("Error updating record for", machine)
        print(err)
        sys.exit(2)

    print("Delted fact", fact, "from machine", machine)

# -----------------------------------------------------------------------------
# Function to add a new machine
# -----------------------------------------------------------------------------
def do_add_machine(table_client, machine):

    # Create new entity
    record = {}
    record['PartitionKey'] = PUPPETCFG_PK
    record['RowKey'] = machine

    try:
        response = table_client.create_entity(entity=record)
    except ResourceExistsError:
        print("Machine", machine, "already exists")
        sys.exit(1)

    print("Machine", machine, "added to configuration")

# -----------------------------------------------------------------------------
# Function to delete a machine
# -----------------------------------------------------------------------------
def do_delete_machine(table_client, machine):

    try:
        data = table_client.get_entity(PUPPETCFG_PK, machine)
    except HttpResponseError as err:
        print("Machine", machine, "does not exist")
        sys.exit(1)

    try:
        table_client.delete_entity(partition_key=PUPPETCFG_PK, row_key=machine)
    except HttpResponseError as err:
        print("Error deleting", machine)
        print(err)
        sys.exit(2)

    print("Machine", machine, "deleted")
