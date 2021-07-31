import sys
#from azure.core.exceptions import EntityAlreadyExists
from azure.data.tables import UpdateMode
from azure.core.exceptions import ResourceExistsError

# Function to list the machines in the Azure table
def do_list(table_client):

    # Get data from Azure Table
    data = table_client.query_entities("PartitionKey eq 'PuppetCfg'")

    for record in data:
        machine = record['RowKey']
        print(machine)

# Function to list the facts for a specific machine
def do_show_machine(table_client, machine):

    # Get data for this machine from Azure Table
    record = table_client.get_entity('PuppetCfg', machine)
    print("Facts for machine", machine)
    for key in record.keys():
        if key == 'PartitionKey' or key == 'Timestamp' or key == 'etag':
            continue

        value = record[key]
        print(key,':',value)

# Function to add/set a fact for a machine
def do_set_fact(table_client, machine, fact, value):

    # Get existing data for this machine
    record = table_client.get_entity('PuppetCfg', machine)
    record[fact] = value
    print(record)
    table_client.update_entity(mode=UpdateMode.REPLACE, entity=record)

# Function to delete a fact for a machine
def do_delete_fact(table_client, machine, fact):

    # Get existing data for this machine
    record = table_client.get_entity('PuppetCfg', machine)
    del record[fact]
    print(record)
    table_client.update_entity(mode=UpdateMode.REPLACE, entity=record)

# Function to add a new machine
def do_add_machine(table_client, machine):

    # Create new entity
    record = {}
    record['PartitionKey'] = 'PuppetCfg'
    record['RowKey'] = machine

    try:
        response = table_client.insert_entity(entity=record)
    except ResourceExistsError:
        print("Machine", machine, "already exists")
        sys.exit(1)

    print("Machine", machine, "added to configuration")