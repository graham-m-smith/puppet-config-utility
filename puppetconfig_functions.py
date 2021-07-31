import sys
#from azure.core.exceptions import EntityAlreadyExists
from azure.data.tables import UpdateMode

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
    table_client.update_entity(mode=UpdateMode.MERGE, entity=record)

# Function to delete a fact for a machine
def do_delete_fact(table_service, table_name, machine, fact):

    # Get existing data for this machine
    record = table_service.get_entity(table_name, 'PuppetCfg', machine)
    del record[fact]
    print(record)
    table_service.insert_or_replace_entity(table_name, record)

# Function to add a new machine
def do_add_machine(table_service, table_name, machine):

    # Create new entity

    record = {}
    record['PartitionKey'] = 'PuppetCfg'
    record['RowKey'] = machine

    try:
        result = table_service.insert_entity(table_name, record)
    except:
        print("Machine", machine, "already exists")
        sys.exit(1)

    print("Machine", machine, "added to configuration")