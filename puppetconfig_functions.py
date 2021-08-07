# -----------------------------------------------------------------------------
# Import Modules
# -----------------------------------------------------------------------------
import sys
import os
import uuid
from puppetconfig_constants import PUPPETCFG_PK, PUPPETVF_PK, PUPPETVFV_PK

# External Modules

try:
    import yaml
except ModuleNotFoundError:
    print("Module PyYAML not instaled [pip3 install PyYAML]")
    sys.exit(2)

try:
    from azure.data.tables import UpdateMode
except ModuleNotFoundError:
    print("Module azure.data.tables not instaled [pip3 install azure.data.tables]")
    sys.exit(2)

try:
    from azure.core.exceptions import ResourceExistsError, HttpResponseError
except:
    print("Module azure.core not instaled [pip3 install azure.core]")
    sys.exit(2)

try:
    from prettytable import PrettyTable
except ModuleNotFoundError:
    print("Module prettytable not instaled [pip3 install prettytable]")
    sys.exit(2)


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

    for key in record.keys():
        # Ignore non-fact related data
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

    # Is this a valid fact
    if check_fact_exists(table_client, fact) == False:
        print("Invalid fact -", fact)
        sys.exit(2)

    # Is there a list of valid values for this fact?
    has_valid_values = check_fact_has_valid_values(table_client, fact)
    if  has_valid_values == True:
        # Check value is valid
        value_is_valid = check_value(table_client, fact, value)
        if value_is_valid == False:
            print('Value', value, "for fact", fact, "is not valid")
            sys.exit(2)
        
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

# -----------------------------------------------------------------------------
# Function to add a valid fact
# -----------------------------------------------------------------------------
def do_add_valid_fact(table_client, fact):

    # Create new entity
    record = {}
    record['PartitionKey'] = PUPPETVF_PK
    record['RowKey'] = fact
    record['ValidValues'] = 'no'

    # Add record to Azure table
    try:
        response = table_client.create_entity(entity=record)
    except ResourceExistsError:
        print("Valid Fact", fact, "already exists")
        sys.exit(1)

    print("Valid Fact", fact, "added to configuration")

# -----------------------------------------------------------------------------
# Function to list valid facts
# -----------------------------------------------------------------------------
def do_list_valid_fact(table_client):

    # Get data from Azure Table
    query = f"PartitionKey eq '{PUPPETVF_PK}'"

    try:
        data = table_client.query_entities(query)
    except HttpResponseError as err:
        print("Error getting list of valid facts")
        print(err)
        sys.exit(2)

    # Create table to display data
    table = PrettyTable()
    table.field_names = ['Valid Facts', 'Has List Of Valid Values']
    table.align = 'l'

    # Add data to table
    for record in data:
        fact = record['RowKey']
        valid_values = record['ValidValues']
        table.add_row([fact, valid_values])

    # Display table
    print(table)

# -----------------------------------------------------------------------------
# Functiom to delete valid fact
# - will need functionality to remove valid fact values if they exist
# - will need functionality to check if this is being used by any machines
# -----------------------------------------------------------------------------
def do_delete_valid_fact(table_client, fact):

    # Check if fact exists
    try:
        data = table_client.get_entity(PUPPETVF_PK, fact)
    except HttpResponseError as err:
        print("Valid Fact", fact, "does not exist")
        sys.exit(1)

    # Delete fact from Azure table
    try:
        table_client.delete_entity(partition_key=PUPPETVF_PK, row_key=fact)
    except HttpResponseError as err:
        print("Error deleting valid fact", fact)
        print(err)
        sys.exit(2)

    print("Valid Fact", fact, "deleted")

# -----------------------------------------------------------------------------
# Check if a fact exists
# -----------------------------------------------------------------------------
def check_fact_exists(table_client, fact):

    fact_exists = True

    # Check if fact exists
    try:
        data = table_client.get_entity(PUPPETVF_PK, fact)
    except HttpResponseError as err:
        fact_exists = False
    
    return fact_exists

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
def check_fact_has_valid_values(table_client, fact):

    has_valid_values = False

    try:
        fact_entity = table_client.get_entity(PUPPETVF_PK, fact)
    except HttpResponseError as err:
        print(err)
        sys.exit(2)

    if fact_entity['ValidValues'] == 'yes':
        has_valid_values = True

    return has_valid_values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
def check_value(table_client, fact, value):

    valid_value = False

    query = f"PartitionKey eq '{PUPPETVFV_PK}' and VFFact eq '{fact}' and VFValue eq '{value}'"
    try:
        data = table_client.query_entities(query)
    except HttpResponseError as err:
        print("error with query")
        print(query)
        print(err)
        sys.exit(2)

    record_count = get_record_count(data)
    print("record_count", record_count)
    if record_count > 0:
        valid_value = True
    
    return valid_value

# -----------------------------------------------------------------------------
# Function to add a valid fact value
# -----------------------------------------------------------------------------
def do_add_valid_fact_value(table_client, fact, value):

    # Check if this is a valid fact

    if not check_fact_exists(table_client, fact):
        print("Fact", fact, "does not exist")
        sys.exit(1)

    # Create new entity
    record = {}
    record['PartitionKey'] = PUPPETVFV_PK
    record['RowKey'] = str(uuid.uuid4())
    record['VFVFact'] = fact
    record['VFVValue'] = value

    # Check if fact/value combination already exists
    if check_valid_fact_value_exists(table_client, fact, value) == True:
        print("Value", value, "for fact", fact, "already exists")
        sys.exit(1)

    # Add record to Azure table
    try:
        response = table_client.create_entity(entity=record)
    except ResourceExistsError:
        print("Something has gone wrong")
        sys.exit(1)

    # Update fact record to indicate it has valid values
    data = table_client.get_entity(PUPPETVF_PK, fact)
    data['ValidValues'] = 'yes'
    try:
        table_client.update_entity(mode=UpdateMode.REPLACE, entity=data)
    except HttpResponseError as err:
        print("Error updating record for", fact)
        print(err)
        sys.exit(2)

    print("Valid Fact Value", value, "added to fact", fact)

# -----------------------------------------------------------------------------
# Check if a valid fact value exists
# -----------------------------------------------------------------------------
def check_valid_fact_value_exists(table_client, fact, value):

    # Get data from Azure Table
    query = f"PartitionKey eq '{PUPPETVFV_PK}' and VFVFact eq '{fact}' and VFVValue eq '{value}'"

    try:
        data = table_client.query_entities(query)
    except HttpResponseError as err:
        print("error with query")
        print(query)
        print(err)
        sys.exit(2)

    record_count = get_record_count(data)
    if record_count == 0:
        fact_value_exists = False
    else:
        fact_value_exists = True

    return fact_value_exists

# -----------------------------------------------------------------------------
# Function to return number of records
# -----------------------------------------------------------------------------
def get_record_count(item):

    record_count = 0
    for entity in item:
        record_count += 1

    return record_count

# -----------------------------------------------------------------------------
# Function to return list of valid values for specified fact
# -----------------------------------------------------------------------------
def do_list_valid_fact_value(table_client, fact):

    # Get data from Azure Table
    query = f"PartitionKey eq '{PUPPETVFV_PK}' and VFVFact eq '{fact}'"

    try:
        data = table_client.query_entities(query)
    except HttpResponseError as err:
        print("error with query")
        print(query)
        print(err)
        sys.exit(2)

    # Create table to display data
    table = PrettyTable()
    table.field_names = [f'Valid Values for fact {fact}']
    table.align = 'l'
    record_count = 0

    # Add data to table
    for record in data:
        record_count += 1
        value = record['VFVValue']
        table.add_row([value])

    # Display table
    if record_count > 0:
        print(table)
    else:
        print("No valid values for fact", fact)

  
# -----------------------------------------------------------------------------
# Function to delete a valid value for a fact
# -----------------------------------------------------------------------------
def do_delete_valid_fact_value(table_client, fact, value):

    # Check if this fact/value combination exists

    if check_valid_fact_value_exists(table_client, fact, value) == False:
        print("Value", value, "for fact", fact, "does not exist")
        sys.exit(1)

    # Get data from Azure Table
    query = f"PartitionKey eq '{PUPPETVFV_PK}' and VFVFact eq '{fact}' and VFVValue eq '{value}'"

    try:
        data = table_client.query_entities(query)
    except HttpResponseError as err:
        print("error with query")
        print(query)
        print(err)
        sys.exit(2)

    # Delete Record
    for record in data:
        row_key = record['RowKey']
        try:
            table_client.delete_entity(partition_key=PUPPETVFV_PK, row_key=row_key)
        except HttpResponseError as err:
            print("Error deleting valid fact", fact)
            print(err)
            sys.exit(2)

    # How many valid values for this fact
    num_valid_values = get_num_valid_values(table_client, fact)

    # Update fact record if no valid values
    if num_valid_values == 0:
        try:
            fact_entity =  table_client.get_entity(PUPPETVF_PK, fact)
        except HttpResponseError as err:
            print("Error")
            print(err)
            sys.exit(2)

        fact_entity['ValidValues'] = 'no'

        try:
            table_client.update_entity(mode=UpdateMode.REPLACE, entity=fact_entity)
        except HttpResponseError as err:
            print("Error updating record for", fact)
            print(err)
            sys.exit(2)

    print("Value", value, "removed from fact", fact)

# -----------------------------------------------------------------------------
# Function to return number of valid values for specified fact
# -----------------------------------------------------------------------------
def get_num_valid_values(table_client, fact):

    query = f"PartitionKey eq '{PUPPETVFV_PK}' and VFVFact eq '{fact}'"
    try:
        data = table_client.query_entities(query)
    except HttpResponseError as err:
        print("error with query")
        print(query)
        print(err)
        sys.exit(2)

    num_values = 0
    for record in data:
        num_values += 1

    return num_values