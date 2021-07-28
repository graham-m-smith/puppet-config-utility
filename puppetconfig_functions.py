# Function to list the machines in the Azure table
def do_list(table_service, table_name):
    print("this is do_list")

    # Get data from Azure Table
    data = table_service.query_entities(table_name, "PartitionKey eq 'PuppetCfg'")

    for record in data:
        machine = record['RowKey']
        print(machine)

# Function to list the facts for a specific machine
def do_show_machine(table_service, table_name, machine):
    print("this is do_show_machine")

    query = "PartitionKey eq 'PuppetCfg' and RowKey eq '" + machine + "'"
    data = table_service.query_entities(table_name, query)
    print(data)