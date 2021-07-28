def do_list(table_service, table_name):
    print("this is do_list")

    # Get data from Azure Table
    data = table_service.query_entities(table_name, "PartitionKey eq 'PuppetCfg'")

    for record in data:
        machine = record['RowKey']
        print(machine)