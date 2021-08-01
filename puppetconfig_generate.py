from time import strftime
import yaml
import io
from os import chown, mkdir, path
import pwd
import grp
import sys
from azure.core.exceptions import HttpResponseError
from shutil import copyfile
import datetime

# -----------------------------------------------------------------------------
# Function to generate facts.yaml file
# -----------------------------------------------------------------------------
def do_generate(table_client):

    # Initialize Variables
    puppet_facts_dir = '/puppet-facts'
    yaml_file = puppet_facts_dir + '/facts.yaml'
    now = datetime.datetime.now()
    timestamp = now.strftime('%d-%m-%Y-%H-%M-%S')
    backup_yaml_file = puppet_facts_dir + '/facts.yaml.' + timestamp
    puppet_uid = pwd.getpwnam('puppet').pw_uid
    puppet_gid = grp.getgrnam('puppet').gr_gid

    yamldata = {
        'server::facts': {}
    }

    nodelist = []
    nodedata = {}

    # Create puppet facts directory id it doesn't exist
    if not path.exists(puppet_facts_dir):
        mkdir(puppet_facts_dir)
        chown(puppet_facts_dir, puppet_uid, puppet_gid)

    # Get data from Azure Table
    try:
        data = table_client.query_entities("PartitionKey eq 'PuppetCfg'")
    except HttpResponseError as err:
        print("Error getting list of machines")
        print(err)
        sys.exit(2)

    # Loop round all records
    for record in data:
        noderecord = {}

        # Iterate round all properties in this record
        for key in record.keys():

            # Exclude unwanted properties
            if key == 'PartitionKey' or key == 'Timestamp' or key == 'etag':
                continue

            # If this is the nodename property (RowKey) add it to the list of nodes
            # otherwise add the property name & value to the noderecord dictionary
            this_value = record[key]
            if key == 'RowKey':
                nodename = this_value
                nodelist.append(this_value)
            else:
                noderecord[key] = this_value

        nodedata[nodename] = noderecord

    # Build yaml data structure
    for nodename in nodelist:
        print("Adding node", nodename)
        record = nodedata[nodename]
        yamldata['server::facts'][nodename] = {}
        for key in record.keys():
            value = record[key]
            print("- Adding fact", key, "value", value)
            yamldata['server::facts'][nodename][key] = value

    # Backup existing yaml file
    if path.exists(yaml_file):
        print("Backing up", yaml_file, "to", backup_yaml_file)
        copyfile(yaml_file, backup_yaml_file)

    # Create yaml file
    print("Creating", yaml_file, "file")
    with io.open(yaml_file, 'w', encoding='utf8') as outfile:
        yaml.dump(yamldata, outfile, default_flow_style=False, allow_unicode=True)

    chown(yaml_file, puppet_uid, puppet_gid)

    print("Completed")
