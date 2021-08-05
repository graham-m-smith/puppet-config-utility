# -----------------------------------------------------------------------------
# Import Modules
# -----------------------------------------------------------------------------
import io
import pwd
import grp
import sys
import datetime
from os import chown, mkdir, path
from shutil import copyfile
from time import strftime
from puppetconfig_constants import PUPPETCFG_PK
from puppetconfig_functions import get_config

# External Modules

try:
    import yaml
except ModuleNotFoundError:
    print("Module PyYAML not instaled [pip3 install PyYAML]")
    sys.exit(2)

try:
    from azure.core.exceptions import HttpResponseError
except:
    print("Module azure.core not instaled [pip3 install azure.core]")
    sys.exit(2)

# -----------------------------------------------------------------------------
# Function to generate facts.yaml file
# -----------------------------------------------------------------------------
def do_generate(table_client, config_file):

    # Load settings from configuration file
    cfg = get_config(config_file)

    # Initialize Variables
    puppet_facts_dir = cfg['generate']['facts_dir']
    yaml_file_name = cfg['generate']['yaml_file']
    yaml_file = f'{puppet_facts_dir}/{yaml_file_name}'

    now = datetime.datetime.now()
    timestamp = now.strftime('%d-%m-%Y-%H-%M-%S')
    backup_yaml_file = f'{puppet_facts_dir}/{yaml_file_name}.{timestamp}'

    puppet_user = cfg['generate']['puppet_user']
    puppet_group = cfg['generate']['puppet_group']
    puppet_uid = pwd.getpwnam(puppet_user).pw_uid
    puppet_gid = grp.getgrnam(puppet_group).gr_gid

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
    query = f"PartitionKey eq '{PUPPETCFG_PK}'"

    try:
        data = table_client.query_entities(query)
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
