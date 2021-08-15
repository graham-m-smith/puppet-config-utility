# -----------------------------------------------------------------------------
# Import Modules
# -----------------------------------------------------------------------------

import sys
from puppetconfig_functions import get_config
import puppetconfig_globals as gbl

# -----------------------------------------------------------------------------
# External Modules
# -----------------------------------------------------------------------------

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
# Function to validate contents of facts.yaml
# -----------------------------------------------------------------------------
def do_validate(table_client, config_file):

   # Load settings from configuration file
    cfg = get_config(config_file)

    # Initialize Variables
    puppet_facts_dir = cfg['generate']['facts_dir']
    yaml_file_name = cfg['generate']['yaml_file']
    yaml_file = f'{puppet_facts_dir}/{yaml_file_name}'

    # Load contents of yaml file

    with open(yaml_file, "r") as ymlfile:
        facts = yaml.safe_load(ymlfile)

    # Iterate through the yaml data

    for section in facts:
        print(section)
        for node in facts[section]:
            print(f"- {node}")
            for item in facts[section][node]:
                value = facts[section][node][item]
                print(f"-- {item} = {value}")

