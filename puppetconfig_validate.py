# -----------------------------------------------------------------------------
# Import Modules
# -----------------------------------------------------------------------------

import sys
from puppetconfig_functions import get_config, check_machine_exists, check_fact_exists, check_fact_has_valid_values, check_value
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

try:
    from prettytable import PrettyTable
except ModuleNotFoundError:
    print("Module prettytable not instaled [pip3 install prettytable]")
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
        yaml_data = yaml.safe_load(ymlfile)

    # Iterate through the yaml data

    validated = True
    error_list = []

    print("Starting validation")

    for section in yaml_data:

        # Check machines
        for node in yaml_data[section]:
            if gbl.VERBOSE:
                print(f"- Checking node {node}")

            # Check that node exists in the table
            if check_machine_exists(table_client, node) == False:
                error_list.append(f"Machine {node} does not exist in table")
                validated = False
                continue

            # Check facts for this machine
            for fact in yaml_data[section][node]:
                value = yaml_data[section][node][fact]
                if gbl.VERBOSE:
                    print(f"-- checking fact {fact}")
                if check_fact_exists(table_client, fact) == False:
                    error_list.append(f"Fact {fact} is invalid for machine {node}")
                    validated = False
                else:
                    # Does this fact have valid values?
                    if check_fact_has_valid_values(table_client, fact) == True:
                        # If so, check that the value is valid
                        
                            error_list.append(f"Value {value} for fact {fact} on machine {node} is invalid")
                            validated = False

    if validated == True:
        print("Validation successful")
    else:
        print("Validation unsuccessful")
        table = PrettyTable()
        table.field_names = ['Error', 'Detail']
        table.align = 'l'
        count = 1

        for error in error_list:
            table.add_row([count, error])
            count += 1

        print(table)

    return validated
