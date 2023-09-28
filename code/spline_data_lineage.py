import json
import os

def fetch_target_ids(execution_plan_path, target_name):
    target_ids = []
    with open(execution_plan_path, 'r') as f:
        execution_plans = json.load(f)
    for plan in execution_plans:
        if plan.get('name') == target_name:
            target_ids.append(plan['_id'])
            print(f"Found target ID: {plan['_id']}")
    return target_ids

def filter_operations(operation_path, target_ids):
    filtered_operations = []
    with open(operation_path, 'r') as f:
        operations = json.load(f)
    for operation in operations:
        if operation.get('_belongsTo') in target_ids and operation.get('type') in ['Read', 'Write']:
            filtered_operations.append(operation)
            print(f"Filtered operation: {operation}")
    return filtered_operations


def generate_and_transform_result_dict(filtered_operations):
    new_data = {'lineage_map': {}}
    read_file_name, write_file_name = None, None  
    read_belongs_to, write_belongs_to = None, None  

    for operation in filtered_operations:
        belongs_to = operation.get('_belongsTo')
        operation_type = operation.get('type')

        if operation_type == 'Read':
            input_source = operation.get('inputSources', [])[0] if operation.get('inputSources') else None
            read_file_name = os.path.basename(input_source) if input_source else None
            read_belongs_to = belongs_to  

        elif operation_type == 'Write':
            output_source = operation.get('outputSource')
            write_file_name = os.path.basename(output_source) if output_source else None
            write_belongs_to = belongs_to  

        if read_file_name and write_file_name and read_belongs_to == write_belongs_to:
            if read_file_name not in new_data['lineage_map']:
                new_data['lineage_map'][read_file_name] = []
            new_data['lineage_map'][read_file_name].append(write_file_name)

    return new_data
    
    
def fetch_redshift_table(filtered_operations, new_data):
    redshift_table = None
    for operation in filtered_operations:
        output_source = operation.get('outputSource', '')
        if 'redshift' in output_source:
            redshift_table = output_source.split('/')[-2]  
            print(f"Redshift table extracted from outputSource: {redshift_table}")

    if redshift_table:
        last_data_flow_file = None
        for key in reversed(list(new_data['lineage_map'].keys())):
            if new_data['lineage_map'][key]:
                last_data_flow_file = new_data['lineage_map'][key][-1]
                break

        if last_data_flow_file:
            new_key = f"{last_data_flow_file}" 
            new_data['lineage_map'][new_key] = [redshift_table]  

        

if __name__ == "__main__":
    execution_plan_path = "/path/to/executionPlan.json"
    operation_path = "/path/to/operation.json"
    target_name = 'Your appName in executionPlan.json'

    target_ids = fetch_target_ids(execution_plan_path, target_name)
    filtered_operations = filter_operations(operation_path, target_ids)
    new_data = generate_and_transform_result_dict(filtered_operations)
    fetch_redshift_table(filtered_operations, new_data)  

    with open("/path/to/spline_lineage_map.json", 'w') as f:
        json.dump(new_data, f, indent=4)

    print(f"New JSON data has been written to /path/to/spline_lineage_map.json")
