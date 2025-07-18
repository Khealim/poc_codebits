import streamlit as st
import json
import pandas as pd
from avro.schema import parse as parse_avro_schema
from flatten_dict import flatten
import re

# Set page configuration
st.set_page_config(
    page_title="Avro Schema Analyzer",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .schema-field {
        padding: 8px;
        margin: 4px 0;
        border-radius: 4px;
        background-color: #f0f2f6;
    }
    .schema-field-primitive {
        border-left: 4px solid #4CAF50;
    }
    .schema-field-record {
        border-left: 4px solid #2196F3;
    }
    .schema-field-array {
        border-left: 4px solid #FF9800;
    }
    .schema-field-enum {
        border-left: 4px solid #9C27B0;
    }
    .schema-field-union {
        border-left: 4px solid #607D8B;
    }
    .ddl-code {
        white-space: pre-wrap;
        font-family: monospace;
        background-color: #1e1e1e;
        color: #f8f8f2;
        padding: 16px;
        border-radius: 6px;
        border-left: 4px solid #2196F3;
        margin: 10px 0;
        overflow-x: auto;
    }
    /* SQL keyword highlighting */
    .ddl-code .keyword {
        color: #569cd6;
        font-weight: bold;
    }
    /* Table name highlighting */
    .ddl-code .table-name {
        color: #4ec9b0;
    }
    /* Comment highlighting */
    .ddl-code .comment {
        color: #608b4e;
        font-style: italic;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar content
st.sidebar.title("Avro Schema Analyzer")
st.sidebar.info("Upload an Avro schema file and customize flattening options.")

# Define Avro to Hive type mapping
AVRO_TO_HIVE_TYPE = {
    'string': 'VARCHAR(255)',
    'int': 'INT',
    'long': 'BIGINT',
    'float': 'FLOAT',
    'double': 'DOUBLE',
    'boolean': 'BOOLEAN',
    'bytes': 'BINARY',
    'null': 'NULL',
    'timestamp-millis': 'TIMESTAMP',
    'date': 'DATE',
    'time-millis': 'VARCHAR(30)',
    'enum': 'VARCHAR(100)'
}

# Helper function to get a good column name
def get_column_name(field_path):
    # Remove array notation
    clean_name = re.sub(r'\[\].item', '', field_path)
    # Convert camelCase to snake_case
    snake_case = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', clean_name).lower()
    # Replace dots with underscores
    snake_case = snake_case.replace('.', '_')
    # Remove any special characters
    snake_case = re.sub(r'[^a-z0-9_]', '', snake_case)
    return snake_case

# Step 1: Load Avro schema
uploaded_file = st.sidebar.file_uploader("Upload your Avro schema (JSON format)")

if uploaded_file:
    schema_json = json.load(uploaded_file)
    schema = parse_avro_schema(json.dumps(schema_json))
    
    # Extract schema metadata
    schema_name = schema_json.get("name", "Unknown")
    schema_namespace = schema_json.get("namespace", "")
    schema_doc = schema_json.get("doc", "No documentation")
    
    # Main content area
    st.title(f"Schema: {schema_namespace}.{schema_name}")
    st.markdown(f"*{schema_doc}*")
    
    # Set up tabs for different views
    schema_tab, ddl_tab = st.tabs(["Schema Structure", "DDL Generator"])
    
    # Step 2: Extract fields and allow array flattening
    def extract_field_paths(schema_obj, prefix="", array_paths=None, named_types=None, include_types=False):
        """
        Extracts flat field paths from an Avro schema. Handles arrays optionally.
        """
        from avro.schema import RecordSchema, ArraySchema, UnionSchema, EnumSchema, Field, PrimitiveSchema

        if named_types is None:
            named_types = {}
        if array_paths is None:
            array_paths = []

        paths = []
        if isinstance(schema_obj, RecordSchema):
            named_types[schema_obj.fullname] = schema_obj
            for field in schema_obj.fields:
                full_name = f"{prefix}.{field.name}" if prefix else field.name
                sub_schema = field.type
                field_type = "union"
                field_doc = getattr(field, 'doc', '')
                
                if isinstance(sub_schema, UnionSchema):
                    # Pick first non-null type
                    non_null_types = [t for t in sub_schema.schemas if t.type != "null"]
                    if non_null_types:
                        sub_schema = non_null_types[0]
                        is_optional = True
                    else:
                        sub_schema = sub_schema.schemas[0]
                        is_optional = False
                else:
                    is_optional = False

                if isinstance(sub_schema, RecordSchema):
                    field_type = "record"
                    paths += extract_field_paths(sub_schema, full_name, array_paths, named_types, include_types)
                elif isinstance(sub_schema, ArraySchema):
                    field_type = "array"
                    # Check if this array's path is in the selected arrays to flatten
                    if full_name in array_paths:
                        # If this array should be flattened, process its items
                        paths += extract_field_paths(sub_schema.items, full_name + "[].item", array_paths, named_types, include_types)
                    else:
                        # If not flattened, just add as array marker and don't process nested fields
                        if include_types:
                            paths.append((full_name + "[]", field_type, field_doc, is_optional))
                        else:
                            paths.append(full_name + "[]")
                elif isinstance(sub_schema, EnumSchema):
                    field_type = "enum"
                    enum_values = sub_schema.symbols if hasattr(sub_schema, 'symbols') else []
                    if include_types:
                        paths.append((full_name, field_type, field_doc, is_optional, enum_values))
                    else:
                        paths.append(full_name + " (enum)")
                else:
                    field_type = sub_schema.type if hasattr(sub_schema, 'type') else "unknown"
                    if include_types:
                        logical_type = getattr(sub_schema, 'logical_type', None)
                        paths.append((full_name, field_type, field_doc, is_optional, logical_type))
                    else:
                        paths.append(full_name)
        return paths

    
    # Extract all array fields first to populate checkboxes
    def find_array_fields(schema_obj, prefix="", array_fields=None, parent_arrays=None):
        """
        Find array fields in the schema, but don't traverse into arrays unless they're already
        selected for flattening in parent_arrays.
        """
        from avro.schema import RecordSchema, ArraySchema, UnionSchema
        
        if array_fields is None:
            array_fields = []
        if parent_arrays is None:
            parent_arrays = []
            
        if isinstance(schema_obj, RecordSchema):
            for field in schema_obj.fields:
                full_name = f"{prefix}.{field.name}" if prefix else field.name
                sub_schema = field.type
                
                if isinstance(sub_schema, UnionSchema):
                    non_null_types = [t for t in sub_schema.schemas if t.type != "null"]
                    if non_null_types:
                        sub_schema = non_null_types[0]
                
                if isinstance(sub_schema, RecordSchema):
                    find_array_fields(sub_schema, full_name, array_fields, parent_arrays)
                elif isinstance(sub_schema, ArraySchema):
                    array_fields.append(full_name)
                    
                    # Only process items in this array if it's in the parent_arrays list
                    # This prevents discovering nested arrays inside of arrays that aren't flattened
                    if full_name in parent_arrays:
                        find_array_fields(sub_schema.items, full_name + "[].item", array_fields, parent_arrays)
        
        return array_fields
    
    # Initialize session state for selected arrays
    if 'selected_arrays' not in st.session_state:
        st.session_state.selected_arrays = []
    
    # Initial discovery of arrays
    if 'first_load' not in st.session_state:
        # First discovery of top-level arrays (only run once)
        array_fields = find_array_fields(schema)
        st.session_state.first_load = True
    else:
        # On subsequent runs, find arrays including those in flattened parent arrays
        array_fields = find_array_fields(schema, parent_arrays=st.session_state.selected_arrays)
    
    # Display array checkboxes
    if array_fields:
        st.sidebar.markdown("### Array Flattening Options")
        
        changed = False
        # Add Select All / Deselect All buttons
        col1, col2 = st.sidebar.columns(2)
        
        with col1:
            if st.button("Select All", key="select_all_arrays"):
                # Mark all arrays as selected
                st.session_state.selected_arrays = array_fields.copy()
                changed = True
        
        with col2:
            if st.button("Deselect All", key="deselect_all_arrays"):
                # Clear all array selections
                st.session_state.selected_arrays = []
                changed = True
        
        # Display individual array checkboxes
        st.sidebar.markdown("#### Arrays to Flatten")
        
        for field in array_fields:
            is_selected = field in st.session_state.selected_arrays
            
            # Create checkbox for this array field
            if st.sidebar.checkbox(f"{field}[]", value=is_selected, key=f"flatten_{field}"):
                if field not in st.session_state.selected_arrays:
                    st.session_state.selected_arrays.append(field)
                    changed = True
            elif field in st.session_state.selected_arrays:
                st.session_state.selected_arrays.remove(field)
                changed = True
        
        # If something changed, trigger a rerun to update available arrays
        if changed:
            st.rerun()
        
    # Use the selected arrays for extraction
    selected_arrays = st.session_state.selected_arrays
    
    # Option to show field types
    show_types = st.sidebar.checkbox("Show field types", value=True)
    
    # Extract fields with type information
    flat_paths = extract_field_paths(schema, array_paths=selected_arrays, include_types=True)
    
    # Function to build hierarchical table structure based on arrays
    def build_table_hierarchy(paths):
        # Find the root table fields (those not in arrays or in arrays but not flattened)
        root_fields = []
        array_tables = {}
        
        # Parse each path to determine if it belongs in the root table or a child table
        for path_info in paths:
            if isinstance(path_info, tuple) and len(path_info) >= 4:
                path, field_type, doc, optional = path_info[:4]
                
                # Check if this field is part of a flattened array
                belongs_to_array = False
                for array_path in selected_arrays:
                    if path.startswith(array_path + "[].item"):
                        # This is a direct match for this array path
                        if array_path not in array_tables:
                            array_tables[array_path] = []
                        array_tables[array_path].append(path_info)
                        belongs_to_array = True
                        break
                
                # If it doesn't belong to any array, it's a root field
                if not belongs_to_array:
                    root_fields.append(path_info)
        
        # Now handle nested arrays separately to ensure they're properly processed
        for array_path in selected_arrays:
            if array_path not in array_tables:
                array_tables[array_path] = []
                
            # Check if this is a nested array (contains another selected array path)
            for nested_array in selected_arrays:
                if nested_array.startswith(array_path + "[].item") and nested_array != array_path:
                    # Find fields that belong to this nested array
                    for path_info in paths:
                        if isinstance(path_info, tuple) and len(path_info) >= 4:
                            path, field_type, _, _ = path_info[:4]
                            if path.startswith(nested_array + "[].item"):
                                # This field belongs to the nested array
                                if nested_array not in array_tables:
                                    array_tables[nested_array] = []
                                array_tables[nested_array].append(path_info)
        return {
            "root_table": root_fields,
            "array_tables": array_tables
        }
    
    # First tab - Schema Structure
    with schema_tab:
        st.subheader("Schema Structure")
        
        if show_types:
            # Create a DataFrame for better visualization
            field_data = []
            for path_info in flat_paths:
                if len(path_info) >= 4:
                    path, field_type, doc, optional = path_info[:4]
                    enum_values = path_info[4] if len(path_info) > 4 and isinstance(path_info[4], list) else None
                    logical_type = path_info[4] if len(path_info) > 4 and not isinstance(path_info[4], list) else None
                    
                    type_display = logical_type if logical_type else field_type
                    if enum_values:
                        type_display = f"enum ({', '.join(enum_values)})"
                    
                    field_data.append({
                        "Field Path": path,
                        "Type": type_display,
                        "Optional": "Yes" if optional else "No",
                        "Documentation": doc
                    })
            
            if field_data:
                df = pd.DataFrame(field_data)
                st.dataframe(df, use_container_width=True, height=600)
        else:
            # Simple list display
            for path in flat_paths:
                if isinstance(path, tuple):
                    st.markdown(f"<div class='schema-field schema-field-{path[1]}'>{path[0]}</div>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<div class='schema-field'>{path}</div>", unsafe_allow_html=True)
    
    # Second tab - DDL Generator
    # Second tab - DDL Generator
    with ddl_tab:
        st.subheader("DDL Generator")
        
        # Build table hierarchy
        table_structure = build_table_hierarchy(flat_paths)
        root_fields = table_structure["root_fields"] = table_structure["root_table"]
        array_tables = table_structure["array_tables"]
        
        # Table naming options
        st.markdown("### Table Naming")
        root_table_name = st.text_input("Root Table Name", value=f"{schema_name.lower()}")
        
        # Container for common fields
        st.markdown("### Common Fields (included in all tables)")
        common_fields = st.text_area("Add common fields (one per line, format: name type)", 
            value="""depaudit_process_run_id STRING
    depaudit_insert_date TIMESTAMP
    depaudit_kafka_topic VARCHAR(100)
    depaudit_kafka_partition INT
    depaudit_kafka_offset BIGINT
    depaudit_kafka_insert_date TIMESTAMP""")
        
        common_fields_list = []
        for line in common_fields.strip().split('\n'):
            if line.strip():
                parts = line.strip().split(None, 1)
                if len(parts) == 2:
                    common_fields_list.append((parts[0], parts[1]))
        
        # Natural Key selection for root table
        st.markdown("### Select Natural Key for Root Table")
        
        root_fields_options = []
        for field_info in root_fields:
            if isinstance(field_info, tuple) and len(field_info) >= 4:
                path, field_type, _, _ = field_info[:4]
                if field_type not in ('array', 'record'):
                    root_fields_options.append(path)
        
        natural_key = st.selectbox("Select natural key for root table", options=root_fields_options)
        
        # Natural keys for array tables
        array_natural_keys = {}
        parent_child_relations = {}
        
        if array_tables:
            st.markdown("### Select Natural Keys for Array Tables")
            
            # First, identify parent-child relationships between arrays
            array_hierarchy = {}
            for array_path in array_tables.keys():
                parts = array_path.split('.')
                parent_array = None
                # Check if this array is nested within another array
                for i in range(len(parts)-1, 0, -1):
                    potential_parent = '.'.join(parts[:i])
                    if potential_parent in array_tables:
                        parent_array = potential_parent
                        break
                        
                if parent_array:
                    if parent_array not in array_hierarchy:
                        array_hierarchy[parent_array] = []
                    array_hierarchy[parent_array].append(array_path)
            
            # Now create selection for each array table
            for array_path, fields in array_tables.items():
                # Create a cleaner display name
                display_name = array_path.replace("[].item", "")
                col1, col2 = st.columns(2)
                
                # Column for natural key selection
                with col1:
                    # Get fields specific to this array table
                    field_options = []
                    for field_info in fields:
                        if isinstance(field_info, tuple) and len(field_info) >= 4:
                            path, field_type, _, _ = field_info[:4]
                            # Skip nested arrays and records
                            if field_type not in ('array', 'record'):
                                # Extract just the field name without the path
                                field_name = path.replace(array_path + ".", "")
                                field_options.append((path, field_name))
                    
                    if field_options:
                        options_dict = {p[1]: p[0] for p in field_options}
                        selected_key = st.selectbox(f"Natural key for {display_name}", 
                                                options=list(options_dict.keys()),
                                                key=f"key_{array_path}")
                        array_natural_keys[array_path] = options_dict[selected_key]
                
                # Column for parent relation selection
                with col2:
                    # Check if this is a nested array
                    if array_path in [child for children in array_hierarchy.values() for child in children]:
                        # This is a nested array, find its parent
                        parent_array = None
                        for p, children in array_hierarchy.items():
                            if array_path in children:
                                parent_array = p
                                break
                        
                        if parent_array:
                            # Offer to choose between root table's key or parent array's key
                            relation_options = ["Root table's key", "Parent array's key"]
                            relation_choice = st.radio(f"Link {display_name} to:", 
                                                    options=relation_options,
                                                    key=f"relation_{array_path}")
                            parent_child_relations[array_path] = {
                                "parent": parent_array if relation_choice == "Parent array's key" else "root",
                                "choice": relation_choice
                            }
        
        # Generate DDL statements
        if st.button("Generate DDL"):
            # Function to convert Avro type to Hive type
            def avro_to_hive_type(field_info):
                _, field_type, _, optional, *rest = field_info + (None,)
                
                # Check for logical type
                logical_type = rest[0] if rest else None
                
                # Handle case when logical_type is a list (enum values)
                if isinstance(logical_type, list):
                    # This is likely enum values list, use enum type
                    hive_type = AVRO_TO_HIVE_TYPE.get('enum', 'STRING')
                elif logical_type:
                    # If it's a string logical type, use it to lookup
                    hive_type = AVRO_TO_HIVE_TYPE.get(logical_type, 'STRING')
                else:
                    # Otherwise use the field type
                    hive_type = AVRO_TO_HIVE_TYPE.get(field_type, 'STRING')
                
                # Special handling for enums
                if field_type == 'enum':
                    hive_type = AVRO_TO_HIVE_TYPE['enum']
                
                return hive_type
            
            # Generate root table DDL
            root_ddl = f"CREATE TABLE {root_table_name} (\n"
            
            # Add common fields
            for name, type_info in common_fields_list:
                root_ddl += f"  {name} {type_info},\n"
            
            # Add natural key and root fields
            natural_key_col_name = ""
            for field_info in root_fields:
                if isinstance(field_info, tuple) and len(field_info) >= 4:
                    path, field_type, doc, optional = field_info[:4]
                    
                    # Generate column name
                    column_name = get_column_name(path)
                    
                    # Mark if this is the natural key
                    if path == natural_key:
                        natural_key_col_name = column_name
                    
                    # For array fields, include them as JSON string
                    if field_type == 'array':
                        root_ddl += f"  {column_name}_json STRING"
                        if doc:
                            root_ddl += f" COMMENT 'JSON array: {doc}'"
                        root_ddl += ",\n"
                    # For record fields not flattened, include them as JSON string
                    elif field_type == 'record':
                        root_ddl += f"  {column_name}_json STRING"
                        if doc:
                            root_ddl += f" COMMENT 'JSON object: {doc}'"
                        root_ddl += ",\n"
                    else:
                        # Normal fields
                        hive_type = avro_to_hive_type(field_info)
                        
                        # Add column
                        root_ddl += f"  {column_name} {hive_type}"
                        if doc:
                            root_ddl += f" COMMENT '{doc}'"
                        root_ddl += ",\n"
            
            # Remove trailing comma
            if root_ddl.endswith(",\n"):
                root_ddl = root_ddl[:-2]
            
            root_ddl += "\n)"
            
            # Generate array table DDLs
            array_ddls = {}
            #
            # Fix for parent-child relationships of nested arrays
            # Replace the current foreign key relationship detection code with this improved version

            # For array tables in the DDL generation part:
            for array_path, fields in array_tables.items():
                # Create table name from array path
                table_name = f"{root_table_name}_{get_column_name(array_path)}"
                
                # Start DDL
                array_ddl = f"CREATE TABLE {table_name} (\n"
                
                # Add common fields
                for name, type_info in common_fields_list:
                    array_ddl += f"  {name} {type_info},\n"
                
                # First check if this is a nested array - this takes precedence
                is_nested_array = False
                parent_array = None
                
                # Find the most immediate parent array (if any)
                for potential_parent in selected_arrays:
                    if array_path != potential_parent and array_path.startswith(potential_parent + "[].item"):
                        # This is potentially a nested array
                        if not parent_array or len(potential_parent) > len(parent_array):
                            parent_array = potential_parent
                            is_nested_array = True
                
                # If this is a nested array, use its parent's natural key
                if is_nested_array and parent_array and parent_array in array_natural_keys:
                    # Use the parent array's natural key
                    parent_key_path = array_natural_keys[parent_array]
                    parent_field_name = parent_key_path.replace(parent_array + ".", "")
                    parent_key_column = get_column_name(parent_field_name)
                    parent_table = f"{root_table_name}_{get_column_name(parent_array)}"
                    
                    # Look up the field info in the parent array table
                    parent_field_info = next((f for f in array_tables[parent_array] if isinstance(f, tuple) and len(f) >= 4 and f[0] == parent_key_path), None)
                    
                    # Add foreign key column referencing the parent array's key
                    if parent_field_info:
                        hive_type = avro_to_hive_type(parent_field_info)
                        array_ddl += f"  {parent_key_column} {hive_type} COMMENT 'Foreign key to {parent_table}',\n"
                    else:
                        array_ddl += f"  {parent_key_column} VARCHAR(255) COMMENT 'Foreign key to {parent_table}',\n"
                
                # If not a nested array or if we couldn't determine the parent properly, check for explicit relationships
                elif array_path in parent_child_relations:
                    relation = parent_child_relations[array_path]
                    if relation["choice"] == "Parent array's key" and relation["parent"] in array_natural_keys:
                        # Use explicitly defined parent
                        parent_array = relation["parent"]
                        parent_key_path = array_natural_keys[parent_array]
                        parent_field_name = parent_key_path.replace(parent_array + ".", "")
                        parent_key_column = get_column_name(parent_field_name)
                        parent_table = f"{root_table_name}_{get_column_name(parent_array)}"
                        
                        # Look up the field info in the parent array table
                        parent_field_info = next((f for f in array_tables[parent_array] if isinstance(f, tuple) and len(f) >= 4 and f[0] == parent_key_path), None)
                        
                        # Add foreign key column referencing the parent array's key
                        if parent_field_info:
                            hive_type = avro_to_hive_type(parent_field_info)
                            array_ddl += f"  {parent_key_column} {hive_type} COMMENT 'Foreign key to {parent_table}',\n"
                        else:
                            array_ddl += f"  {parent_key_column} VARCHAR(255) COMMENT 'Foreign key to {parent_table}',\n"
                    else:
                        # Default to root table's key
                        parent_key_column = natural_key_col_name
                        parent_key_path = natural_key
                        parent_table = root_table_name
                        
                        # Add foreign key to root table
                        parent_field_info = next((f for f in root_fields if isinstance(f, tuple) and len(f) >= 4 and f[0] == parent_key_path), None)
                        if parent_field_info:
                            hive_type = avro_to_hive_type(parent_field_info)
                            array_ddl += f"  {parent_key_column} {hive_type} COMMENT 'Foreign key to {parent_table}',\n"
                        else:
                            array_ddl += f"  {parent_key_column} VARCHAR(255) COMMENT 'Foreign key to {parent_table}',\n"
                else:
                    # Default to root table's key if no relationship specified
                    parent_key_column = natural_key_col_name
                    parent_key_path = natural_key
                    parent_table = root_table_name
                    
                    # Add foreign key to root table
                    parent_field_info = next((f for f in root_fields if isinstance(f, tuple) and len(f) >= 4 and f[0] == parent_key_path), None)
                    if parent_field_info:
                        hive_type = avro_to_hive_type(parent_field_info)
                        array_ddl += f"  {parent_key_column} {hive_type} COMMENT 'Foreign key to {parent_table}',\n"
                    else:
                        array_ddl += f"  {parent_key_column} VARCHAR(255) COMMENT 'Foreign key to {parent_table}',\n"
                
                # Add natural key for this array if defined
                if array_path in array_natural_keys:
                    array_key_path = array_natural_keys[array_path]
                    array_key_info = next((f for f in fields if isinstance(f, tuple) and len(f) >= 4 and f[0] == array_key_path), None)
                    if array_key_info:
                        array_key_col = get_column_name(array_key_path.replace(array_path + ".", ""))
                        array_ddl += f"  {array_key_col} {avro_to_hive_type(array_key_info)} COMMENT 'Natural key for this array',\n"
                
                # Add fields specific to this array
                for field_info in fields:
                    if isinstance(field_info, tuple) and len(field_info) >= 4:
                        path, field_type, doc, optional = field_info[:4]
                        
                        # Skip fields that were already added as keys
                        if array_path in array_natural_keys and path == array_natural_keys[array_path]:
                            continue
                            
                        # Extract just the field name part from the full path
                        field_path = path.replace(array_path + ".", "")
                        column_name = get_column_name(field_path)
                        
                        # For array fields, include them as JSON string
                        if field_type == 'array':
                            array_ddl += f"  {column_name}_json STRING"
                            if doc:
                                array_ddl += f" COMMENT 'JSON array: {doc}'"
                            array_ddl += ",\n"
                        # For record fields not flattened, include them as JSON string
                        elif field_type == 'record':
                            array_ddl += f"  {column_name}_json STRING"
                            if doc:
                                array_ddl += f" COMMENT 'JSON object: {doc}'"
                            array_ddl += ",\n"
                        else:
                            # Normal fields
                            hive_type = avro_to_hive_type(field_info)
                            
                            # Add column
                            array_ddl += f"  {column_name} {hive_type}"
                            if doc:
                                array_ddl += f" COMMENT '{doc}'"
                            array_ddl += ",\n"
                
                # Remove trailing comma
                if array_ddl.endswith(",\n"):
                    array_ddl = array_ddl[:-2]
                
                array_ddl += "\n)"
                
                array_ddls[table_name] = array_ddl
            
            # Display the generated DDLs
            st.markdown("### Root Table DDL")
            st.markdown(f"<div class='ddl-code'>{root_ddl}</div>", unsafe_allow_html=True)
            
            st.markdown("### Array Tables DDL")
            for table_name, ddl in array_ddls.items():
                st.markdown(f"#### {table_name}")
                st.markdown(f"<div class='ddl-code'>{ddl}</div>", unsafe_allow_html=True)

            # Add table relationship visualization
            st.markdown("### Table Relationships")
            st.markdown("""
            <style>
            .table-tree {
                font-family: monospace;
                margin: 20px 0;
                line-height: 1.5em;
            }
            .table-node {
                padding-left: 20px;
                position: relative;
            }
            .table-root {
                font-weight: bold;
                color: #2196F3;
            }
            .table-child {
                position: relative;
            }
            .table-child:before {
                content: "├── ";
                color: #666;
            }
            .table-child:last-child:before {
                content: "└── ";
                color: #666;
            }
            .fk-relation {
                font-size: 0.9em;
                color: #888;
                font-style: italic;
            }
            </style>
            """, unsafe_allow_html=True)

            # Create a hierarchical structure
            st.markdown("<div class='table-tree'>", unsafe_allow_html=True)
            st.markdown(f"<div class='table-root'>{root_table_name}</div>", unsafe_allow_html=True)

            # Group tables by their level in the hierarchy
            table_hierarchy = {"root": [], "level1": [], "level2": [], "other": []}

            # Find direct children of root table
            for array_path, fields in array_tables.items():
                table_name = f"{root_table_name}_{get_column_name(array_path)}"
                
                if array_path in parent_child_relations:
                    relation = parent_child_relations[array_path]
                    parent = relation["parent"]
                    choice = relation["choice"]
                    
                    if choice == "Root table's key" or parent == "root":
                        # This is a direct child of root
                        table_hierarchy["level1"].append({
                            "path": array_path,
                            "table": table_name,
                            "rel_type": "Direct child of root",
                            "fk": f"{natural_key_col_name} → {root_table_name}.{natural_key_col_name}"
                        })
                    else:
                        # This is a child of another array
                        parent_table = f"{root_table_name}_{get_column_name(parent)}"
                        parent_key = array_natural_keys.get(parent, "unknown")
                        parent_key_col = get_column_name(parent_key.replace(parent + ".", ""))
                        table_hierarchy["level2"].append({
                            "path": array_path,
                            "table": table_name,
                            "parent_path": parent,
                            "parent_table": parent_table,
                            "rel_type": "Child of another array",
                            "fk": f"{parent_key_col} → {parent_table}.{parent_key_col}"
                        })
                else:
                    # Check if this is a nested array
                    is_nested = False
                    direct_parent = None
                    for potential_parent in array_tables.keys():
                        if array_path.startswith(potential_parent + "[].item") and array_path != potential_parent:
                            is_nested = True
                            if not direct_parent or len(potential_parent) > len(direct_parent):
                                direct_parent = potential_parent
                    
                    if is_nested and direct_parent:
                        # This is a nested array with an implied parent
                        parent_table = f"{root_table_name}_{get_column_name(direct_parent)}"
                        parent_key = array_natural_keys.get(direct_parent, "unknown")
                        parent_key_col = get_column_name(parent_key.replace(direct_parent + ".", ""))
                        
                        table_hierarchy["level2"].append({
                            "path": array_path,
                            "table": table_name,
                            "parent_path": direct_parent,
                            "parent_table": parent_table,
                            "rel_type": "Nested array (implicit)",
                            "fk": f"{parent_key_col} → {parent_table}.{parent_key_col}"
                        })
                    else:
                        # Direct child of root by default
                        table_hierarchy["level1"].append({
                            "path": array_path,
                            "table": table_name,
                            "rel_type": "Default child of root",
                            "fk": f"{natural_key_col_name} → {root_table_name}.{natural_key_col_name}"
                        })

            # Render the hierarchy
            for table in table_hierarchy["level1"]:
                st.markdown(f"<div class='table-node'><div class='table-child'>{table['table']} <span class='fk-relation'>({table['fk']})</span></div>", unsafe_allow_html=True)
                
                # Find children of this table
                children = [t for t in table_hierarchy["level2"] if t.get("parent_path") == table["path"]]
                
                if children:
                    for child in children:
                        st.markdown(f"<div class='table-node' style='margin-left: 40px;'><div class='table-child'>{child['table']} <span class='fk-relation'>({child['fk']})</span></div></div>", unsafe_allow_html=True)
                
                st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)        
else:
    st.info("Please upload an Avro schema file to begin analysis.")
    
    # Display sample instructions
    st.markdown("""
    ## How to use this tool
    
    1. Upload your Avro schema file (.avsc) using the uploader in the sidebar
    2. The schema structure will be displayed in an easy-to-read format
    3. Use the checkboxes in the sidebar to flatten array fields as needed
    4. Toggle field types display for more detailed information
    5. Go to the DDL Generator tab to create Hive tables and SQL queries
    6. Select natural keys to establish parent-child relationships
    """)