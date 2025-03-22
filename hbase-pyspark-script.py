def create_table(connection, table_name, column_families):
    """Create a new HBase table with specified column families
    
    Args:
        connection: HappyBase connection object
        table_name: Name of the table to create
        column_families: List of column family names or dictionary with options
    """
    # Convert string list to dictionary for proper column family creation
    if isinstance(column_families, list):
        cf_dict = {cf: dict() for cf in column_families}
    else:
        cf_dict = column_families
    
    # Check if table exists before trying to create it
    tables = [t.decode('utf-8') for t in connection.tables()]
    if table_name in tables:
        print(f"Table '{table_name}' already exists. Using existing table.")
        return
        
    try:
        connection.create_table(table_name, cf_dict)
        print(f"Table '{table_name}' created successfully")
    except Exception as e:
        print(f"Error creating table: {e}") 