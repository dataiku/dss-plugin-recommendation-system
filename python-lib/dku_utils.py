def set_column_description(output_dataset, column_descriptions):
    """Set column descriptions of the output dataset based on a dictionary of column descriptions

    Args:
        output_dataset: Output dataiku.Dataset instance
        column_descriptions: Dictionary holding column descriptions (value) by column name (key)
    """
    output_dataset_schema = output_dataset.read_schema()
    for output_col_info in output_dataset_schema:
        output_col_name = output_col_info.get("name", "")
        output_col_info["comment"] = column_descriptions.get(output_col_name)
    output_dataset.write_schema(output_dataset_schema)