# flake8: noqa
# Import necessary libraries
import humanize  # Used for human-readable time formatting
from datetime import timedelta


def print_pipeline_metrics(pipeline) -> None:
    """
    Print detailed metrics about the pipeline execution.
    
    This function extracts timing information from a DLT pipeline's trace objects
    and prints it in a human-readable format, including:
    - Overall runtime
    - Extract stage timing and table information
    - Normalize stage timing and row counts
    - Load stage timing and loaded tables
    
    Args:
        pipeline: A DLT pipeline object with a completed execution
    """
    # Print detailed metrics about the pipeline execution
    print('\n' + '='*80)
    print('*****OVERALL PIPELINE SUMMARY*****')
    
    # Total pipeline duration
    if hasattr(pipeline.last_trace, 'started_at') and hasattr(pipeline.last_trace, 'finished_at'):
        total_duration = pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        print(f"Total Runtime: {humanize.precisedelta(total_duration)}")
    
    print('='*80)
    
    # Extract stage information
    print('\n' + '='*80)
    print('*****EXTRACT INFO*****')
    
    if hasattr(pipeline.last_trace, 'last_extract_info') and pipeline.last_trace.last_extract_info is not None:
        extract_info = pipeline.last_trace.last_extract_info
        
        # Calculate extract duration
        if hasattr(extract_info, 'started_at') and hasattr(extract_info, 'finished_at'):
            extract_duration = extract_info.finished_at - extract_info.started_at
            print(f"Extract Duration: {humanize.precisedelta(extract_duration)}")
        
        # Show table-specific info if available
        if hasattr(extract_info, 'extract_data_info') and extract_info.extract_data_info:
            print(f"\nExtracted {len(extract_info.extract_data_info)} tables:")
            for table_info in extract_info.extract_data_info:
                # Try to access common properties that might be in the table_info object
                table_name = getattr(table_info, 'name', getattr(table_info, 'table_name', 
                                     getattr(table_info, 'destination_name', 'Unknown')))
                row_count = getattr(table_info, 'row_count', getattr(table_info, 'rows', 
                                    getattr(table_info, 'count', 'Unknown')))
                print(f"  {table_name}: {row_count} rows")
        
        # If no table-specific info is available, try to print the load_packages info
        elif hasattr(extract_info, 'load_packages') and extract_info.load_packages:
            all_resources = []
            for package in extract_info.load_packages:
                if hasattr(package, 'keys'):
                    all_resources.extend(list(package.keys()))
            
            if all_resources:
                print(f"\nExtracted {len(all_resources)} tables:")
                for resource in all_resources:
                    print(f"  {resource}")
    else:
        print("No extract information available")
    
    print('='*80)
    
    # Normalize stage information
    print('\n' + '='*80)
    print('*****NORMALIZE INFO***** ')
    
    if hasattr(pipeline.last_trace, 'last_normalize_info') and pipeline.last_trace.last_normalize_info is not None:
        normalize_info = pipeline.last_trace.last_normalize_info
        
        # Calculate normalize duration
        if hasattr(normalize_info, 'started_at') and hasattr(normalize_info, 'finished_at'):
            normalize_duration = normalize_info.finished_at - normalize_info.started_at
            print(f"Normalize Duration: {humanize.precisedelta(normalize_duration)}")
        
        # Show row counts if available
        if hasattr(normalize_info, 'row_counts') and normalize_info.row_counts:
            print(f"\nNormalized {len(normalize_info.row_counts)} tables:")
            for table_name, count in normalize_info.row_counts.items():
                print(f"  {table_name}: {count:,} rows")
    else:
        print("No normalize information available")
    
    print('='*80)
    
    # Load stage information
    print('\n' + '='*80)
    print('*****LOAD INFO*****')
    
    if hasattr(pipeline.last_trace, 'last_load_info') and pipeline.last_trace.last_load_info is not None:
        load_info = pipeline.last_trace.last_load_info
        
        # Calculate load duration
        if hasattr(load_info, 'started_at') and hasattr(load_info, 'finished_at'):
            load_duration = load_info.finished_at - load_info.started_at
            print(f"Load Duration: {humanize.precisedelta(load_duration)}")
        
        # Show destination information
        if hasattr(load_info, 'destination_type'):
            print(f"\nDestination: {load_info.destination_type}")
        if hasattr(load_info, 'dataset_name'):
            print(f"Dataset: {load_info.dataset_name}")
        
        # Try to extract loaded table names from metrics
        loaded_tables = set()
        if hasattr(load_info, 'metrics') and load_info.metrics:
            for load_id, metrics_list in load_info.metrics.items():
                for metrics_item in metrics_list:
                    if 'job_metrics' in metrics_item:
                        for job_id in metrics_item['job_metrics'].keys():
                            # Extract table name from job_id (usually in format: table_name.hash.parquet)
                            table_name = job_id.split('.')[0] if '.' in job_id else job_id
                            loaded_tables.add(table_name)
        
        if loaded_tables:
            print(f"\nLoaded {len(loaded_tables)} tables:")
            for table in sorted(loaded_tables):
                print(f"  {table}")
        
        # Show if any jobs failed
        if hasattr(load_info, 'has_failed_jobs'):
            print(f"\nFailed Jobs: {'Yes' if load_info.has_failed_jobs else 'No'}")
    else:
        print("No load information available")
    
    print('='*80)
