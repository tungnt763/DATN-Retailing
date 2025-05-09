from airflow.decorators import task

@task.external_python(python='/opt/airflow/soda_venv/bin/python')
def data_quality_check(table_name, dataset):
    from soda.scan import Scan

    scan_name=f'data_quality_check_{table_name}'
    data_source=f'retail_{dataset}'

    print('Running Soda Scan ...')
    config_file = f'include/soda/{dataset}/configuration.yml'
    checks_path = f'include/soda/{dataset}/checks/sources/check_{table_name}.yml'

    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name(data_source)
    scan.add_sodacl_yaml_files(checks_path)
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()
    print(scan.get_logs_text())

    if result != 0:
        raise ValueError('Soda Scan failed')

    return result