from snowflake_utils.analyticsdb_io import AnalyticsDBConnector
import pandas as pd


dfs = pd.read_csv(
    '/Users/dushan.wijesinghe/Documents/DatagripProjects/analysts-airflow/Nav Term Data - For Dushan (1).csv', sep = ';',
    error_bad_lines=False)

db_io = AnalyticsDBConnector(schema_name='reports')
db_io.use_local_connection('dushan.wijesinghe@transferwise.com')


schema = 'SANDBOX_DB.SANDBOX_DUSHAN_WIJESINGHE'

file_format = schema + '.' + 'cs_semicolon_csv'
csv_format = {"encoding": "utf-8", "sep": ";"}
kwargs = {"stage_format": file_format, "file_format": csv_format}

missing_employee_list_table_name = 'missing_employee_list'
missing_employee_list_full_table_name = schema + '.' + missing_employee_list_table_name

missing_employee_list_sheet = dfs.rename(
    columns={'No': 'wiser_id', 'Job Title (English)': 'job_title', 'Employment Date': 'employment_date',
             'Termination Date': 'termination_date', 'Manager Name': 'manager_name',
             'Employee Group Name': 'employee_group_name', 'Employee Subgroup Name': 'employee_subgroup_name',
             'Company E-Mail': 'company_email'})

missing_employee_list = missing_employee_list_sheet[
    ['wiser_id', 'Name', 'job_title', 'Status', 'employment_date', 'termination_date', 'FTE', 'manager_name',
     'employee_group_name', 'employee_subgroup_name', 'company_email', 'Country']]

db_io.write(df=missing_employee_list,
            table=missing_employee_list_full_table_name,
            if_exists='append',
            index=False,
            clean_data=False, **kwargs)
