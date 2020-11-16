import snowflake.connector
import sys
import json
import os
from AWSParameterStoreHelper import AWSParameterStoreHelper


class SnowflakeConnection:
    def __init__(self, config=None, warehouse='DATA_ETL', timezone='Canada/Eastern'):

        #Making sure the warehouse exists and we have permission
        self.warehouse = warehouse
        self.timezone = timezone

        config = {
                'username':'snowflake/rds/username/',
                'host':'snowflake/rds/account-name',
                'password':'snowflake/rds/pwd'
            }

        # Checking if appropriate keys are passed
        list_of_keys = list(config.keys())
        # Sorting the keys to compare
        list_of_keys.sort()
        if (list_of_keys != ['host','password','username']):
            print('Please provide a value for host, password, and username')
            exit()

        # Creating a list of parameter names
        config_parameters_list = [val for val in config.values()]

        # Feed the list to the parameter store
        aws_param = AWSParameterStoreHelper().get_parameter_values(config_parameters_list)

        if aws_param is False:
            exit()

        # Retrieving the values from the parameter store corresponding to config values , here the path will get replace with the actual value of the key
        for parameter_key, parameter_path in config.items():
            param_value = aws_param[parameter_path]
            config[parameter_key] = param_value

        self.config = config

        self.sn = snowflake.connector.connect(
            user=self.config['username'],
            password=self.config['password'],
            account=self.config['host'],
            warehouse=self.warehouse
        )
        self.cur = self.sn.cursor()

        try:
            temp = {}
            temp['sys.argv'] = list(sys.argv)
            query_tag = json.dumps(temp)
            file_path = os.path.abspath(sys.modules['__main__'].__file__)
            query = "ALTER SESSION SET QUERY_TAG = \'{0} | {1}\';".format(file_path, query_tag)
            self.execute_string(query)
        except Exception as e:
            print(e)

        if self.timezone != 'Canada/Eastern':
            print(f">>> {__name__}: Setting Session timezone to {self.timezone}.")
            self.connection.execute_string(f"alter session set timezone = '{self.timezone}'")


    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.cur.close()
        self.sn.close()

    @property
    def connection(self):
        return self.sn

    @property
    def cursor(self):
        return self.cur

    def execute_string(self, sql):
        record = self.connection.execute_string(sql)
        for record_list in record:
            for item in record_list:
                print(f">>> {__name__} : {item}")
        return record

    def query(self, sql):
        self.cursor.execute(sql)
        return self.fetchall()

    def fetchall(self):
        return self.cursor.fetchall()

    # Executes file at specified path along with eecution some dictionary driven variable replacement
    # Usage: execute_sql_file('C:/test/filename.sql', {'test':1})
    def execute_sql_file(self, file_path, variable_dictionary={}):
        with open(file_path,'r') as sqlfile:
            record = self.connection.execute_string(sqlfile.read().format(**variable_dictionary))
            for record_list in record:
                for item in record_list:
                    print(f">>> {__name__} : {item}")
            return record