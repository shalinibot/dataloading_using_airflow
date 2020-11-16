import boto3

class AWSParameterStoreHelper:


        def __init__(self, aws_env=None):

                # Pulling parameters from AWS environment
                self.aws_session = boto3.Session(profile_name=aws_account)

        @staticmethod
        def get_parameters(region, param_names):
                ssm = boto3.client('ssm', region)
                response = ssm.get_parameters(Names=param_names, WithDecryption=True)
                value_dictionary = {}
                for parameter in response['Parameters']:
                    value_dictionary[parameter['Name']] = parameter
                return value_dictionary

        def get_parameter_values(self, parameter_list):
                if type(parameter_list) == list:
                        aws_ssm = self.aws_session.client('ssm', 'us-east-1')
                        aws_parameters = aws_ssm.get_parameters(Names=parameter_list, WithDecryption=True)
                        if len(aws_parameters['IncorrentParameters']) > 0:
                                print(">>> ERROR: ({}) Incorrect Parameter(s) name given: {}".format(__name__,aws_parameters['IncorrectParameters']))
                                return {}
                        else:
                                parameters = {}
                                for p in aws_parameters['Parameters']:
                                        parameters[p['Name']] = p['Value']
                                return parameters

                else:
                        print(">>> ERROR: ({}) Need to provide parameters in a list format.".format(__name__))
                        return {}