import boto3


class TestClusterAndStack:

    def __init__(self):
        pass

    @staticmethod
    def create_emr_test_cluster():
        client = boto3.client('emr')
        response = \
            client.run_job_flow(
                Name='ScalingTest',
                LogUri='s3://aws-logs-706008486279-eu-central-1',
                ReleaseLabel='emr-4.3.0',
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': 'MasterInstanceGroup',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm3.xlarge',
                            'InstanceCount': 1
                        }, {
                            'Name': 'CoreInstanceGroup',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'm3.xlarge',
                            'InstanceCount': 1
                        }, {
                            'Name': 'TaskInstanceGroup',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'TASK',
                            'InstanceType': 'm3.xlarge',
                            'InstanceCount': 1
                        }
                    ],
                    'Ec2KeyName': 'data-engineering-dev',
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': True,
                    'Ec2SubnetId': 'subnet-40128b37'
                },
                Applications=[
                    {
                        'Name': 'Spark',
                    },
                    {
                        'Name': 'Hadoop',
                    }
                ],
                Tags=[
                    {
                        'Key': 'usecase',
                        'Value': 'emr-autoscaling'
                    }
                ],
                JobFlowRole='EMR_EC2_DefaultRole',
                ServiceRole='EMR_DefaultRole'
            )

        return response['JobFlowId']


if __name__ == '__main__':
    print(TestClusterAndStack.create_emr_test_cluster())
