# Description

Scale your AWS Elastic MapReduce Cluster by automatically adding or removing
Task Instances. Every 5 minutes an AWS Cloudwatch Rule triggers an AWS Lambda
Function which checks AWS Cloudwatch Metrics to decide whether to scale up or
down.

## Scaling Rules

Scaling is only initiated when no scaling is currently in progress. In addition
downscaling is not performed during office hours. Apart from that the following
rules are used to decide whether to scale or not.

- scaling up
    - at least 1 YARN container has been pending during the past 5 minutes
    - at least 1 task instance group is not running its maximum of configured instances
- scaling down
    - average memory consumption by YARN is below a given threshold for the last hour
    - at least 1 task instance group is running above its minimum of configured instances
    - the current time is not in office hours on a week day

# Instance Group Selection

Currently only task instance groups are eligible for scaling and only those with
a spot bid price. If the cluster has more than one task instance group it sorts
all groups by their bid price in descending order and then selects the first eligible
group for scaling.

# Build

This project is built using [PyBuilder](http://pybuilder.github.io/). To setup your build
environment simply do the following:

```bash
virtualenv -p python2.7 venv
source venv/bin/activate
pip install pybuilder
pyb install_dependencies
```

To perform a build, i.e. execute unit tests and package the zip file for AWS Lambda:

```bash
pyb -X package_lambda_code
```

# Deployment
## automatic way
### deploy changes to AWS
committing changes triggers a teamcity build

[Link to teamcity build](https://teamcity.rz.is/viewType.html?buildTypeId=DataScience_EmrAutoscaling)

### update lambda function code
```aws lambda update-function-code --function-name insights-cluster-AutoscalingStack-ScalingFunction-6YKMEWZ2YOPQ --region eu-west-1 --s3-bucket is24-data-pro-artifacts --s3-key emr/lambda_autoscaling/latest/emr-autoscaling.zip```

## semi-manual way
### Upload Lambda Function to S3

To upload the lambda Function to S3, run the following command with your S3 bucket name:

```bash
pyb -X -P bucket_name=<S3-bucket-name> upload_zip_to_s3 lambda_release
```

The `upload_zip_to_s3` part of the above command loads the zip file which has been packaged
previously into the S3 bucket as specified with the `bucket_name` parameter. The key is
`emr/lambda_autoscaling/<project-version>/emr-autoscaling.zip`. The `lambda_release` part
copies the uploaded file from `emr/lambda_autoscaling/<project-version>/emr-autoscaling.zip`
to `/emr/lambda_autoscaling/latest/emr-autoscaling.zip`.

### (Re-)Deploy Cloudformation Stack

The Cloudformation Stacks are deployed using [cfn-sphere](https://github.com/cfn-sphere/cfn-sphere).
Since you cannot update lambda functions with Cloudformation (i.e. with new code), it is
neccessary to recreate the stack.

You can delete an already deployed stack with the following statement:

```bash
cf delete -c src/main/resources/cfn/stacks.yaml
```

To deploy the stack - and thus make sure that it uses the latest version of the lambda
function - you can do the following (replace with your own parameter values):

```bash
cf sync \
  -c \
  --parameter "emr-autoscaling.scalingFunctionCodeBucket=<S3-bucket-name>" \
  --parameter "emr-autoscaling.emrJobFlowId=<EMR-cluster-id>" \
  src/main/resources/cfn/stacks.yaml
```

The function offers a few parameters to customize its behaviour. These are described
in the next section. You can override the defaults simply by adding
`--parameter "<parameter-name>=<parameter-value>"` snippets to the above command.

# Parameters

## Mandatory Parameters

- scalingFunctionCodeBucket
    - S3 Bucket into which the scaling function is uploaded
    - prefix is `/emr/lambda_autoscaling/<project-version>/emr-autoscaling.zip`
    - in addition the latest version is copied to `/emr/lambda_autoscaling/latest/emr-autoscaling.zip`
- emrJobFlowId
    - ID of the EMR cluster which is to be scaled

## Optional Parameters

- emrDownScalingMemoryAllocationThreshold
    - when the average memory consumption by YARN drops below this value a downscaling
      is triggered
    - floating point in range [0.0, 1.0]
    - defaults to 0.6
- emrScalingMinInstances
    - minimum number of instances that has to be kept for each task instance group
    - integer >= 0
    - defaults to 0
- emrScalingMaxInstances
    - maximum number of instances that is allowed for each task instance group
    - integer >= 0
    - defaults to 20
- officeHoursStart
    - begin of office hour range during which no downscaling will be initiated
    - integer between 0 and 24
    - defaults to 8
- officeHoursEnd
    - end of office hour range during which no downscaling will be initiated
    - integer between 0 and 24
    - defaults to 18