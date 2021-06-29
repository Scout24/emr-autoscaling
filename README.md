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

This project is built using Make. To setup your build
environment simply do the following:

```bash
make setup-environment
```

To perform a build, i.e. execute unit tests and package the zip file for AWS Lambda:

```bash
make package
```

# Deployment to AWS
Committing changes triggers a Jenkins build

[Link to the Jenkins build](https://core-data-platform.fizz.cloud.scout24.com/job/Data%20Platform%20Tools/job/EMR%20Auto%20Scaling/)


# Parameters

The Function takes 2 sets of parameters:

## Mandatory Parameters

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