import boto3
from datetime import datetime, timedelta
import logging

import math


class Emr:

    def __init__(self, job_flow_id, min_instances = 0, max_instances = 20, region = None):
        self.min_instances = min_instances
        self.max_instances = max_instances
        self.job_flow_id = job_flow_id
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        if region:
            self.emr = boto3.client("emr", region_name = region)
            self.cloudwatch = boto3.client("cloudwatch", region_name = region)
        else:
            self.emr = boto3.client("emr")
            self.cloudwatch = boto3.client("cloudwatch")

    def get_average_of_last_hour(self, metric_name):
        now = datetime.utcnow().replace(second = 0, microsecond = 0)
        stats = self.cloudwatch.get_metric_statistics (
            Namespace = "AWS/ElasticMapReduce",
            MetricName = metric_name,
            StartTime = now - timedelta(hours = 1),
            EndTime = now,
            Period = 3600,
            Statistics = [
                "Average",
            ],
            Unit = "Count",
            Dimensions = [
                {
                    "Name": "JobFlowId",
                    "Value": self.job_flow_id
                }
            ]
        )
        average = stats["Datapoints"][0]["Average"]
        return average

    def get_pending_containers(self):
        now = datetime.utcnow().replace(second = 0, microsecond = 0)
        stats = self.cloudwatch.get_metric_statistics (
            Namespace = "AWS/ElasticMapReduce",
            MetricName = "ContainerPending",
            StartTime = now - timedelta(minutes = 5),
            EndTime = now,
            Period = 300,
            Statistics = [
                "Maximum",
            ],
            Unit = "Count",
            Dimensions = [
                {
                    "Name": "JobFlowId",
                    "Value": self.job_flow_id
                }
            ]
        )
        return stats["Datapoints"][0]["Maximum"]

    def get_task_instance_groups(self):
        instance_groups = self.emr.list_instance_groups(ClusterId = self.job_flow_id)["InstanceGroups"]
        return filter (
            lambda g: g.has_key("BidPrice") and g["InstanceGroupType"] == "TASK",
            instance_groups
        )

    def scaling_in_progress(self):
        groups = self.get_task_instance_groups()
        for group in groups:
            if group["RequestedInstanceCount"] != group["RunningInstanceCount"]:
                return True
        return False

    def scale(self, direction):
        groups = sorted (
            self.get_task_instance_groups(),
            key = lambda g: g["BidPrice"],
            reverse = True
        )
        for group in groups:
            target_requested_instances = group["RequestedInstanceCount"] + math.ceil(direction * 0.2 * group["RequestedInstanceCount"])
            if self.min_instances <= target_requested_instances <= self.max_instances:
                self.emr.modify_instance_groups (
                    InstanceGroups = [
                        {
                            "InstanceGroupId": group["Id"],
                            "InstanceCount": target_requested_instances
                        }
                    ]
                )
                self.logger.info (
                    "[{}   --   {}] New number of task instances is {}.".format (
                        group["Name"],
                        group["InstanceType"],
                        target_requested_instances
                    )
                )
                break
            else:
                self.logger.info (
                    "[{}   --   {}] New number of task instances is {}, out of bounds of ({}-{})".format (
                        group["Name"],
                        group["InstanceType"],
                        target_requested_instances,
                        self.min_instances,
                        self.max_instances
                    )
                )
