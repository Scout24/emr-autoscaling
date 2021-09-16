import boto3
from datetime import datetime, timedelta

from emr_autoscaling.constants import UP
from emr_autoscaling.utils import get_logger

import math


class Emr:

    def __init__(self, job_flow_id, min_instances = 0, max_instances = 20, region = None):
        self.min_instances = min_instances
        self.max_instances = max_instances
        self.job_flow_id = job_flow_id
        self.logger = get_logger('EMR')
        self.logger.info("Changing to reflect in lambda")
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
        instance_groups = self.emr.list_instance_groups(ClusterId=self.job_flow_id)["InstanceGroups"]
        return [g for g in instance_groups if "BidPrice" in g and g["InstanceGroupType"] == "TASK"]

    def scaling_in_progress(self):
        groups = self.get_task_instance_groups()
        for group in groups:
            if group["RequestedInstanceCount"] != group["RunningInstanceCount"]:
                return True

        return False

    def is_termination_protected(self):
        termination_protected = self.emr.describe_cluster(ClusterId=self.job_flow_id)["Cluster"]["TerminationProtected"]
        self.logger.info("Is cluster %s termination protected? %s" % (self.job_flow_id, termination_protected))
        return termination_protected

    @staticmethod
    def calculate_new_instance_count(current_instance_count, direction):
        if current_instance_count == 0 and direction == UP:
            return 1

        roundfunc = math.ceil if direction == UP else math.floor
        return int(current_instance_count + roundfunc(direction * 0.2 * current_instance_count))

    @staticmethod
    def is_target_count_not_reached(current_requested_instances, target_requested_instances):
        return current_requested_instances != target_requested_instances

    def scale(self, direction):
        groups = sorted (
            self.get_task_instance_groups(),
            key=lambda g: g["BidPrice"],
            reverse=True
        )
        for group in groups:
            current_requested_instances = group['RequestedInstanceCount']
            target_requested_instances = self.calculate_new_instance_count(group['RequestedInstanceCount'], direction)

            if self.is_target_count_not_reached(current_requested_instances, target_requested_instances) \
                    and self.min_instances <= target_requested_instances <= self.max_instances:
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

            self.logger.info(
                "[{}   --   {}] New number of task instances is {}, out of bounds of ({}-{})".format (
                    group.get("Name", "dummy group name"),
                    group.get("InstanceType", "dummy instance type"),
                    target_requested_instances,
                    self.min_instances,
                    self.max_instances
                )
            )
