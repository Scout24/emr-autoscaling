from datetime import datetime, timedelta

from emr_autoscaling.emr import Emr
from mock import patch
from unittest import TestCase

class EmrTest(TestCase):

    def setUp(self):
        self.cw_metric_avg = "MyTestMetricAvg"
        self.cw_metric_container_pending = "ContainerPending"
        self.job_flow = "myJobFlow"
        self.emr_task_instance_group_id = "MyTaskInstanceGroupId"
        self.emr_task_instance_group_name = "MyTaskInstanceGroupName"
        self.emr_task_instance_group_instance_type = "MyTaskInstanceGroupInstanceType"

    @patch("emr_autoscaling.emr.boto3.client")
    def test_returns_average_of_last_hour(self, mock_cw):
        mock_stats = mock_cw.return_value.get_metric_statistics
        mock_stats.return_value = {
            "Datapoints": [
                {
                    "Average": 42.0
                }
            ]
        }
        avg = Emr(job_flow_id = self.job_flow, region = "eu-west-1").get_average_of_last_hour(self.cw_metric_avg)
        now = datetime.utcnow().replace(second = 0, microsecond = 0)
        self.assertEqual(avg, 42.0)
        mock_stats.assert_called_with (
            Namespace = "AWS/ElasticMapReduce",
            MetricName = self.cw_metric_avg,
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
                    "Value": self.job_flow
                }
            ]
        )

    @patch("emr_autoscaling.emr.boto3.client")
    def test_gets_pending_containers(self, mock_cw):
        mock_stats = mock_cw.return_value.get_metric_statistics
        mock_stats.return_value = {
            "Datapoints": [
                {
                    "Maximum": 42
                }
            ]
        }
        max = Emr(job_flow_id = self.job_flow, region = "eu-west-1").get_pending_containers()
        now = datetime.utcnow().replace(second = 0, microsecond = 0)
        self.assertEqual(max, 42)
        mock_stats.assert_called_with (
            Namespace = "AWS/ElasticMapReduce",
            MetricName = self.cw_metric_container_pending,
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
                    "Value": self.job_flow
                }
            ]
        )

    @patch("emr_autoscaling.emr.boto3.client")
    def test_get_one_task_instance_group(self, mock_emr):
        mock_instance_groups = mock_emr.return_value.list_instance_groups
        mock_instance_groups.return_value = {
            "InstanceGroups": [
                {
                    "InstanceGroupType": "MASTER",
                    "InstanceGroupName": "MyMasterGroup"
                },
                {
                    "InstanceGroupType": "CORE",
                    "InstanceGroupName": "MyCoreGroup"
                },
                {
                    "InstanceGroupType": "TASK",
                    "InstanceGroupName": "MyTaskGroup",
                    "BidPrice": 1.2
                }
            ]
        }
        groups = Emr(job_flow_id = self.job_flow, region = "eu-west-1").get_task_instance_groups()
        self.assertListEqual (
            groups,
            [
                {"InstanceGroupType": "TASK", "InstanceGroupName": "MyTaskGroup", "BidPrice": 1.2}
            ]
        )
        mock_instance_groups.assert_called_with(ClusterId = self.job_flow)

    @patch("emr_autoscaling.emr.boto3.client")
    def test_get_two_task_instance_groups(self, mock_emr):
        mock_instance_groups = mock_emr.return_value.list_instance_groups
        mock_instance_groups.return_value = {
            "InstanceGroups": [
                {
                    "InstanceGroupType": "MASTER",
                    "InstanceGroupName": "MyMasterGroup"
                },
                {
                    "InstanceGroupType": "CORE",
                    "InstanceGroupName": "MyCoreGroup"
                },
                {
                    "InstanceGroupType": "TASK",
                    "InstanceGroupName": "MyTaskGroup",
                    "BidPrice": 1.2
                },
                {
                    "InstanceGroupType": "TASK",
                    "InstanceGroupName": "MyOtherTaskGroup",
                    "BidPrice": 1.2
                }
            ]
        }
        groups = Emr(job_flow_id = self.job_flow, region = "eu-west-1").get_task_instance_groups()
        self.assertListEqual (
            groups,
            [
                {"InstanceGroupType": "TASK", "InstanceGroupName": "MyTaskGroup", "BidPrice": 1.2},
                {"InstanceGroupType": "TASK", "InstanceGroupName": "MyOtherTaskGroup", "BidPrice": 1.2}
            ]
        )
        mock_instance_groups.assert_called_with(ClusterId = self.job_flow)

    @patch("emr_autoscaling.emr.boto3.client")
    def test_get_no_task_instance_group(self, mock_emr):
        mock_instance_groups = mock_emr.return_value.list_instance_groups
        mock_instance_groups.return_value = {
            "InstanceGroups": [
                {
                    "InstanceGroupType": "MASTER",
                    "InstanceGroupName": "MyMasterGroup"
                },
                {
                    "InstanceGroupType": "CORE",
                    "InstanceGroupName": "MyCoreGroup"
                }
            ]
        }
        groups = Emr(job_flow_id = self.job_flow, region = "eu-west-1").get_task_instance_groups()
        self.assertListEqual(groups, [])

    @patch("emr_autoscaling.emr.boto3.client")
    def test_dont_get_task_instance_group_without_bid_price(self, mock_emr):
        mock_instance_groups = mock_emr.return_value.list_instance_groups
        mock_instance_groups.return_value = {
            "InstanceGroups": [
                {
                    "InstanceGroupType": "MASTER",
                    "InstanceGroupName": "MyMasterGroup"
                },
                {
                    "InstanceGroupType": "CORE",
                    "InstanceGroupName": "MyCoreGroup"
                },
                {
                    "InstanceGroupType": "TASK",
                    "InstanceGroupName": "MyTaskGroup"
                }
            ]
        }
        groups = Emr(job_flow_id = self.job_flow, region = "eu-west-1").get_task_instance_groups()
        self.assertListEqual(groups, [])

    @patch("emr_autoscaling.emr.Emr.get_task_instance_groups")
    def test_upscaling_in_progress(self, mock_get_task_instance_group):
        mock_get_task_instance_group.return_value = [
            {
                "RequestedInstanceCount": 2,
                "RunningInstanceCount": 1
            }
        ]
        self.assertTrue (
            Emr (
                job_flow_id = self.job_flow,
                region = "eu-west-1"
            ).scaling_in_progress()
        )

    @patch("emr_autoscaling.emr.Emr.get_task_instance_groups")
    def test_downscaling_in_progress(self, mock_get_task_instance_group):
        mock_get_task_instance_group.return_value = [
            {
                "RequestedInstanceCount": 1,
                "RunningInstanceCount": 2
            }
        ]
        self.assertTrue (
            Emr (
                job_flow_id = self.job_flow,
                region = "eu-west-1"
            ).scaling_in_progress()
        )

    @patch("emr_autoscaling.emr.Emr.get_task_instance_groups")
    def test_no_scaling_in_progress(self, mock_get_task_instance_group):
        mock_get_task_instance_group.return_value = [
            {
                "RequestedInstanceCount": 1,
                "RunningInstanceCount": 1
            }
        ]
        self.assertFalse (
            Emr (
                job_flow_id = self.job_flow,
                region = "eu-west-1"
            ).scaling_in_progress()
        )

    @patch("emr_autoscaling.emr.boto3.client")
    @patch("emr_autoscaling.emr.Emr.get_task_instance_groups")
    def test_scaling_because_inner_bounds(self, mock_get_task_instance_group, mock_emr):
        mock_get_task_instance_group.return_value = [
            {
                "Id": self.emr_task_instance_group_id,
                "RequestedInstanceCount": 5,
                "BidPrice": 1.2,
                "Name": self.emr_task_instance_group_name,
                "InstanceType": self.emr_task_instance_group_instance_type
            }
        ]
        mock_modify_instance_groups = mock_emr.return_value.modify_instance_groups
        Emr (
            job_flow_id = self.job_flow,
            min_instances = 5,
            max_instances = 10,
            region = "eu-west-1"
        ).scale (
            direction = 1
        )
        mock_modify_instance_groups.assert_called_with (
            InstanceGroups = [
                {
                    "InstanceGroupId": self.emr_task_instance_group_id,
                    "InstanceCount": 6
                }
            ]
        )

    @patch("emr_autoscaling.emr.boto3.client")
    @patch("emr_autoscaling.emr.Emr.get_task_instance_groups")
    def test_no_scaling_because_above_upper_bound(self, mock_get_task_instance_group, mock_emr):
        mock_get_task_instance_group.return_value = [
            {
                "Id": self.emr_task_instance_group_id,
                "RequestedInstanceCount": 10,
                "BidPrice": 1.2,
                "Name": self.emr_task_instance_group_name,
                "InstanceType": self.emr_task_instance_group_instance_type
            }
        ]
        mock_modify_instance_groups = mock_emr.return_value.modify_instance_groups
        Emr (
            job_flow_id = self.job_flow,
            min_instances = 5,
            max_instances = 10,
            region = "eu-west-1"
        ).scale (
            direction = 1
        )
        mock_modify_instance_groups.assert_not_called()

    @patch("emr_autoscaling.emr.boto3.client")
    @patch("emr_autoscaling.emr.Emr.get_task_instance_groups")
    def test_no_scaling_because_below_lower_bound(self, mock_get_task_instance_group, mock_emr):
        mock_get_task_instance_group.return_value = [
            {
                "Id": self.emr_task_instance_group_id,
                "RequestedInstanceCount": 5,
                "BidPrice": 1.2,
                "Name": self.emr_task_instance_group_name,
                "InstanceType": self.emr_task_instance_group_instance_type
            }
        ]
        mock_modify_instance_groups = mock_emr.return_value.modify_instance_groups
        Emr (
            job_flow_id = self.job_flow,
            min_instances = 5,
            max_instances = 10,
            region = "eu-west-1"
        ).scale (
            direction = -1
        )
        mock_modify_instance_groups.assert_not_called()

    @patch("emr_autoscaling.emr.boto3.client")
    @patch("emr_autoscaling.emr.Emr.get_task_instance_groups")
    def test_scales_only_once(self, mock_get_task_instance_group, mock_emr):
        mock_get_task_instance_group.return_value = [
            {
                "Id": self.emr_task_instance_group_id,
                "RequestedInstanceCount": 5,
                "BidPrice": 1.2,
                "Name": self.emr_task_instance_group_name,
                "InstanceType": self.emr_task_instance_group_instance_type
            },
            {
                "Id": self.emr_task_instance_group_id + "_expensive",
                "RequestedInstanceCount": 5,
                "BidPrice": 1.5,
                "Name": self.emr_task_instance_group_name,
                "InstanceType": self.emr_task_instance_group_instance_type
            }
        ]
        mock_modify_instance_groups = mock_emr.return_value.modify_instance_groups
        Emr (
            job_flow_id = self.job_flow,
            min_instances = 5,
            max_instances = 10,
            region = "eu-west-1"
        ).scale (
            direction = 1
        )
        mock_modify_instance_groups.assert_called_once_with (
            InstanceGroups = [
                {
                    "InstanceGroupId": self.emr_task_instance_group_id + "_expensive",
                    "InstanceCount": 6
                }
            ]
        )
    @patch("emr_autoscaling.emr.boto3.client")
    def test_is_termination_protected_True(self, mock_emr):
        mock_describe_cluster = mock_emr.return_value.describe_cluster
        mock_describe_cluster.return_value = {
            "Cluster": {
                "TerminationProtected": True
            }
        }
        self.assertTrue(Emr(job_flow_id=self.job_flow).is_termination_protected())

    @patch("emr_autoscaling.emr.boto3.client")
    def test_is_termination_protected_False(self, mock_emr):
        mock_describe_cluster = mock_emr.return_value.describe_cluster
        mock_describe_cluster.return_value = {
            "Cluster": {
                "TerminationProtected": False
            }
        }
        self.assertFalse(Emr(job_flow_id=self.job_flow).is_termination_protected())