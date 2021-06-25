from datetime import datetime, timezone

from src.python.emr_autoscaling.emr import Emr
from src.python.emr_autoscaling.scaler import EmrScaler
from mock import patch
from unittest import TestCase


MODULE_BASE = "src.python.emr_autoscaling"


class EmrScalerTest(TestCase):

    def setUp(self):
        self.threshold = 0.7
        self.emr = Emr(job_flow_id="myJobFlow", region="eu-west-1")

    def test_before_3_pm_is_not_in_office_hours(self):
        self.assertFalse(
            EmrScaler(
                self.emr,
                office_hours_start=15,
                office_hours_end=22
            ).is_in_office_hours(
                datetime(2016, 5, 4, 14, 59, 59)
            )
        )

    def test_before_7_am_is_not_in_office_hours(self):
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 4, 6, 59, 59)))

    def test_at_7_am_is_in_office_hours(self):
        self.assertTrue(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 4, 7)))

    def test_at_3_pm_is_in_office_hours(self):
        self.assertTrue(
            EmrScaler(
                self.emr,
                office_hours_start=15,
                office_hours_end=22
            ).is_in_office_hours(
                datetime(2016, 5, 4, 15)
            )
        )

    def test_after_6_pm_is_not_in_office_hours(self):
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 4, 18, 0, 1)))

    def test_after_10_pm_is_not_in_office_hours(self):
        self.assertFalse(
            EmrScaler(
                self.emr,
                office_hours_start=15,
                office_hours_end=22
            ).is_in_office_hours(
                datetime(2016, 5, 4, 22, 0, 1)
            )
        )

    def test_at_6_pm_is_in_office_hours(self):
        self.assertTrue(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 4, 18)))

    def test_at_10_pm_is_in_office_hours(self):
        self.assertTrue(
            EmrScaler(
                self.emr,
                office_hours_start=15,
                office_hours_end=22
            ).is_in_office_hours(
                datetime(2016, 5, 4, 22)
            )
        )

    def test_at_1_pm_is_in_office_hours(self):
        self.assertTrue(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 4, 13)))

    def test_at_7_pm_is_in_office_hours(self):
        self.assertTrue(
            EmrScaler(
                self.emr,
                office_hours_start=15,
                office_hours_end=22
            ).is_in_office_hours(
                datetime(2016, 5, 4, 19)
            )
        )

    def test_sunday_is_not_in_office_hours(self):
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 1, 6)))
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 1, 7)))
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 1, 13)))
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 1, 18)))
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 1, 19)))

    @patch(f"{MODULE_BASE}.emr.Emr.get_pending_containers")
    def test_should_scale_up_with_pending_containers(self, mock_get_containers):
        mock_get_containers.return_value = 1
        self.assertTrue(EmrScaler(self.emr).should_scale_up())

    @patch(f"{MODULE_BASE}.emr.Emr.get_pending_containers")
    def test_should_not_scale_up_without_pending_containers(self, mock_get_containers):
        mock_get_containers.return_value = 0
        self.assertFalse(EmrScaler(self.emr).should_scale_up())

    @patch(f"{MODULE_BASE}.emr.Emr.get_average_of_last_hour")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.is_in_office_hours")
    def test_should_scale_down_out_of_office_hours(self, mock_office_hours, mock_get_average):
        mock_get_average.side_effect = [60.0, 100.0]
        mock_office_hours.return_value = False
        self.assertTrue(EmrScaler(self.emr).should_scale_down(self.threshold))

    @patch(f"{MODULE_BASE}.emr.Emr.get_average_of_last_hour")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.is_in_office_hours")
    def test_should_not_scale_down_in_office_hours(self, mock_office_hours, mock_get_average):
        mock_get_average.side_effect = [60.0, 100.0]
        mock_office_hours.return_value = True
        self.assertFalse(EmrScaler(self.emr).should_scale_down(self.threshold))

    @patch(f"{MODULE_BASE}.emr.Emr.get_average_of_last_hour")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.is_in_office_hours")
    def test_should_not_scale_down_when_memory_ratio_above_threshold(self, mock_office_hours, mock_get_average):
        mock_get_average.side_effect = [80.0, 100.0]
        mock_office_hours.return_value = False
        self.assertFalse(EmrScaler(self.emr).should_scale_down(self.threshold))

    @patch(f"{MODULE_BASE}.emr.Emr.scale")
    @patch(f"{MODULE_BASE}.emr.Emr.scaling_in_progress")
    def test_maybe_dont_scale_because_in_progress(self, mock_scaling_in_progress, mock_scale):
        mock_scaling_in_progress.return_value = True
        EmrScaler(self.emr).maybe_scale(0.7)
        mock_scale.assert_not_called

    @patch(f"{MODULE_BASE}.scaler.EmrScaler.should_scale_up")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.should_scale_down")
    @patch(f"{MODULE_BASE}.emr.Emr.scale")
    @patch(f"{MODULE_BASE}.emr.Emr.scaling_in_progress")
    def test_maybe_scale_up(self, mock_scaling_in_progress, mock_scale, mock_should_scale_down, mock_should_scale_up):
        mock_scaling_in_progress.return_value = False
        mock_should_scale_down.return_value = False
        mock_should_scale_up.return_value = True
        EmrScaler(self.emr).maybe_scale(0.7)
        mock_scale.assert_called_with(1)

    @patch(f"{MODULE_BASE}.scaler.EmrScaler.should_scale_up")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.should_scale_down")
    @patch(f"{MODULE_BASE}.emr.Emr.scale")
    @patch(f"{MODULE_BASE}.emr.Emr.scaling_in_progress")
    def test_maybe_scale_down(self, mock_scaling_in_progress, mock_scale, mock_should_scale_down, mock_should_scale_up):
        mock_scaling_in_progress.return_value = False
        mock_should_scale_down.return_value = True
        mock_should_scale_up.return_value = False
        EmrScaler(self.emr).maybe_scale(0.7)
        mock_scale.assert_called_with(-1)

    @patch(f"{MODULE_BASE}.scaler.EmrScaler.should_scale_up")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.should_scale_down")
    @patch(f"{MODULE_BASE}.emr.Emr.scale")
    @patch(f"{MODULE_BASE}.emr.Emr.scaling_in_progress")
    def test_maybe_dont_scale_because_nothing_to_do(self, mock_scaling_in_progress, mock_scale, mock_should_scale_down,
                                                    mock_should_scale_up):
        mock_scaling_in_progress.return_value = False
        mock_should_scale_down.return_value = False
        mock_should_scale_up.return_value = False
        EmrScaler(self.emr).maybe_scale(0.7)
        mock_scale.assert_not_called

    @patch(f"{MODULE_BASE}.scaler.EmrScaler.shutdown")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.is_after_shutdown_time")
    def test_do_not_shutdown_if_too_early(self, mock_is_after_shutdown_time, mock_shutdown):
        mock_is_after_shutdown_time.return_value = False
        EmrScaler(self.emr).maybe_shutdown()
        mock_shutdown.assert_not_called()

    def test_is_before_shutdown_time(self):
        new_time = datetime(2019, 10, 23, 22, 59, 59)
        delta = datetime.fromtimestamp(new_time.timestamp()) - datetime.utcfromtimestamp(new_time.timestamp())

        self.assertFalse(EmrScaler(self.emr).is_after_shutdown_time(new_time - delta))

    def test_is_after_shutdown_time(self):
        new_time = datetime(2019, 10, 23, 23, 0, 1)
        delta = datetime.fromtimestamp(new_time.timestamp()) - datetime.utcfromtimestamp(new_time.timestamp())

        print(f'\n\n---------\n{EmrScaler(self.emr).shutdown_time}\n{new_time}\n{new_time - delta}')

        self.assertTrue(EmrScaler(self.emr).is_after_shutdown_time(new_time - delta))

    @patch(f"{MODULE_BASE}.emr.Emr.is_termination_protected")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.shutdown")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.is_after_shutdown_time")
    def test_do_not_shutdown_if_no_parent_stack_given(self, mock_is_after_shutdown_time, mock_shutdown,
                                                      mock_is_termination_protected):
        mock_is_after_shutdown_time.return_value = True
        mock_is_termination_protected.return_value = False
        EmrScaler(self.emr, parent_stack=None).maybe_shutdown()
        mock_shutdown.assert_not_called()

    @patch(f"{MODULE_BASE}.emr.Emr.is_termination_protected")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.shutdown")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.is_after_shutdown_time")
    def test_do_not_shutdown_if_termination_protected(self, mock_is_after_shutdown_time, mock_shutdown,
                                                      mock_is_termination_protected):
        mock_is_after_shutdown_time.return_value = True
        mock_is_termination_protected.return_value = True
        EmrScaler(self.emr, parent_stack="parent").maybe_shutdown()
        mock_shutdown.assert_not_called()

    @patch(f"{MODULE_BASE}.emr.Emr.is_termination_protected")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.shutdown")
    @patch(f"{MODULE_BASE}.scaler.EmrScaler.is_after_shutdown_time")
    def test_shutdown_all_conditions_are_met(self, mock_is_after_shutdown_time, mock_shutdown,
                                             mock_is_termination_protected):
        mock_is_after_shutdown_time.return_value = True
        mock_is_termination_protected.return_value = False
        EmrScaler(self.emr, parent_stack="parent").maybe_shutdown()
        mock_shutdown.assert_called()

    @patch(f"{MODULE_BASE}.emr.boto3.client")
    def test_shutdown_deletes_stack(self, mock_client):
        parent_stack = "parent"
        stack_deletion_role = "aws:iam:foo:bar"
        mock_cf = mock_client.return_value.delete_stack
        EmrScaler(self.emr, parent_stack=parent_stack, stack_deletion_role=stack_deletion_role).shutdown()
        mock_cf.assert_called_with(StackName=parent_stack, RoleARN=stack_deletion_role)
