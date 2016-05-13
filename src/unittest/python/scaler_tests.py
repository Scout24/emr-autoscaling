from datetime import datetime

from emr_autoscaling.emr import Emr
from emr_autoscaling.scaler import EmrScaler
from mock import patch
from unittest import TestCase

class EmrScalerTest(TestCase):

    def setUp(self):
        self.threshold = 0.7
        self.emr = Emr(job_flow_id = "myJobFlow", region = "eu-west-1")

    def test_before_7_am_is_not_in_office_hours(self):
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 4, 6, 59, 59)))

    def test_at_7_am_is_in_office_hours(self):
        self.assertTrue(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 4, 7)))

    def test_after_6_pm_is_not_in_office_hours(self):
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 4, 18, 00, 01)))

    def test_at_6_pm_is_in_office_hours(self):
        self.assertTrue(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 4, 18)))

    def test_at_1_pm_is_in_office_hours(self):
        self.assertTrue(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 4, 13)))

    def test_sunday_is_not_in_office_hours(self):
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 1, 6)))
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 1, 7)))
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 1, 13)))
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 1, 18)))
        self.assertFalse(EmrScaler(self.emr).is_in_office_hours(datetime(2016, 5, 1, 19)))

    @patch("emr_autoscaling.emr.Emr.get_pending_containers")
    def test_should_scale_up_with_pending_containers(self, mock_get_containers):
        mock_get_containers.return_value = 1
        self.assertTrue(EmrScaler(self.emr).should_scale_up())

    @patch("emr_autoscaling.emr.Emr.get_pending_containers")
    def test_should_not_scale_up_without_pending_containers(self, mock_get_containers):
        mock_get_containers.return_value = 0
        self.assertFalse(EmrScaler(self.emr).should_scale_up())

    @patch("emr_autoscaling.emr.Emr.get_average_of_last_hour")
    @patch("emr_autoscaling.scaler.EmrScaler.is_in_office_hours")
    def test_should_scale_down_out_of_office_hours(self, mock_office_hours, mock_get_average):
        mock_get_average.side_effect = [60.0, 100.0]
        mock_office_hours.return_value = False
        self.assertTrue(EmrScaler(self.emr).should_scale_down(self.threshold))

    @patch("emr_autoscaling.emr.Emr.get_average_of_last_hour")
    @patch("emr_autoscaling.scaler.EmrScaler.is_in_office_hours")
    def test_should_not_scale_down_in_office_hours(self, mock_office_hours, mock_get_average):
        mock_get_average.side_effect = [60.0, 100.0]
        mock_office_hours.return_value = True
        self.assertFalse(EmrScaler(self.emr).should_scale_down(self.threshold))

    @patch("emr_autoscaling.emr.Emr.get_average_of_last_hour")
    @patch("emr_autoscaling.scaler.EmrScaler.is_in_office_hours")
    def test_should_not_scale_down_when_memory_ratio_above_threshold(self, mock_office_hours, mock_get_average):
        mock_get_average.side_effect = [80.0, 100.0]
        mock_office_hours.return_value = False
        self.assertFalse(EmrScaler(self.emr).should_scale_down(self.threshold))

    @patch("emr_autoscaling.emr.Emr.scale")
    @patch("emr_autoscaling.emr.Emr.scaling_in_progress")
    def test_maybe_dont_scale_because_in_progress (
            self, 
            mock_scaling_in_progress, 
            mock_scale
    ):
        mock_scaling_in_progress.return_value = True
        EmrScaler(self.emr).maybe_scale(0.7)
        mock_scale.assert_not_called

    @patch("emr_autoscaling.scaler.EmrScaler.should_scale_up")
    @patch("emr_autoscaling.scaler.EmrScaler.should_scale_down")
    @patch("emr_autoscaling.emr.Emr.scale")
    @patch("emr_autoscaling.emr.Emr.scaling_in_progress")
    def test_maybe_scale_up (
            self, 
            mock_scaling_in_progress, 
            mock_scale,
            mock_should_scale_down,
            mock_should_scale_up
    ):
        mock_scaling_in_progress.return_value = False
        mock_should_scale_down.return_value = False
        mock_should_scale_up.return_value = True
        EmrScaler(self.emr).maybe_scale(0.7)
        mock_scale.assert_called_with(1)

    @patch("emr_autoscaling.scaler.EmrScaler.should_scale_up")
    @patch("emr_autoscaling.scaler.EmrScaler.should_scale_down")
    @patch("emr_autoscaling.emr.Emr.scale")
    @patch("emr_autoscaling.emr.Emr.scaling_in_progress")
    def test_maybe_scale_down (
            self, 
            mock_scaling_in_progress,
            mock_scale,
            mock_should_scale_down,
            mock_should_scale_up
    ):
        mock_scaling_in_progress.return_value = False
        mock_should_scale_down.return_value = True
        mock_should_scale_up.return_value = False
        EmrScaler(self.emr).maybe_scale(0.7)
        mock_scale.assert_called_with(-1)

    @patch("emr_autoscaling.scaler.EmrScaler.should_scale_up")
    @patch("emr_autoscaling.scaler.EmrScaler.should_scale_down")
    @patch("emr_autoscaling.emr.Emr.scale")
    @patch("emr_autoscaling.emr.Emr.scaling_in_progress")
    def test_maybe_dont_scale_because_nothing_to_do (
            self, 
            mock_scaling_in_progress,
            mock_scale,
            mock_should_scale_down,
            mock_should_scale_up
    ):
        mock_scaling_in_progress.return_value = False
        mock_should_scale_down.return_value = False
        mock_should_scale_up.return_value = False
        EmrScaler(self.emr).maybe_scale(0.7)
        mock_scale.assert_not_called
