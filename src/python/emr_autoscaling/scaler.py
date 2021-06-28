from datetime import datetime
from src.python.pytz import timezone
import logging
import boto3
from .utils import get_logger
from .constants import UP, DOWN


class EmrScaler:

    def __init__(self, emr, min_instances=0, max_instances=20, office_hours_start=7, office_hours_end=18,
                 shutdown_time=23, parent_stack=None, stack_deletion_role=None):
        self.min_instances = min_instances
        self.max_instances = max_instances
        self.office_hours_start = office_hours_start
        self.office_hours_end = office_hours_end
        self.logger = get_logger('EMRScaler')
        self.emr = emr
        self.time_zone = timezone('Europe/Berlin')

        #Calculating offset of timezone to subtract from the shutdown time
        self.time_offset = int(
            (datetime
                .now(self.time_zone)
                .utcoffset()
                .total_seconds()) / (60 * 60)
        )
        self.shutdown_time = datetime\
            .now(self.time_zone)\
            .replace(hour=shutdown_time - self.time_offset, minute=0, second=0, microsecond=0)

        self.parent_stack = parent_stack
        self.cloud_formation = boto3.client('cloudformation')
        self.stack_deletion_role = stack_deletion_role

    def is_in_office_hours(self, curr_time):
        self.logger.info("it is now {HOUR}:{MINUTE} on {WEEKDAY} ({DAY_NUMBER})"
                         .format(HOUR=curr_time.hour, MINUTE=curr_time.minute,
                                 WEEKDAY=curr_time.strftime("%A"), DAY_NUMBER=curr_time.weekday()))
        self.logger.info("office hours are from {OFFICE_HOURS_START} until {OFFICE_HOURS_END}"
                         .format(OFFICE_HOURS_START=self.office_hours_start, OFFICE_HOURS_END=self.office_hours_end))
        return (
            curr_time.hour >= self.office_hours_start and (
                curr_time.hour < self.office_hours_end or
                curr_time.hour == self.office_hours_end and (curr_time.minute == curr_time.second == curr_time.microsecond == 0)
            ) and (
                curr_time.weekday() <= 4
            )
        )

    def should_scale_down(self, threshold):
        memory_used_ratio = self.emr.get_average_of_last_hour("MemoryAllocatedMB") / self.emr.get_average_of_last_hour("MemoryTotalMB")
        if memory_used_ratio <= threshold:
            if self.is_in_office_hours(datetime.now(self.time_zone)):
                self.logger.info (
                    "Memory used ratio {} is below threshold of {}, but won't scale down due to office hours.".format (
                        memory_used_ratio, threshold
                    )
                )
                return False
            self.logger.info (
                "Memory used ratio {} is below threshold of {}, should scale down.".format (
                    memory_used_ratio, threshold
                )
            )
            return True
        return False

    def should_scale_up(self):
        container_pending = self.emr.get_pending_containers()
        if container_pending > 0:
            self.logger.info("{} containers are waiting, should scale up.".format(container_pending))
            return True
        else:
            return False

    def maybe_scale(self, threshold):
        if self.emr.scaling_in_progress():
            self.logger.info("Scaling is already running, doing nothing.")
        else:
            scale_up_needed = self.should_scale_up()
            scale_down_needed = self.should_scale_down(threshold)
            if scale_up_needed:
                self.emr.scale(UP)
            elif scale_down_needed:
                self.emr.scale(DOWN)
            else:
                self.logger.info("Nothing to do, going back to sleep.")

    def maybe_shutdown(self):
        self.logger.info("Parent stack: %s" % self.parent_stack)
        if self.is_after_shutdown_time() and not self.emr.is_termination_protected() and self.parent_stack:
            self.shutdown()

    def shutdown(self):
        self.cloud_formation.delete_stack(StackName=self.parent_stack, RoleARN=self.stack_deletion_role)

    def is_after_shutdown_time(self, time=None):
        time = time or datetime.now()
        time = self.time_zone.localize(time)
        self.logger.info("Current time: %s, shutdown time %s" % (time, self.shutdown_time))
        return self.shutdown_time.time() <= time.time()

