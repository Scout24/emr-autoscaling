from datetime import datetime
import logging


class EmrScaler:

    def __init__(self, emr, min_instances = 0, max_instances = 20):
        self.min_instances = min_instances
        self.max_instances = max_instances
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.emr = emr

    def is_in_office_hours(self, curr_time = datetime.now()):
        return (
            curr_time.hour >= 7 and (
                curr_time.hour < 18 or
                curr_time.hour == 18 and (curr_time.minute == curr_time.second == curr_time.microsecond == 0)
            ) and (
                curr_time.weekday() <= 4
            )
        )

    def should_scale_down(self, threshold):
        memory_used_ratio = self.emr.get_average_of_last_hour("MemoryAllocatedMB") / self.emr.get_average_of_last_hour("MemoryTotalMB")
        if memory_used_ratio <= threshold:
            if self.is_in_office_hours():
                self.logger.info (
                    "Memory used ratio {} is below threshold of {}, but won't scale down due to office hours.".format (
                        memory_used_ratio, threshold
                    )
                )
                return False
            else:
                self.logger.info (
                    "Memory used ratio {} is below threshold of {}, should scale down.".format (
                        memory_used_ratio, threshold
                    )
                )
                return True
        else:
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
            scale_down_needed = self.should_scale_down(threshold)
            scale_up_needed = self.should_scale_up()
            if scale_up_needed:
                self.emr.scale(1)
            elif scale_down_needed:
                self.emr.scale(-1)
            else:
                self.logger.info("Nothing to do, going back to sleep.")
