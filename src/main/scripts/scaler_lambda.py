from emr_autoscaling.emr import Emr
from emr_autoscaling.scaler import EmrScaler


def lambda_handler(event, context):
    job_flow_id = event["JobFlowId"]
    threshold = float(event["Threshold"])
    min_instances = int(event["MinInstances"])
    max_instances = int(event["MaxInstances"])
    office_hours_start = int(event["OfficeHoursStart"])
    office_hours_end = int(event["OfficeHoursEnd"])
    scaler = EmrScaler (
        emr = Emr (
            job_flow_id = job_flow_id,
            min_instances = min_instances,
            max_instances = max_instances
        ),
        min_instances = min_instances,
        max_instances = max_instances,
        office_hours_start = office_hours_start,
        office_hours_end = office_hours_end
    )
    scaler.maybe_scale(threshold)
