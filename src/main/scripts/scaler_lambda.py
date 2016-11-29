from emr_autoscaling.emr import Emr
from emr_autoscaling.scaler import EmrScaler


def lambda_handler(event, context):
    job_flow_id = event["JobFlowId"]
    threshold = float(event["Threshold"])
    min_instances = int(event["MinInstances"])
    max_instances = int(event["MaxInstances"])
    office_hours_start = int(event["OfficeHoursStart"])
    office_hours_end = int(event["OfficeHoursEnd"])
    shutdown_time = int(event["ShutdownTime"])

    parent_stack_id = event["ParentStackId"] if "ParentStackId" in event else None
    stack_deletion_role = event["StackDeletionRole"] if "StackDeletionRole" in event else None
    scaler = EmrScaler (
        emr = Emr (
            job_flow_id = job_flow_id,
            min_instances = min_instances,
            max_instances = max_instances
        ),
        min_instances = min_instances,
        max_instances = max_instances,
        office_hours_start = office_hours_start,
        office_hours_end = office_hours_end,
        shutdown_time=shutdown_time,
        parent_stack=parent_stack_id,
        stack_deletion_role=stack_deletion_role
    )
    scaler.maybe_shutdown()
    scaler.maybe_scale(threshold)
