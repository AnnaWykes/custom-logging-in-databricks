import json
import cldbx.utils.post_log as pl


def log_error(wks_id, wks_shared_key, body):
    return pl.post_data(wks_id, wks_shared_key, json.dumps(body), "mycustomlogs_error")

def log_info(wks_id, wks_shared_key, body):
    return pl.post_data(wks_id, wks_shared_key, json.dumps(body), "mycustomlogs_info")

def log_critical(wks_id, wks_shared_key, body):
    return pl.post_data(wks_id, wks_shared_key, json.dumps(body), "mycustomlogs_critical")

def log_warning(wks_id, wks_shared_key, body):
    return pl.post_data(wks_id, wks_shared_key, json.dumps(body), "mycustomlogs_warning")