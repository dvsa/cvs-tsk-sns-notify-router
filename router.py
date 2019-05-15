import datetime
import json
import logging
import os
from configparser import ConfigParser
from typing import Dict, List
from urllib.parse import quote

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from boto3_type_annotations.lambda_ import Client
from botocore.exceptions import ClientError

patch_all(double_patch=True)

LAMBDA_NAME = os.getenv('NOTIFY_LAMBDA_NAME')

COLOURS = {
    'INSUFFICIENT_DATA': 'fce94f',
    'ALARM': 'ef2929',
    'OK': 'acda00'
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
config = ConfigParser()
config.read('config.ini')


@xray_recorder.capture('Invoke DevOps Notify')
def send_payload(payload: Dict):
    try:
        lamb: Client = boto3.client('lambda', 'eu-west-1')
        resp: Dict = lamb.invoke(FunctionName=LAMBDA_NAME, Payload=json.dumps(payload))
        err = resp.get('FunctionError')
        if err is not None:
            logger.info(f"FunctionError: {err}")
            raise RuntimeError(f'{LAMBDA_NAME} failed to notify with {payload}')
        else:
            return resp['Payload'].read()
    except ClientError:
        logger.exception("Error when invoking DevOps notify.")
        raise


def get_recipients(alarm_name) -> List[str]:
    return [r.strip() for r in config.get(section="Recipients", option=alarm_name).split(',')]


def handler(event, context):
    logger.info(f"Event: {str(event)}")
    records = event['Records']
    for record in records:
        message = json.loads(record['Sns']['Message'])
        logger.info(f"Message: {str(message)}")
        timestamp = datetime.datetime.fromisoformat(record['Sns']['Timestamp'].replace("Z", "+00:00"))
        region = record['EventSubscriptionArn'].split(":")[3]
        alarm_name = message['AlarmName']
        old_state = message['OldStateValue']
        new_state = message['NewStateValue']
        reason = message['NewStateReason']
        trigger = message['Trigger']
        if trigger['Namespace'] == 'ETL':
            template_id = config.get(section="Templates", option="ETLAlarm")
            for recipient in get_recipients(alarm_name):
                payload = {
                    "message_type": "email",
                    "to": recipient,
                    "template_id": template_id,
                    "template_vars": {
                        "alarm_name": alarm_name,
                        "statistic": trigger['Statistic'],
                        "metric_name": trigger['MetricName'],
                        "operator": trigger['ComparisonOperator'],
                        "threshold": trigger['Threshold'],
                        "eval_periods": trigger['EvaluationPeriods'],
                        "period": trigger['Period'],
                        "time": timestamp.isoformat(),
                        "old_state": old_state,
                        "new_state": new_state,
                        "reason": reason,
                        "region": region,
                        "quoted_alarm_name": quote(alarm_name)
                    }
                }
                send_payload(payload)
        else:
            payload = {
                "message_type": "teams",
                "body": {
                    "@type": "MessageCard",
                    "@context": "https://schema.org/extensions",
                    "summary": f"AWS Cloudwatch Alarm: {alarm_name} has transitioned",
                    "title": "AWS Cloudwatch Alarm",
                    "themeColor": COLOURS[new_state],
                    "sections": [
                        {
                            "title": f"{alarm_name} has transitioned",
                            "text": f"{trigger['Statistic']} {trigger['MetricName']} {trigger['ComparisonOperator']}"
                            f" {trigger['Threshold']} for {trigger['EvaluationPeriods']}"
                            f" periods(s) of {trigger['Period']} seconds.",
                            "facts": [
                                {
                                    "name": "Time",
                                    "value": timestamp.isoformat()
                                },
                                {
                                    "name": "Old State",
                                    "value": old_state
                                },
                                {
                                    "name": "New State",
                                    "value": new_state
                                },
                                {
                                    "name": "Reason",
                                    "value": reason
                                }
                            ],
                        }
                    ],
                    "potentialAction": [
                        {
                            "@type": "OpenUri",
                            "name": "Link to Alarm",
                            "targets": [
                                {
                                    "os": "default",
                                    "uri": f"https://console.aws.amazon.com/cloudwatch/home?region={region}"
                                    f"#alarm:alarmFilter=ANY;name={quote(alarm_name)}"
                                }
                            ]
                        }
                    ]
                }
            }
            send_payload(payload)
