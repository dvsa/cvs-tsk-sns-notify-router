import datetime
import json
import logging
import os
from typing import Dict
from urllib.parse import quote

import boto3
from boto3_type_annotations.lambda_ import Client
from botocore.exceptions import ClientError

LAMBDA_NAME = os.getenv('NOTIFY_LAMBDA_NAME')

COLOURS = {
    'INSUFFICIENT_DATA': 'ffb900',
    'ALARM': 'f25022',
    'OK': '7Fba00'
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
        metric_namespace = trigger['Namespace']

        payload = {
            "message_type": "teams",
            "body": {
                "@type": "MessageCard",
                "@context": "https://schema.org/extensions",
                "summary": f"AWS Cloudwatch Alarm: {alarm_name} has transitioned",
                "title": f"**AWS Cloudwatch Alarm**",
                "themeColor": COLOURS[new_state],
                "sections": [
                    {
                        "title": f"{alarm_name} has transitioned",
                        "text": f"{trigger['Statistic']} {trigger['MetricName']} {trigger['ComparisonOperator']}"
                        f" {trigger['Threshold']} for {trigger['EvaluationPeriods']}"
                        f" periods(s) of {trigger['Period']} seconds.",
                        "facts": [
                            {"Time": timestamp.isoformat()},
                            {"Old State": old_state},
                            {"New State": new_state},
                            {"Reason": reason}
                        ]
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
        if metric_namespace == 'ETL':
            payload['web_hook_url'] = os.getenv('DATA_MIGRATION_WEBHOOK')

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
