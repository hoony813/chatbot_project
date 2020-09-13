import sys
import os
import boto3
import logging

from boto3.dynamodb.conditions import Key, Attr

def main():
    try:
        dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2',endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
    except:
        logging.error('could not connect to dynamodb')
        sys.exit(1)

    table = dynamodb.Table('top_tracks')

    response = table.query(
        KeyConditionExpression=Key('artist_id').eq('0L8ExT028jH3ddEcZwqJJ5'),
        FilterExpression=Attr('popularity').gt(70)
    )

    print(response)
    print(len(response['Items']))


if __name__ == "__main__":
    main()