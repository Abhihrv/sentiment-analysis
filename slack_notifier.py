import mysql.connector
from mysql.connector import Error
import time
from timeloop import Timeloop
from datetime import timedelta
from xlsxwriter.workbook import Workbook
import requests
import json
import os
tl = Timeloop()
url='https://events.pagerduty.com/v2/enqueue'
from twitter_streamer import heartbeat,process_id

def fetch_sentiment():
    try:
        con = mysql.connector.connect(host='localhost',
                                      database='sentiment', user='sentiment', password='sentiment', charset='utf8mb4')

        if con.is_connected():
            """
            Insert twitter data
            """
            cursor = con.cursor(buffered=True)

            query = "SELECT * FROM sentiment_record WHERE time_of_the_day > NOW() - INTERVAL 15 MINUTE"
            cursor.execute(query)
            record=cursor.fetchall()
            con.commit()

    except Error as e:
        print(e)

    cursor.close()
    con.close()

    return record

def webhook_print():
    data = fetch_sentiment()
    url = 'https://hooks.slack.com/services/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'
    string1 = 'Outage Alert! There are ' + str(len(data)) + ' new opinions which need attention!'
    for item in data:
        string1 = string1 + '\n' + str(item[3]) + ', Social Handle:' + str(item[2]) + ', Time:' + str(item[5])
    response = requests.post(url, data=json.dumps({'text': string1}))
    print(response.status_code)



def pager_duty_incident():
    data = fetch_sentiment()
    final_data = list()
    for item in data:
        if item[4] < 0:
            final_data.append(item)

    if len(final_data) >= 5:
        url = 'https://events.pagerduty.com/v2/enqueue'
        status_ids = " There are " + str(len(final_data)) + ' new opinions which need attention!'
        for item in final_data:
            status_ids += '\n' + str(item[3]) + ', Social Handle:' + str(item[2]) + ', Time:' + str(item[5])
        alert = {
            "payload": {
                "summary": "Outage Warning!" + status_ids,
                "timestamp": "2015-07-17T08:42:58.315+0000",
                "source": "http://ec2-54-213-119-85.us-west-2.compute.amazonaws.com/",
                "severity": "warning",
            },
            "routing_key": "03efdd8a50dd466a91d63bba742026d3",
            "dedup_key": "samplekeyhere",

            "links": [{
                "href": "https://example.com/",
                "text": "Link text"
            }],
            "event_action": "trigger",
            "client": "Sample Monitoring Service",

        }
        response = requests.post(url, data=json.dumps(alert))
        print(response.status_code)


@tl.job(interval=timedelta(seconds=60))
def alert_job_every_1min():
    heartbeat('slack')
    pager_duty_incident()

if __name__ == "__main__":
    pid = os.getpid()
    process_id(pid,'slack')
    tl.start(block=True)