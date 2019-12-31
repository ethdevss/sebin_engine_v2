import requests

from marshmallow import Schema, fields, pprint
import datetime as dt

from mongoengine import *
import pymongo

from time import sleep

import dateutil

class Candles30m(Document):
    timestamp = DateTimeField(required=True, unique=True)
    symbol = StringField(required=True)
    open = IntField()
    high = IntField()
    low = IntField(required=True)
    close = IntField(required=True)
    trades = IntField()
    volume = IntField()
    meta = {'collection': 'candles30m'}


current_timestamp = dt.datetime.now()
minute = current_timestamp.minute
second = current_timestamp.second

if minute >= 30:
    # 현재 시간이 30분 이상일 경우
    # 35분일경우, 5분 데이터를 가져옴
    if str(minute)[0] == '3':
        minute_param = str(minute)[1]
    elif str(minute)[0] == '4':
        minute_param = '1' + str(minute)[1]
    elif str(minute)[0] == '5':
        minute_param = '2' + str(minute)[1]
    minute = int(minute_param)
    current_timestamp = current_timestamp - dt.timedelta(minutes=minute, seconds=second)
else:
   # 30분 미만일 경우
    current_timestamp = current_timestamp - dt.timedelta(minutes=minute, seconds=second)
    
start_time = current_timestamp - dt.timedelta(days=21)
#end_time = current_timestamp - dt.timedelta(hours=9)
#don't consider UTC-9 from aws ec2
end_time = current_timestamp

class TimeSchema(Schema):
    end_time = fields.DateTime()
    start_time = fields.DateTime()

schema = TimeSchema()

connection = connect(db='market_data')

print(start_time)
print(end_time)

isLast = False
while True:
    if start_time + dt.timedelta(hours=8) > end_time:
        start_time = start_time + dt.timedelta(hours=8) 
        input_data = {
            'end_time': end_time,
            'start_time': start_time - dt.timedelta(hours=8)
        }
        if isLast:
            break
        isLast = True

    if not isLast:
        input_data = {
            'end_time': start_time + dt.timedelta(hours=8),
            'start_time': start_time
        }

    result = schema.dump(input_data)
    startTime = result['start_time']
    endTime = result['end_time']

    dataset_url = "https://www.bitmex.com/api/v1/trade/bucketed?binSize=5m&partial=false&symbol=XBTUSD&reverse=true"
    
    start_param = "&startTime=" + startTime
    end_param = "&endTime=" + endTime

    sleep(2)
    url = dataset_url + start_param + end_param
    print(url)
    response = requests.get(url)
    ohlcv = response.json()

    start_time = start_time + dt.timedelta(hours=8)
   
    low_price_list = []
    close_price_list = []
    timestamp_list = []
    idx = 0    
    for data in ohlcv:
        timestamp = data['timestamp']
        symbol = data['symbol']
        open = data['open']
        high = data['high']
        low = data['low']
        close = data['close']
        trades = data['trades']
        volume = data['volume']
        
        low_price_list.append(low)
        close_price_list.append(close)
        timestamp_list.append(timestamp)
        idx = idx + 1
        # 5분봉 6개 타이밍, 즉 30분봉이 완성된 시점의 데이터
        #(30분, 25분, 20분, 15분, 10분, 5분) 총 6개의 5분봉 데이터가 필요함 for make 30m candle
        if idx % 6 == 0:
            low = min(low_price_list)
            low_price_list = []
            close = close_price_list[0]
            close_price_list = []
            res = dateutil.parser.parse(timestamp_list[0])
            timestamp_list = []
            new_timestamp = str(res)[:19] + '.000Z'
            idx = 0
            candles30m = Candles30m(timestamp=new_timestamp, symbol=symbol, low=low,
                                    close=close)
            try:
                candles30m.save()
            except pymongo.errors.DuplicateKeyError:
                print('duplicate key erorr')
            except NotUniqueError:
                print('not unique')
            except Exception as e:
                print(e)
