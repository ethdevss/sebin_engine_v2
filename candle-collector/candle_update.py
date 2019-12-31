import schedule
import time

import datetime as dt
from mongoengine import *

import requests
import pymongo

from marshmallow import Schema, fields, pprint

class MarketData(Document):
    timestamp = DateTimeField(required=True, unique=True)
    symbol = StringField(required=True)
    open = IntField()
    high = IntField()
    low = IntField(required=True)
    close = IntField(required=True)
    trades = IntField()
    volume = IntField()
    meta = {'collection': 'candles'}


class Candle1m(Document):
    timestamp = DateTimeField(required=True, unique=True)
    symbol = StringField(required=True)
    open = IntField()
    high = IntField()
    low = IntField(required=True)
    close = IntField(required=True)
    trades = IntField()
    volume = IntField()
    meta = {'collection': 'candles1m'}


class TimeSchema(Schema):
    end_time = fields.DateTime()
    start_time = fields.DateTime()


def get_low_price_from_1m_candles():
    candles = Candle1m.objects().order_by('-timestamp')[:4]
        
    # 캔들 데이터 중 Low가격에 대한 정보를 리스트에 저장한다.
    low_list = [float(candle.low) for candle in candles]
    return min(low_list)


def get_recent_trades(start, end):
    recent_trades_url = "https://www.bitmex.com/api/v1/trade?symbol=XBTUSD&count=1000&reverse=true"

    start_param = "&startTime=" + start
    end_param = "&endTime=" + end

    url = recent_trades_url + start_param + end_param
    response = requests.get(url)
    recent_trades = response.json()

    low_price = recent_trades[0]['price']
    for recent_trade in recent_trades:
        if low_price > recent_trade['price']:
            low_price = recent_trade['price']
    return low_price


def get_close_price():
    url = "https://www.bitmex.com/api/v1/trade?symbol=XBT&count=10&reverse=true"
    response = requests.get(url)
    close_price = response.json()[0]['price']
    return close_price


def get_candle_low_price():
    current_time = dt.datetime.now()
    start_time = current_time - dt.timedelta(20)

    current_time2 = start_time
    start_time2 = current_time2 - dt.timedelta(20)

    current_time3 = start_time2
    start_time3 = current_time3 - dt.timedelta(20)

    end_time_list = []
    start_time_list = []

    end_time_list.append(current_time)
    end_time_list.append(current_time2)
    end_time_list.append(current_time3)

    start_time_list.append(start_time)
    start_time_list.append(start_time2)
    start_time_list.append(start_time3)
    # 방금 Close 된 1분봉의 LOW Price를 Recent Trades API를 통해 가져온다.
    low_price_list = []
    
    for start_time, end_time in zip(start_time_list, end_time_list):
        input_data = {
            'start_time': start_time,
            'end_time': end_time
        }
        schema = TimeSchema()
        result = schema.dump(input_data)
        startTime = result['start_time']
        endTime = result['end_time']

        startTime = startTime[:20] + '00000'
        endTime = endTime[:20] + '00000'

        low_price = get_recent_trades(startTime, endTime)
        low_price_list.append(low_price)

    low_price = get_low_price_from_1m_candles()
    low_price_list.append(low_price)
    #low_price_list = [int(i) for i in low_price_list]
    return min(low_price_list)


def job():
    close = get_close_price()
    low = get_candle_low_price()
    timestamp = str(dt.datetime.now()).split('.')[0] + ".000Z"
    timestamp = timestamp[:16] + ':00.000Z'
    symbol = "XBTUSD"

    print("low price: " + str(low))
    print("close price: " + str(close))
    marketdata = MarketData(timestamp=timestamp, symbol=symbol, low=low, close=close)
    try:
        marketdata.save()
    except pymongo.errors.DuplicateKeyError:
        pass
    except NotUniqueError:
        pass
    except Exception as e:
        print(e)


connection = connect(db='market_data')

for hour in range(24):
    for minute in range(0, 60, 5):
        hour = str(hour)
        minute = str(minute)
        if len(hour) == 1:
            hour = '0' + hour
        if len(minute) == 1:
            minute = '0' + minute
        param = str(hour) + ":" + str(minute)
        schedule.every().day.at(param).do(job)

while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        print(e)
