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


class Candle30m(Document):
    timestamp = DateTimeField(required=True, unique=True)
    symbol = StringField(required=True)
    open = IntField()
    high = IntField()
    low = IntField(required=True)
    close = IntField(required=True)
    trades = IntField()
    volume = IntField()
    meta = {'collection': 'candles30m'}


class TimeSchema(Schema):
    end_time = fields.DateTime()
    start_time = fields.DateTime()


# 1분봉 캔들 데이터가 저장되어 있는 Collections에서 가장 최근에 쌓인 1분봉 캔들 4개를 가져온다.
# 가져온 1분봉 캔들 데이터 중, 가장 낮은 low_price를 리턴하는 함수이다.
def get_low_price_from_1m_candles():
    candles = Candle1m.objects().order_by('-timestamp')[:4]
        
    # 캔들 데이터 중 Low가격에 대한 정보를 리스트에 저장한다.
    low_list = [float(candle.low) for candle in candles]
    return min(low_list)


# start부터 end까지 거래된 거래들 중, 가장 낮은 가격(low_price)를 리턴하는 함수이다.
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


# 가장 최근에 거래된 가격정보를 가져온다. 보통 봉의 Close Price를 구할 때 사용한다. 
def get_close_price():
    url = "https://www.bitmex.com/api/v1/trade?symbol=XBT&count=10&reverse=true"
    response = requests.get(url)
    close_price = response.json()[0]['price']
    return close_price


# 4개의 1분봉 캔들 데이터를 통해 0분 ~ 4분까지의 거래된 내역들 중 가장 낮은 가격을 가져온다. 
# 최근 1분동안 거래된 내역들 중에서 가장 낮은 가격을 가져온다.
# 0분 ~ 4분동안 거래된 내역중 가장 낮은 가격  + 4분 ~ 5분동안 거래된 내역들 중 가장 낮은 가격의 비교를 통해
# 최근 5분동안 거래된 내역들 중 가장 낮은 가격을 가져온다.
def get_candle_low_price():
    current_time = dt.datetime.now()
    start_time = current_time - dt.timedelta(seconds=20)

    current_time2 = start_time
    start_time2 = current_time2 - dt.timedelta(seconds=20)

    current_time3 = start_time2
    start_time3 = current_time3 - dt.timedelta(seconds=20)

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

    low_price_list.append(low_price)
    return min(low_price_list)


# 5분봉 Collections에서 최근 5개의 5분봉 데이터를 불러온다
def get_recent_5m_candles():
    candles5m = MarketData.objects().order_by('-timestamp')[:5]
    # 5개의 5분봉 캔들 데이터 중 Low가격에 대한 정보를 리스트에 저장한다.
    low_list = [float(candle5m.low) for candle5m in candles5m]
    return min(low_list)


def update_30m_job():
    # 가장 최근에 거래된 가격을 가져온다. 이 가격은 Close price에 사용된다.
    close = get_close_price()

    # 30분봉 중, 25분동안 거래된 거래들 중 가장 낮은 가격을 가져온다.
    recent_25m_low_price = get_recent_5m_candles()

    # 1분봉 Collections에서 최근 4개의 캔들 데이터 중 가장 낮은 가격을 가져온다.
    recent_4m_low_price = get_low_price_from_1m_candles()

    # 최근 1분동안 발생한 Recent Trades를 중 가장 낮은 가격을 가져온다.
    recent_1m_low_price = get_candle_low_price()

    
    low_price_list = []
    low_price_list.append(recent_25m_low_price)
    low_price_list.append(recent_4m_low_price)
    low_price_list.append(recent_1m_low_price)
    
    low = min(low_price_list)
    
    timestamp = str(dt.datetime.now()).split('.')[0] + ".000Z"
    timestamp = timestamp[:16] + ':00.000Z'
    symbol = "XBTUSD"

    print("low price: " + str(low))
    print("close price: " + str(close))

    candle30m = Candle30m(timestamp=timestamp, symbol=symbol, low=low, close=close)
    try:
        candle30m.save()
    except pymongo.errors.DuplicateKeyError:
        pass
    except NotUniqueError:
        pass
    except Exception as e:
        print(e)


connection = connect(db='market_data')

for hour in range(24):
    for minute in range(0, 60, 30):
        hour = str(hour)
        minute = str(minute)
        if len(hour) == 1:
            hour = '0' + hour
        if len(minute) == 1:
            minute = '0' + minute
        param = str(hour) + ":" + str(minute)
        schedule.every().day.at(param).do(update_30m_job)

while True:
    schedule.run_pending()
    time.sleep(1)
