# PRECISION = "s"
# MEASUREMENT = "candlesticks"
# EXCHANGE = "bitmex"
#
# SUBSCRIPTION_DICT = {"op":"subscribe", "args":'tradeBin1m:{}'}
# PAIR_URL = "https://www.bitmex.com/api/v1/instrument/active"
# WEBSOCK_URL = "wss://www.bitmex.com/realtime?subscribe=tradeBin5m:XBTUSD"
# TYPE = "PERPEPTUAL"

import websocket
import json
from mongoengine import *

import pymongo
import telegram
import dateutil.parser

class MarketData(Document):
    timestamp = DateTimeField(required=True, unique=True)
    symbol = StringField(required=True)
    open = IntField(required=True)
    high = IntField(required=True)
    low = IntField(required=True)
    close = IntField(required=True)
    trades = IntField(required=True)
    volume = IntField(required=True)
    meta = {'collection': 'candles'}


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


def on_message(ws, message):
    data = dict()
    if "partial" in message:
        message = json.loads(message)
        timestamp = message['data'][0]['timestamp']
        symbol = message['data'][0]['symbol']
        open = message['data'][0]['open']
        high = message['data'][0]['high']
        low = message['data'][0]['low']
        close = message['data'][0]['close']
        trades = message['data'][0]['trades']
        volume = message['data'][0]['volume']

        parse_datetime = dateutil.parser.parse(timestamp)
        parse_datetime_minute = parse_datetime.minute
       
        # 30분봉이 만들어지는 순간(마지막 5분봉이 완성되는 순간)이라면
        if parse_datetime_minute == '0' or parse_datetime_minute == '30':
            # 5분, 10분, 15분, 20분, 25분
            # 기존 5분봉 Collections에 있는 가장 최근 5개의 캔들 데이터를 가져온다
            candles5m = MarketData.objects().order_by('-timestamp')[:5]
            
            low_list = [float(candle5m.low) for candle5m in candles5m]
            low_list.append(low)
            low = min(low_list)
            try:
                candle_row = Candle30m.objects(timestamp=timestamp).get()
                candle_row.update(close=close, low=low)
            except Exception as e:
                candle30m = Candle30m(timestamp=timestamp, symbol=symbol, low=low, close=close)
                candle30m.save()
                print(e)

        marketdata = MarketData(timestamp=timestamp, symbol=symbol, open=open, 
                                high=high, low=low, close=close, trades=trades,
                                volume=volume)
        try:
            candle_row = MarketData.objects(timestamp=timestamp).get()
            candle_row.update(open=open, high=high, low=low, close=close, trades=trades, volume=volume)
        except Exception as e:
            marketdata = MarketData(timestamp=timestamp, symbol=symbol, open=open, 
                            high=high, low=low, close=close, trades=trades,
                            volume=volume)
            marketdata.save()
            print(e)
    elif "insert" in message:
        message = json.loads(message)
        timestamp = message['data'][0]['timestamp']
        symbol = message['data'][0]['symbol']
        open = message['data'][0]['open']
        high = message['data'][0]['high']
        low = message['data'][0]['low']
        close = message['data'][0]['close']
        trades = message['data'][0]['trades']
        volume = message['data'][0]['volume']

        parse_datetime = dateutil.parser.parse(timestamp)
        parse_datetime_minute = parse_datetime.minute
       
        # 30분봉이 만들어지는 순간(마지막 5분봉이 완성되는 순간)이라면
        if parse_datetime_minute == '0' or parse_datetime_minute == '30':
            # 5분, 10분, 15분, 20분, 25분
            # 기존 5분봉 Collections에 있는 가장 최근 5개의 캔들 데이터를 가져온다
            candles5m = MarketData.objects().order_by('-timestamp')[:5]
            
            low_list = [float(candle5m.low) for candle5m in candles5m]
            low_list.append(low)
            low = min(low_list)
            try:
                candle_row = Candle30m.objects(timestamp=timestamp).get()
                candle_row.update(close=close, low=low)
            except Exception as e:
                candle30m = Candle30m(timestamp=timestamp, symbol=symbol, low=low, close=close)
                candle30m.save()
                print(e)

        marketdata = MarketData(timestamp=timestamp, symbol=symbol, open=open,
                                high=high, low=low, close=close, trades=trades, 
                                volume=volume)
        try:
            candle_row = MarketData.objects(timestamp=timestamp).get()
            candle_row.update(open=open, high=high, low=low, close=close, trades=trades, volume=volume)
        except Exception as e:
            marketdata = MarketData(timestamp=timestamp, symbol=symbol, open=open, 
                            high=high, low=low, close=close, trades=trades,
                            volume=volume)
            marketdata.save()
            print(e)
          
    print(message)

def on_error(ws, error):
    message = str(error) + "candle collector의 websocket 연결이 종료되었습니다."
    ws.on_close(ws)

def on_close(ws):
    print("### closed ###")
    message = "candle collector의 websocket 연결이 종료되었습니다."
    ws.close()

def on_open(ws):
    print("### open ###")

def run(endpoint):
    connection = connect(db='market_data')
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(endpoint,
                                on_open = on_open,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)

    while True:
        try:
            ws.run_forever(ping_interval=30, ping_timeout=25)
        except:
            pass

        
if __name__ == "__main__":
    command = 'subscribe=tradeBin5m:XBTUSD'
    endpoint = 'wss://www.bitmex.com/realtime?'+command
    
    run(endpoint)
