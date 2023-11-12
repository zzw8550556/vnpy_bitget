from urllib.parse import urlencode
import base64
import json
import hmac
import sys
from copy import copy
from datetime import datetime, timedelta
from time import time,sleep
from threading import Lock
from typing import Dict, List, Any,Union


from vnpy.event import Event,EventEngine
from vnpy_rest import RestClient, Request
from vnpy_websocket import WebsocketClient
from types import coroutine
from asyncio import (
    run_coroutine_threadsafe,
)
from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    BarData,
    AccountData,
    PositionData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.utility import ZoneInfo


# 中国时区
CHINA_TZ = ZoneInfo("Asia/Shanghai")

REST_HOST = "https://api.bitget.com"
WEBSOCKET_DATA_HOST = "wss://ws.bitget.com/mix/v1/stream"               # Market Data
WEBSOCKET_TRADE_HOST = "wss://ws.bitget.com/mix/v1/stream"    # Account and Order

STATUS_BITGETS2VT: Dict[int, Status] = {
    "init": Status.NOTTRADED,
    "new": Status.NOTTRADED,
    "partially_filled": Status.PARTTRADED,
    "partial-fill": Status.PARTTRADED,
    "canceled": Status.CANCELLED,
    "cancelled": Status.CANCELLED,
    "filled": Status.ALLTRADED,
    "full-fill": Status.ALLTRADED,
}

PLANSTATUS_BITGETS2VT: Dict[int, Status] = {
    "not_trigger": Status.NOTTRADED,
    "triggered":Status.ALLTRADED,
    "fail_trigger": Status.REJECTED,
    "cancel": Status.CANCELLED,
    "executing":Status.NOTTRADED,
    
}

ORDERTYPE_VT2BITGETS: Dict[OrderType, Any] = {
    OrderType.LIMIT: "limit",
    OrderType.MARKET: "market",
}
ORDERTYPE_BITGETS2VT: Dict[Any, OrderType] = {v: k for k, v in ORDERTYPE_VT2BITGETS.items()}

DIRECTION_VT2BITGETS: Dict[Direction, str] = {
    Direction.LONG: "buy_single",
    Direction.SHORT: "sell_single",
}
DIRECTION_BITGETS2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BITGETS.items()}

HOLDSIDE_BITGETS2VT: Dict[str,Direction] = {
    "long":Direction.LONG,
    "short":Direction.SHORT
}

INTERVAL_VT2BITGETS: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1H",
    Interval.DAILY: "1D"
}

TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}



class BitGetSGateway(BaseGateway):
    """
    * bitget接口
    * 单向持仓模式
    """
    
    default_name: str = "BITGET_USDT"

    default_setting: Dict[str, Any] = {
        "API Key": "",
        "Secret Key": "",
        "Passphrase":"",
        "会话数": 3,
        "代理地址": "",
        "代理端口": "",
    }

    exchanges = [Exchange.BITGET]        #由main_engine add_gateway调用
    #------------------------------------------------------------------------------------------------- 
    def __init__(self, event_engine: EventEngine, gateway_name: str):
        """
        """
        super(BitGetSGateway,self).__init__(event_engine, gateway_name)
        self.orders: Dict[str, OrderData] = {}
        self.rest_api = BitGetSRestApi(self)
        self.trade_ws_api = BitGetSTradeWebsocketApi(self)
        self.market_ws_api = BitGetSDataWebsocketApi(self)
        self.count = 0  #轮询计时:秒
    #------------------------------------------------------------------------------------------------- 
    def connect(self,setting:dict = {}):
        """
        """

        key = setting["API Key"]
        secret = setting["Secret Key"]
        session_number = setting["会话数"]
        passphrase = setting["Passphrase"]
        proxy_host = setting["代理地址"]
        proxy_port = setting["代理端口"]

        self.rest_api.connect(key, secret,passphrase,session_number,
                              proxy_host, proxy_port)
        self.trade_ws_api.connect(key, secret,passphrase,proxy_host, proxy_port)
        self.market_ws_api.connect(key, secret,passphrase,proxy_host, proxy_port)

        self.init_query()
    #------------------------------------------------------------------------------------------------- 
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅合约
        """
        self.market_ws_api.subscribe(req)
    #------------------------------------------------------------------------------------------------- 
    def send_order(self, req: OrderRequest) -> str:
        """
        发送委托单
        """
        return self.rest_api.send_order(req)
    #------------------------------------------------------------------------------------------------- 
    def cancel_order(self, req: CancelRequest) -> Request:
        """
        取消委托单
        """
        self.rest_api.cancel_order(req)
    #------------------------------------------------------------------------------------------------- 
    def query_account(self) -> Request:
        """
        查询账户
        """
        self.rest_api.query_account()
    #------------------------------------------------------------------------------------------------- 
    def query_order(self,symbol:str):
        """
        查询活动委托单
        """
        self.rest_api.query_order(symbol)
    #-------------------------------------------------------------------------------------------------  
    def query_position(self, symbol: str):
        """
        查询持仓
        """
        pass
    #-------------------------------------------------------------------------------------------------   
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)
    #---------------------------------------------------------------------------------------
    def on_order(self, order: OrderData) -> None:
        """
        收到委托单推送，BaseGateway推送数据
        """
        self.orders[order.orderid] = copy(order)
        super().on_order(order)
    #---------------------------------------------------------------------------------------
    def get_order(self, orderid: str) -> OrderData:
        """
        用vt_orderid获取委托单数据
        """
        return self.orders.get(orderid, None)
    #------------------------------------------------------------------------------------------------- 
    def close(self) -> None:
        """
        关闭接口
        """
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()
    #------------------------------------------------------------------------------------------------- 
    def process_timer_event(self, event: Event):
        """
        处理定时任务
        """
        pass
    #------------------------------------------------------------------------------------------------- 
    def init_query(self):
        """
        初始化定时查询
        """
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
#------------------------------------------------------------------------------------------------- 
class BitGetSRestApi(RestClient):
    """
    BITGET REST API
    """
    def __init__(self, gateway: BitGetSGateway):
        """
        """
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.host: str = ""
        self.key: str = ""
        self.secret: str = ""

        self.order_count: int = 10000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        self.all_contracts:List[str] = []          #所有vt_symbol合约列表
        self.product_types = ["umcbl","sumcbl"]   # USDT,USDC,币本位合约："umcbl","cmcbl","dmcbl","sumcbl"
        self.product_coin_map = {
            "UMCBL": "USDT",
            "CMCBL": "USDC",
            "SUMCBL": "SUSDT",
            "umcbl": "USDT",
            "cmcbl": "USDC",
            "sumcbl": "SUSDT",
        }
        self.delivery_date_map:Dict[str,str] = {}
        self.contract_inited:bool = False
    #------------------------------------------------------------------------------------------------- 
    def sign(self, request) -> Request:
        """
        生成签名
        """
        timestamp = str(int(time() * 1000))
        path = request.path
        method = request.method
        data = request.data
        params = request.params
        if method == "GET":
            path += "?" + urlencode(params)
        if method == "POST":
            body = request.data = json.dumps(data)
        else:
            body = ""
        
        message = timestamp + method + path + body
        signature = create_signature(self.secret,message)

        if not request.headers:
            request.headers = {}
            request.headers["ACCESS-KEY"] = self.key
            request.headers["ACCESS-SIGN"] = signature
            request.headers["ACCESS-TIMESTAMP"] = timestamp
            request.headers["ACCESS-PASSPHRASE"] = self.passphrase
            request.headers["Content-Type"] = "application/json"
        return request
    #------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        key: str,
        secret: str,
        passphrase: str,
        session_number: int,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        连接REST服务
        """
        self.key = key
        self.secret = secret
        self.passphrase = passphrase
        self.connect_time = (
            int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )

        self.init(REST_HOST, proxy_host, proxy_port)
        self.start(session_number)

        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API启动成功")

        self.query_contract()
        self.query_account()
        self.query_order()
    #------------------------------------------------------------------------------------------------- 
    def get_margin_coin(self,symbol:str):
        """
        获取保证金币种
        """
        product_type = symbol.split("_")[1]
        margin_coin = self.product_coin_map.get(product_type,None)
        if not margin_coin:
            margin_coin = symbol.split("USD")[0]
        return margin_coin
    #------------------------------------------------------------------------------------------------- 
    def set_leverage(self,symbol:str):
        """
        设置杠杆
        """
        
        data = {
            "symbol":symbol,
            "marginCoin":self.get_margin_coin(symbol),
            "leverage":20,
        }
        self.add_request(
            method="POST",
            path="/api/mix/v1/account/setLeverage",
            callback=self.on_leverage,
            data = data
        )
    #------------------------------------------------------------------------------------------------- 
    def on_leverage(self,data:dict,request: Request):
        pass
    #------------------------------------------------------------------------------------------------- 
    def query_account(self) -> Request:
        """
        查询账户数据
        """
        for product in self.product_types:
            params = {"productType":product}
            self.add_request(
                method="GET",
                path="/api/mix/v1/account/accounts",
                callback=self.on_query_account,
                params= params
            )
    #------------------------------------------------------------------------------------------------- 
    def query_order(self):
        """
        查询合约活动委托单
        """
        for product in self.product_types:
            params = {
                "productType": product,
                "marginCoin":self.product_coin_map.get(product,None)
                }
            self.add_request(
                method="GET",
                path="/api/mix/v1/order/marginCoinCurrent",
                callback=self.on_query_order,
                params=params
            )
            self.add_request(
                method="GET",
                path="/api/mix/v1/plan/currentPlan",
                callback=self.on_query_order_Algo,
                params=params
            )

    #------------------------------------------------------------------------------------------------- 
    def query_contract(self) -> Request:
        """
        获取合约信息
        """
        for product in self.product_types:
            params = {"productType":product}    
            self.add_request(
                method="GET",
                path="/api/mix/v1/market/contracts",
                params = params,
                callback=self.on_query_contract,
            )
    #------------------------------------------------------------------------------------------------- 
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        查询历史数据
        """
        history : List[BarData] = []
        start= req.start
        if start>datetime.now(CHINA_TZ)-timedelta(days=30):
            count = 1000
        else:
            count=200
        time_delta = TIMEDELTA_MAP[req.interval]

        while True:
            end = start + time_delta * count

            # 查询K线参数
            params = {
                "symbol": req.symbol,
                "granularity": INTERVAL_VT2BITGETS[req.interval],
                "startTime": str(int(start.timestamp() * 1000)),
                "endTime": str(int(end.timestamp() * 1000)),
                "limit":str(count)
            }
            if start>datetime.now(CHINA_TZ)-timedelta(days=30):
                resp = self.request(
                    "GET",
                    "/api/mix/v1/market/candles",
                    params=params
                )
            else:
                resp = self.request(
                    "GET",
                    "/api/mix/v1/market/history-candles",
                    params=params
                )
            # resp为空或收到错误代码则跳出循环
            if not resp:
                msg = f"获取历史数据失败，状态码：{resp}"
                self.gateway.write_log(msg)
                break
            elif resp.status_code // 100 != 2:
                msg = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                rawdata = resp.json()
                if not rawdata:
                    msg: str = f"获取历史数据为空，开始时间：{start}"
                    self.gateway.write_log(msg)
                    break

                buf: List[BarData] = []
                for data in rawdata:
                    dt = get_local_datetime(int(data[0]))

                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=float(data[5]),
                        open_price=float(data[1]),
                        high_price=float(data[2]),
                        low_price=float(data[3]),
                        close_price=float(data[4]),
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                history.extend(buf)
                
                begin_time: datetime = buf[0].datetime
                end_time: datetime = buf[-1].datetime

                msg: str = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin_time} - {end_time}"
                self.gateway.write_log(msg)

                
                if len(buf) < count:
                    break
                if start>req.end:
                    break

                start = bar.datetime
        return history
    #------------------------------------------------------------------------------------------------- 
    def new_local_orderid(self) -> str:
        """
        生成local_orderid
        """
        with self.order_count_lock:
            self.order_count += 1
            local_orderid = str(self.connect_time+self.order_count)
            return local_orderid
    #------------------------------------------------------------------------------------------------- 
    def send_order(self, req: OrderRequest) -> str:
        """
        发送委托单
        """
        local_orderid =req.symbol + "-" + self.new_local_orderid()
        order = req.create_order_data(
            local_orderid,
            self.gateway_name
        )
        if order.type==OrderType.STOP:
            data = {
                "symbol": req.symbol,
                "marginCoin":self.get_margin_coin(req.symbol),
                "clientOid": local_orderid,
                "triggerPrice": str(req.price),
                "triggerType":"fill_price",
                "size": str(req.volume),
                "side": DIRECTION_VT2BITGETS.get(req.direction),
                "orderType": ORDERTYPE_VT2BITGETS.get(OrderType.MARKET),
            }

            if req.offset == Offset.CLOSE:
                data["reduceOnly"] = True

            self.add_request(
                method="POST",
                path="/api/mix/v1/plan/placePlan",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_error=self.on_send_order_error,
                on_failed=self.on_send_order_failed
            )
        else:
            data = {
                "symbol": req.symbol,
                "marginCoin":self.get_margin_coin(req.symbol),
                "clientOid": local_orderid,
                "price": str(req.price),
                "size": str(req.volume),
                "side": DIRECTION_VT2BITGETS.get(req.direction),
                "orderType": ORDERTYPE_VT2BITGETS.get(req.type),
                "timeInForceValue": "normal"
            }

            if req.offset == Offset.CLOSE:
                data["reduceOnly"] = True

            self.add_request(
                method="POST",
                path="/api/mix/v1/order/placeOrder",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_error=self.on_send_order_error,
                on_failed=self.on_send_order_failed
            )

        self.gateway.on_order(order)
        return order.vt_orderid
    #------------------------------------------------------------------------------------------------- 
    def cancel_order(self, req: CancelRequest) -> Request:
        """
        取消委托单
        """
        order: OrderData = self.gateway.get_order(req.orderid)
        #计划委托单撤单
        if order.type==OrderType.STOP:
            data = {
                "symbol": req.symbol,
                "marginCoin":self.get_margin_coin(req.symbol),
                "clientOid":req.orderid,
                "planType":"normal_plan"
            }
            self.add_request(
                method="POST",
                path="/api/mix/v1/plan/cancelPlan",
                callback=self.on_cancel_order,
                on_failed=self.on_cancel_order_failed,
                data=data,
                extra=order
            )
        #普通委托单撤单
        else:
            data = {
                "symbol": req.symbol,
                "marginCoin":self.get_margin_coin(req.symbol),
                "clientOid":req.orderid
            }
            self.add_request(
                method="POST",
                path="/api/mix/v1/order/cancel-order",
                callback=self.on_cancel_order,
                on_failed=self.on_cancel_order_failed,
                data=data,
                extra=order
            )
    #------------------------------------------------------------------------------------------------- 
    def on_query_account(self, data: dict, request: Request) -> None:
        """
        收到账户数据回报
        """
        if self.check_error(data, "查询账户"):
            return
        for account_data in data["data"]:
            account = AccountData(
                accountid=account_data["marginCoin"] + "_" + self.gateway_name,
                balance= float(account_data["equity"]),
                frozen=float(account_data["locked"]),
                gateway_name=self.gateway_name,
            )
            if account.balance:
                self.gateway.on_account(account)
                
        self.gateway.write_log("账户资金查询成功")

    #------------------------------------------------------------------------------------------------- 
    def on_query_order(self, data: dict, request: Request) -> None:
        """
        收到委托回报
        """

        if self.check_error(data, "查询活动委托"):
            return
        data = data["data"]
        if not data:
            return
        for order_data in data:
            order_datetime =  get_local_datetime(int(order_data["cTime"]))

            order = OrderData(
                orderid=order_data["clientOid"],
                symbol=order_data["symbol"],
                exchange=Exchange.BITGET,
                price=order_data["price"],
                volume=order_data["size"],
                type=ORDERTYPE_BITGETS2VT[order_data["orderType"]],
                direction=DIRECTION_BITGETS2VT[order_data["side"]],
                traded=float(order_data["filledQty"]),
                status=STATUS_BITGETS2VT[order_data["state"]],
                datetime= order_datetime,
                gateway_name=self.gateway_name,
            )
            if order_data["reduceOnly"]:
                order.offset = Offset.CLOSE

            self.gateway.on_order(order)

        self.gateway.write_log("当前委托信息查询成功")
    def on_query_order_Algo(self, data: dict, request: Request) -> None:
        """
        收到委托回报
        """

        if self.check_error(data, "查询活动委托"):
            return
        data = data["data"]
        if not data:
            return
        for order_data in data:
            order_datetime =  get_local_datetime(int(order_data["cTime"]))

            order = OrderData(
                orderid=order_data["clientOid"],
                symbol=order_data["symbol"],
                exchange=Exchange.BITGET,
                price=order_data["triggerPrice"],
                volume=order_data["size"],
                type=OrderType.STOP,
                direction=DIRECTION_BITGETS2VT[order_data["side"]],
                traded=0.0,
                status=PLANSTATUS_BITGETS2VT[order_data["status"]],
                datetime= order_datetime,
                gateway_name=self.gateway_name,
            )
            if order_data["reduceOnly"]:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)

        self.gateway.write_log("计划委托信息查询成功")
    #------------------------------------------------------------------------------------------------- 
    def on_query_contract(self, data: dict, request: Request) -> None:
        """
        收到合约参数回报
        """
        if self.check_error(data, "查询合约"):
            return
        for contract_data in data["data"]:
            price_place = contract_data["pricePlace"]
            contract = ContractData(
                symbol=contract_data["symbol"],
                exchange=Exchange.BITGET,
                name=contract_data["symbolName"],
                pricetick=float(contract_data["priceEndStep"]) * float(f"1e-{price_place}"),
                size=20,    # 合约杠杆
                min_volume=float(contract_data["minTradeNum"]),
                product=Product.FUTURES,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True
            )
            # 保存交割合约名称
            if contract.name[-1].isdigit():
                self.delivery_date_map[contract.symbol] = contract.name
            self.gateway.on_contract(contract)
            if contract.vt_symbol not in self.all_contracts:
                self.all_contracts.append(contract.vt_symbol)
        product_type = contract_data["supportMarginCoins"][0]
        self.gateway.write_log(f"交易接口：{self.gateway_name}，{product_type}合约信息查询成功")
        self.contract_inited = True
    #------------------------------------------------------------------------------------------------- 
    def on_send_order(self, data: dict, request: Request) -> None:
        """
        """
        order = request.extra

        if self.check_error(data, "委托"):
            order.status = Status.REJECTED
            self.gateway.on_order(order)
    #------------------------------------------------------------------------------------------------- 
    def on_send_order_failed(self, status_code, request: Request) -> None:
        """
        收到委托失败回报
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    #------------------------------------------------------------------------------------------------- 
    def on_send_order_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)
    #------------------------------------------------------------------------------------------------- 
    def on_cancel_order(self, data: dict, request: Request) -> None:
        """
        """
        self.check_error(data, "撤单")
    #------------------------------------------------------------------------------------------------- 
    def on_cancel_order_failed(
        self,
        status_code,
        request: Request
    ) -> None:
        """
        收到撤单失败回报
        """
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    #------------------------------------------------------------------------------------------------- 
    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ) -> None:
        """
        Callback to handler request exception.
        """
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )
    #------------------------------------------------------------------------------------------------- 
    def check_error(self, data: dict, func: str = "") -> bool:
        """
        """
        if data["msg"] == "success":
            return False

        error_code = data["code"]
        error_msg = data["msg"]
        self.gateway.write_log(f"{func}请求出错，代码：{error_code}，信息：{error_msg}")
        return True

#------------------------------------------------------------------------------------------------- 
class BitGetSWebsocketApiBase(WebsocketClient):
    """
    """

    def __init__(self, gateway):
        """
        """
        super(BitGetSWebsocketApiBase, self).__init__()

        self.gateway: BitGetSGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.passphrase: str = ""
        self.count = 0

    def send_packet(self, packet: dict):
        """
        发送数据包字典到服务器。

        如果需要发送非json数据，请重载实现本函数。
        """
        if self._ws:
            if packet in ["pong","ping"]:
                text = packet
            else:
                text: str = json.dumps(packet)
            self._record_last_sent_text(text)

            coro: coroutine = self._ws.send_str(text)
            run_coroutine_threadsafe(coro, self._loop)
    def unpack_data(self, data: str):
        """
        对字符串数据进行json格式解包

        如果需要使用json以外的解包格式，请重载实现本函数。
        """
        if data in ["pong","ping"]:
            return data
        else:
            return json.loads(data)
    #------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        key: str,
        secret: str,
        passphrase: str,
        url: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        """
        self.key = key
        self.secret = secret
        self.passphrase = passphrase

        self.init(url, proxy_host, proxy_port)
        self.start()
        self.gateway.event_engine.register(EVENT_TIMER,self.send_ping)
    #------------------------------------------------------------------------------------------------- 
    def send_ping(self,event):
        self.count +=1
        if self.count < 20:
            return
        self.count = 0
        self.send_packet("ping")
    #------------------------------------------------------------------------------------------------- 
    def login(self) -> int:
        """
        """
        timestamp = str(int(time()))
        message = timestamp + "GET" + "/user/verify"
        signature = create_signature(self.secret,message)
        params = {
            "op":"login",
            "args":[
                {
                    "apiKey":self.key,
                    "passphrase":self.passphrase,
                    "timestamp":timestamp,
                    "sign":signature
                }
            ]
        }
        return self.send_packet(params)
    #------------------------------------------------------------------------------------------------- 
    def on_login(self, packet) -> None:
        """
        """
        pass
    #------------------------------------------------------------------------------------------------- 
    def on_data(self, packet):
        pass
    #------------------------------------------------------------------------------------------------- 
    def on_packet(self, packet:Union[str,dict]) -> None:
        """
        """
        if packet == "pong":
            return
        if "event" in packet:
            if packet["event"] == "login" and packet["code"] == 0:
                self.on_login()
            elif packet["event"] == "error":
                self.on_error_msg(packet)
        else:
            self.on_data(packet)
    #------------------------------------------------------------------------------------------------- 
    def on_error_msg(self, packet) -> None:
        """
        """
        msg = packet["msg"]
        self.gateway.write_log(f"交易接口：{self.gateway_name} WebSocket API收到错误回报，回报信息：{msg}")
#------------------------------------------------------------------------------------------------- 
class BitGetSDataWebsocketApi(BitGetSWebsocketApiBase):
    """
    """

    def __init__(self, gateway: BitGetSGateway):
        """
        """
        super().__init__(gateway)

        self.ticks:Dict[str,TickData] = {}
    #------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        key: str,
        secret: str,
        passphrase:str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        """
        super().connect(
            key,
            secret,
            passphrase,
            WEBSOCKET_DATA_HOST,
            proxy_host,
            proxy_port
        )
    #------------------------------------------------------------------------------------------------- 
    def on_connected(self) -> None:
        """
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，行情Websocket API连接成功")

        for inst_id in list(self.ticks):
            self.subscribe_data(inst_id)
    #-------------------------------------------------------------------------------------------------
    def on_disconnected(self):
        """
        ws行情断开回调
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，行情Websocket API连接断开")
    #------------------------------------------------------------------------------------------------- 
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅合约
        """
        # 等待rest合约数据推送完成再订阅
        while not self.gateway.rest_api.contract_inited:
            sleep(1)

        tick = TickData(
            symbol=req.symbol,
            name=req.symbol,
            exchange=Exchange.BITGET,
            datetime=datetime.now(CHINA_TZ),
            gateway_name=self.gateway_name,
        )
        symbol = tick.symbol
        
        # 交割合约inst_id赋值
        if symbol[-1].isdigit():
            inst_id = self.gateway.rest_api.delivery_date_map.get(symbol,"")
        else:
            inst_id = symbol.split("_")[0]
        self.ticks[inst_id] = tick
        self.subscribe_data(inst_id)
    #------------------------------------------------------------------------------------------------- 
    def topic_subscribe(self,channel:str,inst_id:str):
        """
        主题订阅
        """
        req = {
            "op":"subscribe",
            "args":[
                {
                    "instType":"MC",
                    "channel":channel,
                    "instId":inst_id
                }
            ]
        }
        self.send_packet(req)
    #------------------------------------------------------------------------------------------------- 
    def subscribe_data(self, inst_id: str) -> None:
        """
        订阅市场深度主题
        """
        # 过滤过期inst_id
        if not inst_id:
            return
        # 订阅tick，行情深度，最新成交
        channels = ["ticker","books5","tradeNew"]
        for channel in channels:
            self.topic_subscribe(channel,inst_id)
    #------------------------------------------------------------------------------------------------- 
    def on_data(self, packet) -> None:
        """
        """
        channel = packet["arg"]["channel"]    #全量推送snapshot，增量推送update
        if "action" in packet:
            if channel == "ticker":
                self.on_tick(packet["data"])
            elif channel == "books5":
                self.on_depth(packet)
            elif channel == "tradeNew":
                self.on_public_trade(packet)
    #------------------------------------------------------------------------------------------------- 
    def on_tick(self, data: dict) -> None:
        """
        收到tick数据推送
        """
        for tick_data in data:
            inst_id = tick_data["instId"]
            tick = self.ticks[inst_id]
            tick.name = inst_id
            tick.datetime = get_local_datetime(tick_data["systemTime"])
            tick.open_price = float(tick_data["openUtc"])
            tick.high_price = float(tick_data["high24h"])
            tick.low_price = float(tick_data["low24h"])
            tick.last_price = float(tick_data["last"])
            tick.open_interest = float(tick_data["holding"])
            tick.volume = float(tick_data["baseVolume"])    #quoteVolume：usd成交量，baseVolume：本币成交量
            tick.bid_price_1 = float(tick_data["bestBid"])
            tick.bid_volume_1 = float(tick_data["bidSz"])
            tick.ask_price_1 = float(tick_data["bestAsk"])
            tick.ask_volume_1 = float(tick_data["askSz"])
            self.gateway.on_tick(copy(tick))
    #------------------------------------------------------------------------------------------------- 
    def on_depth(self, packet: dict) -> None:
        """
        行情深度推送
        """
        data = packet["data"][0]
        inst_id = packet["arg"]["instId"]
        tick = self.ticks[inst_id]

        tick.datetime = get_local_datetime(int(data["ts"]))

        bids = data["bids"]
        asks = data["asks"]
        for index in range(len(bids)):
            price, volume = bids[index]
            tick.__setattr__("bid_price_" + str(index + 1), float(price))
            tick.__setattr__("bid_volume_" + str(index + 1), float(volume))

        for index in range(len(asks)):
            price, volume = asks[index]
            tick.__setattr__("ask_price_" + str(index + 1), float(price))
            tick.__setattr__("ask_volume_" + str(index + 1), float(volume))

        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    #------------------------------------------------------------------------------------------------- 
    def on_public_trade(self,packet):
        """
        收到公开成交回报
        """
        data = packet["data"][0]
        inst_id = packet["arg"]["instId"]
        tick = self.ticks[inst_id]
        tick.last_price = float(data["p"])
        tick.datetime = get_local_datetime(data["ts"])
#------------------------------------------------------------------------------------------------- 
class BitGetSTradeWebsocketApi(BitGetSWebsocketApiBase):
    """
    """
    def __init__(self, gateway: BitGetSGateway):
        """
        """
        self.trade_count = 0
        super().__init__(gateway)
    #------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        key: str,
        secret: str,
        passphrase:str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        """
        super().connect(
            key,
            secret,
            passphrase,
            WEBSOCKET_TRADE_HOST,
            proxy_host,
            proxy_port
        )
    #------------------------------------------------------------------------------------------------- 
    def subscribe_private(self) -> int:
        """
        订阅私有频道
        """
        inst_types = ["UMCBL","SUMCBL"]  #产品类型 UMCBL:专业合约私有频道,DMCBL:混合合约私有频道(币本位合约),CMCBL:USDC专业合约,SUMCBL模拟盘
        # 订阅持仓
        for inst_type in inst_types:
            req = {
                "op": "subscribe",
                "args": [{
                    "instType": inst_type,
                    "channel": "positions",
                    "instId": "default"
                }]
            }
            self.send_packet(req)
        # 订阅委托
        for inst_type in inst_types:
            req = {
                "op": "subscribe",
                "args": [{
                    "instType": inst_type,
                    "channel": "orders",
                    "instId": "default"
                }]
            }
            self.send_packet(req)
        # 订阅计划委托
        for inst_type in inst_types:
            req = {
                "op": "subscribe",
                "args": [{
                    "instType": inst_type,
                    "channel": "ordersAlgo",
                    "instId": "default"
                }]
            }
        self.send_packet(req)
    #------------------------------------------------------------------------------------------------- 
    def on_connected(self) -> None:
        """
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，交易Websocket API连接成功")
        self.login()
    #-------------------------------------------------------------------------------------------------
    def on_disconnected(self):
        """
        ws交易断开回调
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，交易Websocket API连接断开")
    #------------------------------------------------------------------------------------------------- 
    def on_login(self) -> None:
        """
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，交易Websocket API登录成功")
        self.subscribe_private()
    #------------------------------------------------------------------------------------------------- 
    def on_packet(self, packet:Union[str,dict]) -> None:
        """
        """
        if packet == "pong":
            return
        if "event" in packet:
            if packet["event"] == "login" and packet["code"] == 0:
                self.on_login()
            elif packet["event"] == "error":
                self.on_error_msg(packet)
        else:
            self.on_data(packet)
        ######在日志显示所有ws包
        self.gateway.write_log(packet)
        ########################
    def on_data(self, packet) -> None:
        """
        """
        channel = packet["arg"]["channel"]
        data = packet["data"]
        if "action" in packet:
            if channel == "positions":
                self.on_position(data)
            elif channel == "orders":
                self.on_order(data)
            elif channel == "ordersAlgo":
                self.on_order_Algo(data)
    #------------------------------------------------------------------------------------------------- 
    def on_order(self, raw: dict) -> None:
        """
        收到委托回报
        """
        for data in raw:
            if STATUS_BITGETS2VT[data["status"]]==Status.ALLTRADED:
                price=float(data["avgPx"])
            else:
                price=float(data["px"])
            order_datetime = get_local_datetime(data["cTime"])
            orderid = data["clOrdId"]
            order = OrderData(
                symbol=data["instId"],
                exchange=Exchange.BITGET,
                orderid=orderid,
                type=ORDERTYPE_BITGETS2VT[data["ordType"]],
                direction=DIRECTION_BITGETS2VT[data["tS"]],
                price=price,
                volume=float(data["sz"]),
                traded=float(data["accFillSz"]),
                status=STATUS_BITGETS2VT[data["status"]],
                datetime = order_datetime,
                gateway_name=self.gateway_name
            )
            self.gateway.on_order(order)

            # 推送成交事件
            if STATUS_BITGETS2VT[data["status"]]!=Status.ALLTRADED:
                return
            self.trade_count += 1
            trade = TradeData(
                symbol=order.symbol,
                exchange=Exchange.BITGET,
                orderid=order.orderid,
                tradeid=str(self.trade_count),
                direction=order.direction,
                offset=order.offset,
                price=float(data["fillPx"]),
                volume=float(data["fillSz"]),
                datetime= get_local_datetime(int(data["fillTime"])),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)
    #------------------------------------------------------------------------------------------------- 
    def on_order_Algo(self, raw: dict) -> None:
        """
        收到计划委托回报
        """
        for data in raw:
            order_datetime = get_local_datetime(int(data["cTime"]))
            orderid = data["cOid"]
            if PLANSTATUS_BITGETS2VT[data["state"]]==Status.ALLTRADED:
                traded=float(data["sz"])
            else:
                traded=None
            order = OrderData(
                symbol=data["instId"],
                exchange=Exchange.BITGET,
                orderid=orderid,
                type=OrderType.STOP,
                direction=DIRECTION_BITGETS2VT[data["tS"]],
                price=float(data["ordPx"]),
                volume=float(data["sz"]),
                traded=traded,
                status=PLANSTATUS_BITGETS2VT[data["state"]],
                datetime = order_datetime,
                gateway_name=self.gateway_name
            )
            self.gateway.on_order(order)

            # 推送成交事件
            if not order.traded:
                return
            self.trade_count += 1
            trade = TradeData(
                symbol=order.symbol,
                exchange=Exchange.BITGET,
                orderid=order.orderid,
                tradeid=str(self.trade_count),
                direction=order.direction,
                offset=order.offset,
                price=float(data["ordPx"]),
                volume=float(data["sz"]),
                datetime= get_local_datetime(int(data["uTime"])),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)
    #------------------------------------------------------------------------------------------------- 
    def on_position(self,data:dict):
        """
        收到持仓回报
        """
        for pos_data in data:
            position = PositionData(
                symbol = pos_data["instId"],
                exchange = Exchange.BITGET,
                direction = HOLDSIDE_BITGETS2VT[pos_data["holdSide"]],
                volume = float(pos_data["available"]),
                price = float(pos_data["averageOpenPrice"]),
                pnl = float(pos_data["upl"]),
                gateway_name = self.gateway_name
            )
            self.gateway.on_position(position)
#------------------------------------------------------------------------------------------------- 
def create_signature(secret:str,message:str):
    mac = hmac.new(bytes(secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256').digest()
    sign_str =  base64.b64encode(mac).decode()
    return sign_str

def get_local_datetime(timestamp: float) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    dt: datetime = dt.replace(tzinfo=CHINA_TZ)
    return dt
