import sys;sys.path.append(".")
import logging
from logging.handlers import RotatingFileHandler
import requests
import time
import typing
import collections
from urllib.parse import urlencode
import hmac
import hashlib
import threading
from dados.models import *
from dados.strategies import MovingaverageStrategy
import pandas as pd
import traceback

# Configuração do logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Definir nível do logger principal

formatter = logging.Formatter('%(asctime)s %(levelname)s :: %(message)s')
file_handler = RotatingFileHandler('info.log', maxBytes=5*1024*1024, backupCount=3)  # 5 MB por arquivo, com até 3 backups
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
from dados.utilsTelegram import BotTelegram
class BinanceClient:
    def __init__(self, public_key: str, secret_key: str, testnet: bool, futures: bool,ChaveTelegram="", idTelegram=""):

        """
        https://binance-docs.github.io/apidocs/futures/en
        :param public_key:
        :param secret_key:
        :param testnet:
        :param futures: if False, the Client will be a Spot API Client
        """

        self.futures = futures
        self.executar = False
        if self.futures:
            self.platform = "binance_futures"
            if testnet:
                self._base_url = "https://testnet.binancefuture.com"
                self._wss_url = "wss://stream.binancefuture.com/ws"
            else:
                self._base_url = "https://fapi.binance.com"
                self._wss_url = "wss://fstream.binance.com/ws"
        else:
            self.platform = "binance_spot"
            if testnet:
                self._base_url = "https://testnet.binance.vision"
                self._wss_url = "wss://testnet.binance.vision/ws"
            else:
                self._base_url = "https://api.binance.com"
                self._wss_url = "wss://stream.binance.com:9443/ws"

        self._public_key = public_key
        self._secret_key = secret_key
        self.telegram = BotTelegram(ID=idTelegram,CHAVE=ChaveTelegram)
        self.telegramOK = True if idTelegram!="" else False
        self._headers = {'X-MBX-APIKEY': self._public_key}

        self.contracts = self.get_contracts()
        self.balances = self.get_balances()

        self.prices = dict()
        self.strategies: typing.Dict[
            int, MovingaverageStrategy] = dict()
        self.rodar()

    def timestamp(self):
        try:
            response = requests.get("https://api.binance.com/api/v3/time")
            if response.status_code == 200:
                return response.json()['serverTime']
            else:
                return int(time.time() * 1000)
        except:
            return int(time.time() * 1000)
        
    def _add_log(self, msg: str, tipo='INFO'):

        """
        Add a log to the list so that it can be picked by the update_ui() method of the root component.
        :param msg:
        :return:
        """
        if tipo=='INFO':
            print(msg)
            logger.info(msg.replace("="*100,""))
        elif tipo=='CRITICO':
            print(msg)
            logger.critical(msg.replace("="*100,""))
        elif tipo=='ERRO':
            print(msg)
            logger.error(msg.replace("="*100,""))      
        else:
            logger.warning(msg.replace("="*100,""))      

    def _generate_signature(self, data: typing.Dict) -> str:

        """
        Generate a signature with the HMAC-256 algorithm.
        :param data: Dictionary of parameters to be converted to a query string
        :return:
        """

        return hmac.new(self._secret_key.encode(), urlencode(data).encode(), hashlib.sha256).hexdigest()

    def _make_request(self, method: str, endpoint: str, data: typing.Dict):

        """
        Wrapper that normalizes the requests to the REST API and error handling.
        :param method: GET, POST, DELETE
        :param endpoint: Includes the /api/v1 part
        :param data: Parameters of the request
        :return:
        """

        if method == "GET":
            try:
                response = requests.get(self._base_url + endpoint, params=data, headers=self._headers)
            except:  # Takes into account any possible error, most likely network errors
                self._add_log(f"Connection error while making {method} request to {endpoint}: {traceback.format_exc()}",tipo="ERRO")
                return None

        elif method == "POST":
            try:
                response = requests.post(self._base_url + endpoint, params=data, headers=self._headers)
            except:
                self._add_log(f"Connection error while making {method} request to {endpoint}: {traceback.format_exc()}",tipo="ERRO")
                return None

        elif method == "DELETE":
            try:
                response = requests.delete(self._base_url + endpoint, params=data, headers=self._headers)
            except:
                self._add_log(f"Connection error while making {method} request to {endpoint}: {traceback.format_exc()}",tipo="ERRO")
                return None
        else:
            raise ValueError()

        if response.status_code == 200:  # 200 is the response code of successful requests
            return response.json()
        else:
            self._add_log(f"Error while making {method} request to {endpoint}: {response.json()} (error code {response.status_code})",tipo="ERRO")
            return None

    def get_contracts(self) -> typing.Dict[str, Contract]:

        """
        Get a list of symbols/contracts on the exchange to be displayed in the OptionMenus of the interface.
        :return:
        """

        if self.futures:
            exchange_info = self._make_request("GET", "/fapi/v1/exchangeInfo", dict())
        else:
            exchange_info = self._make_request("GET", "/api/v3/exchangeInfo", dict())

        contracts = dict()

        if exchange_info is not None:
            for contract_data in exchange_info['symbols']:
                contracts[contract_data['symbol']] = Contract(contract_data, self.platform)

        return collections.OrderedDict(sorted(contracts.items()))  # Sort keys of the dictionary alphabetically

    def get_historical_candles(self, contract: Contract, interval: str, limit = 1000) -> typing.List[Candle]:

        """
        Get a list of the most recent candlesticks for a given symbol/contract and interval.
        :param contract:
        :param interval: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
        :return:
        """

        data = dict()
        data['symbol'] = contract.symbol
        data['interval'] = interval
        data['limit'] = limit  # The maximum number of candles is 1000 on Binance Spot

        if self.futures:
            raw_candles = self._make_request("GET", "/fapi/v1/klines", data)
        else:
            raw_candles = self._make_request("GET", "/api/v3/klines", data)

        candles = []

        if raw_candles is not None:
            for c in raw_candles:
                candles.append(Candle(c, interval, self.platform))

        return candles

    def get_bid_ask(self, contract: Contract) -> typing.Dict[str, float]:

        """
        Get a snapshot of the current bid and ask price for a symbol/contract, to be sure there is something
        to display in the Watchlist.
        :param contract:
        :return:
        """

        data = dict()
        data['symbol'] = contract.symbol

        if self.futures:
            ob_data = self._make_request("GET", "/fapi/v1/ticker/bookTicker", data)
        else:
            ob_data = self._make_request("GET", "/api/v3/ticker/bookTicker", data)

        if ob_data is not None:
            if contract.symbol not in self.prices:  # Add the symbol to the dictionary if needed
                self.prices[contract.symbol] = {'bid': float(ob_data['bidPrice']), 'ask': float(ob_data['askPrice'])}
            else:
                self.prices[contract.symbol]['bid'] = float(ob_data['bidPrice'])
                self.prices[contract.symbol]['ask'] = float(ob_data['askPrice'])

            return self.prices[contract.symbol]

    def get_balances(self) -> typing.Dict[str, Balance]:

        """
        Get the current balance of the account, the data is different between Spot and Futures
        :return:
        """

        data = dict()
        data['timestamp'] = self.timestamp()
        data['signature'] = self._generate_signature(data)

        balances = dict()

        if self.futures:
            account_data = self._make_request("GET", "/fapi/v2/account", data)
        else:
            account_data = self._make_request("GET", "/api/v3/account", data)

        if account_data is not None:
            if self.futures:
                for a in account_data['assets']:
                    balances[a['asset']] = Balance(a, self.platform)
            else:
                for a in account_data['balances']:
                    balances[a['asset']] = Balance(a, self.platform)

        return balances


    def place_orderV2(self, contract: Contract, order_type: str, quantity: float, side: str, price=None,
                    tif=None, tipo = 'NEW') -> OrderStatus:
        """
        Place an order. Based on the order_type, the price and tif arguments are not required
        :param contract:
        :param order_type: LIMIT, MARKET, STOP, TAKE_PROFIT, LIQUIDATION
        :param quantity:
        :param side:
        :param price:
        :param tif:
        :return:
        """
        operacao = "SL" if "STOP" in order_type.upper() else ""
        data = dict()
        data['symbol'] = contract.symbol
        data['side'] = side.upper()
        data['quantity'] = round(int(quantity / contract.lot_size) * contract.lot_size, 8)  # int() to round down
        data['type'] = order_type.upper()  # Makes sure the order type is in uppercase

        if operacao.upper()!="SL":
            if price is not None:
                data['price'] = round(round(price / contract.tick_size) * contract.tick_size, 8)
                data['price'] = '%.*f' % (contract.price_decimals, data['price'])  # Avoids scientific notation
        else:
            data['reduceOnly'] = True
            data['priceProtect'] = True
            if price is not None:
                data['stopPrice'] = round(round(price / contract.tick_size) * contract.tick_size, 8)
                data['stopPrice'] = '%.*f' % (contract.price_decimals, data['stopPrice'])  # Avoids scientific notation
        
        if tif is not None:
            data['timeInForce'] = tif

        data['timestamp'] = self.timestamp()
        data['signature'] = self._generate_signature(data)
        
        LOG = "\n"+"="*100+f"\nCRIANDO ORDEM {operacao} PARA {data['symbol']}\nLADO: {data['side']}\nQUANTIDADE: {data['quantity']}\nTIPO: {tipo}\nHORA: {datetime.datetime.today():%H:%M:%S %d/%m/%Y}\n"
        if price:
            LOG+=f"PREÇO: {price}\n"
        if self.futures:
            order_status = self._make_request("POST", "/fapi/v1/order", data)
        else:
            order_status = self._make_request("POST", "/api/v3/order", data)
        if order_status is not None:

            if not self.futures:
                if order_status['status'] == "FILLED":
                    order_status['avgPrice'] = self._get_execution_price(contract, order_status['orderId'])
                else:
                    order_status['avgPrice'] = 0

            order_status = OrderStatus(order_status, self.platform)
        LOG +="="*100
        self._add_log(LOG)
        return order_status


    def cancel_order(self, contract: Contract, order_id: int) -> OrderStatus:

        data = dict()
        data['orderId'] = order_id
        data['symbol'] = contract.symbol

        data['timestamp'] = self.timestamp()
        data['signature'] = self._generate_signature(data)

        if self.futures:
            order_status = self._make_request("DELETE", "/fapi/v1/order", data)
        else:
            order_status = self._make_request("DELETE", "/api/v3/order", data)

        if order_status is not None:
            if not self.futures:
                # Get the average execution price based on the recent trades
                order_status['avgPrice'] = self._get_execution_price(contract, order_id)
            order_status = OrderStatus(order_status, self.platform)

        return order_status

    def _get_execution_price(self, contract: Contract, order_id: int) -> float:

        """
        For Binance Spot only, find the equivalent of the 'avgPrice' key on the futures side.
        The average price is the weighted sum of each trade price related to the order_id
        :param contract:
        :param order_id:
        :return:
        """

        data = dict()
        data['timestamp'] = self.timestamp()
        data['symbol'] = contract.symbol
        data['signature'] = self._generate_signature(data)

        trades = self._make_request("GET", "/api/v3/myTrades", data)

        avg_price = 0

        if trades is not None:

            executed_qty = 0
            for t in trades:
                if t['orderId'] == order_id:
                    executed_qty += float(t['qty'])

            for t in trades:
                if t['orderId'] == order_id:
                    fill_pct = float(t['qty']) / executed_qty
                    avg_price += (float(t['price']) * fill_pct)  # Weighted sum

        return round(round(avg_price / contract.tick_size) * contract.tick_size, 8)

    def get_order_status(self, contract: Contract, order_id: int) -> OrderStatus:

        data = dict()
        data['timestamp'] = self.timestamp()
        data['symbol'] = contract.symbol
        data['orderId'] = order_id
        data['signature'] = self._generate_signature(data)

        if self.futures:
            order_status = self._make_request("GET", "/fapi/v1/order", data)
        else:
            order_status = self._make_request("GET", "/api/v3/order", data)

        if order_status is not None:
            if not self.futures:
                if order_status['status'] == "FILLED":
                    # Get the average execution price based on the recent trades
                    order_status['avgPrice'] = self._get_execution_price(contract, order_id)
                else:
                    order_status['avgPrice'] = 0

            order_status = OrderStatus(order_status, self.platform)

        return order_status

    def pegarPosicoesAbertas(self):

        for strategy in self.strategies.values(): #PEGANDO POSICOES ABERTAS
            strategy.trades = []
            strategy.last_sl = None
            strategy.stop_price = None
            strategy.stop_mov = None
            data={}
            data['timestamp'] = self.timestamp()
            data['symbol'] = strategy.contract.symbol
            data['signature'] = self._generate_signature(data)
            dadosPosicao = self._make_request("GET", '/fapi/v2/positionRisk', data) 
            if len(dadosPosicao)==0:
                continue
            posicoesAberta = [x for x in dadosPosicao if float(x['positionAmt'])!=0]
            if len(posicoesAberta)>0: # SE TIVER ATIVO COMPRADO PARA AQUELA strategy
                for posicaoAberta in posicoesAberta:
                    self._add_log("="*100)
                    self._add_log(f"ATIVO: {posicaoAberta['symbol']}")
                    self._add_log(f"POSIÇÂO ABERTA ENCONTRADA") #POSICAO DE STOP LOSS
                    resp=self._make_request("GET", '/fapi/v1/allOrders', data)
                    
                    dadosPosicoes = [x for x in resp if x['status'].upper()=='NEW' and "STOP" in x['type'].upper()]
                    if len(dadosPosicoes)>1:
                        self._add_log(f"ENCONTRADA MAIS DE UMA ORDEM SL EM ABERTO")
                        for i,posicao in enumerate(dadosPosicoes[:-1]): #CANCELA TODAS AS ORDENS, MENOS A ULTIMA
                            self._add_log(f"CANCELANDO POSIÇÕES SL ANTERIORES PARA {posicao['symbol']} N{i+1}")
                            self.cancel_order(strategy.contract,posicao['orderId'])
                    if len(dadosPosicoes)>0:
                        self._add_log("MANTENDO A ULTIMA ORDEM SL EM ABERTO PARA O ATIVO")
                        posicao = dadosPosicoes[-1] #ADICIONA A ULTIMA ORDEM PARA O PROGRAMA LER.
                        dadosPosicoesExecutadas = [x for x in resp if x['status'].lower()=='filled' and x['side'].lower()!=posicao['side'].lower()]
                        if len(dadosPosicoesExecutadas)>0:
                            posicaoExecutada = dadosPosicoesExecutadas[-1]
                            priceExecutado = posicaoExecutada['avgPrice']
                            new_trade = Trade({"time": posicaoExecutada['time'], "entry_price": float(priceExecutado),
                                                "contract": self.contracts[posicaoExecutada['symbol']], "strategy": 'Movingaverage', "side": posicaoExecutada['side'].upper(),
                                                "status": "open", "pnl": 0, "quantity": float(posicaoExecutada['origQty']),
                                                "entry_id": posicaoExecutada['orderId']})
                        else:
                            price = posicao['stopPrice']
                            new_trade = Trade({"time": posicao['time'], "entry_price": float(price),
                                                "contract": self.contracts[posicao['symbol']], "strategy": 'Movingaverage', "side": 'BUY' if posicao['side'].upper()=='SELL' else 'SELL',
                                                "status": "open", "pnl": 0, "quantity": float(posicao['origQty']),
                                                "entry_id": posicao['orderId']})
                        if new_trade.side.upper() == "BUY":
                            strategy.stop_price = (new_trade.entry_price * (1 - strategy.stop_loss / 100))
                        else:
                            strategy.stop_price = (new_trade.entry_price * (1 + strategy.stop_loss / 100))
                        strategy.trades.append(new_trade)
                        ORDEM = OrderStatus(posicao, self.platform)
                        strategy.last_sl = ORDEM
                        strategy.stop_mov = strategy.stop_price
                        threading.Thread(target=strategy.actualize_stop_loss, args=(new_trade,self,)).start() #ATUALIZA O STOPLOSS A PARTIR DO AVR
                        strategy.ongoing_position = True
                    else:
                        self._add_log(f"NENHUMA ORDEM SL EM ABERTO. *CHECAR ATIVO NA BINANCE*")
                        strategy.ongoing_position = False
                    self._add_log("="*100)
            else: #SE NÂO TIVER ATIVO COMPRADO, FECHA ORDENS EM ABERTO
                resp=self._make_request("GET", '/fapi/v1/allOrders', data)
                dadosPosicoes = [x for x in resp if x['status']=='NEW']
                for i,posicao in enumerate(dadosPosicoes):
                    self.cancel_order(strategy.contract,posicao['orderId'])
                    self._add_log(f"CANCELANDO POSIÇÕES ANTERIORES PARA {posicao['symbol']} N{i+1}")

    def lerDadosEntrada(self):
        try:
            dados = pd.read_excel("DadosEntrada.xlsx",sheet_name='DADOS').to_dict(orient='records')
        except:
            dados = pd.read_excel("BotBinance/DadosEntrada.xlsx",sheet_name='DADOS').to_dict(orient='records')
        self.strategies = {}
        for i,info in enumerate(dados): #CRIA AS ESTRATEGIAS A PARTIR DO EXCEL
            contrato = self.contracts[info['Contrato']]
            new_strategy = MovingaverageStrategy(self, contrato, 'Binance', info['TimeFrame'],info['% Balance '],info['TakeProfit'], info['StopLoss'], {'ema_fast':info['EmaFast'],'ema_slow':info['EmaSlow'],"atr_period":info['AtrPeriod'],"atr_multiplier":info['AtrMultiplier']})
            new_strategy.candles = self.get_historical_candles(contrato, info['TimeFrame'])[:-1]
            if len(new_strategy.candles) == 0:
                self._add_log( f'HISTORICO NAO ENCONTRADO PARA {contrato}')
                continue
            self.strategies[i] = new_strategy

    def RodarThreads(self,strategy):
        while strategy in self.strategies.values():
            try:
                ultimoDado=self.get_historical_candles(strategy.contract,strategy.tf,limit=3)[-2]
                self._on_message(contract=strategy.contract, candle=ultimoDado)
            except:
                self._add_log(f"ERRO AO PEGAR DADOS CANDLES {strategy.contract.symbol} {traceback.format_exc()}",tipo="CRITICO")
            time.sleep(30)


    def rodar(self,ignoreTelegram=False):
        self._add_log("="*100)
        self._add_log("Binance Futures Client successfully initialized")
        self.executar = True
        self.lerDadosEntrada()
        self.pegarPosicoesAbertas()
        if self.telegramOK and not ignoreTelegram: threading.Thread(target=self.telegram.GetMessageUser,args=(self,)).start()
        for strategy in self.strategies.values():
            threading.Thread(target=strategy.check_stop_loss_status,args=(self,)).start() #FICA SEMPRE VERIFICANDO O STOPLOSS
            threading.Thread(target=self.RodarThreads,args=(strategy,)).start() #FICA SEMPRE VERIFICANDO OS SINAIS PRA CADA ESTRATEGIA
        
    def PegarAcumulado(self):
        for estrategia in  self.strategies.values():
            acumulado = 0
            self._add_log(f'ESTRATÉGIA DO {estrategia.contract.symbol}')
            for trade in estrategia.trades:
                self._add_log(f"STATUS: {trade.status}, OPERAÇÃO: {trade.side.upper()}, QUANTIDADE: {trade.quantity},RESULTADO: {trade.pnl}")
                acumulado+=trade.pnl
            self._add_log(f"ACUMULADO DA OPERAÇÂO PARA {estrategia.contract.symbol}: {acumulado}")

    def _on_message(self, contract, candle):
        self.get_bid_ask(contract) #PEGANDO PREÇOS ATUALIZADOS
        for b_index, strat in self.strategies.items():
            if strat.contract.symbol == contract.symbol:
                for trade in strat.trades:
                    if trade.status == "open" and trade.entry_price is not None:
                        if trade.side.upper() == "BUY":
                            trade.pnl = (self.prices[contract.symbol]['bid'] - trade.entry_price) * trade.quantity
                        elif trade.side.upper() == "SELL":
                            trade.pnl = (trade.entry_price - self.prices[contract.symbol]['ask']) * trade.quantity

        for key, strat in self.strategies.items():
            if strat.contract.symbol == contract.symbol:
                res = strat.parse_trades(candle)  # Updates candlesticks
                strat.check_trade(res)


    def get_trade_size(self, contract: Contract, price: float, balance_pct: float):

        """
        Compute the trade size for the strategy module based on the percentage of the balance to use
        that was defined in the strategy component.
        :param contract:
        :param price: Used to convert the amount to invest into an amount to buy/sell
        :param balance_pct:
        :return:
        """

        self._add_log("Getting Binance trade size...")

        balance = self.get_balances()

        if balance is not None:
            if contract.quote_asset in balance:  # On Binance Spot, the quote asset isn't necessarily USDT
                if self.futures:
                    balance = balance[contract.quote_asset].wallet_balance
                else:
                    balance = balance[contract.quote_asset].free
            else:
                return None
        else:
            return None

        if balance*balance_pct<5:
            self._add_log("BALANCE % INSUFICIENTE PARA REALIZAR A ORDEM. FAÇA O AJUSTE PARA QUE O MINIMO SEJA 5 DOLARES")
            return None
        trade_size = (balance * balance_pct / 100) / price

        trade_size = round(round(trade_size / contract.lot_size) * contract.lot_size, 8)  # Removes extra decimals

        # self._add_log("Binance current %s balance = %s, trade size = %s", contract.quote_asset, balance, trade_size)

        return trade_size
    
    def pegarThreads(self):
        active_threads = threading.enumerate()
        self._add_log(f"Threads ativos: {len(active_threads)}")
        for thread in active_threads:
            self._add_log("="*100)
            self._add_log(f" - Thread name: {thread.name}")
            self._add_log(f"   Thread identificador: {thread.ident}")
            self._add_log(f"   Thread alive: {thread.is_alive()}")
            self._add_log("="*100)
        return [x.name for x in active_threads]
