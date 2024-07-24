import sys;sys.path.append(".")
import sys;sys.path.append("dados")
import threading
from typing import *
import time
import traceback
from threading import Timer
import pandas as pd
from models import *
if TYPE_CHECKING:  # Import the connector class names only for typing purpose (the classes aren't actually imported)
    from binance import BinanceClient

# TF_EQUIV is used in parse_trades() to compare the last candle timestamp to the new trade timestamp
TF_EQUIV = {"1m": 60, '3m':180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "12h": 43200, "1d": 86400}


class Strategy:
    def __init__(self, client: "BinanceClient", contract: Contract, exchange: str,
                 timeframe: str, balance_pct: float, take_profit: float, stop_loss: float, strat_name, other_params):

        self.client = client
        self.last_sl = []
        self.contract = contract
        self.exchange = exchange
        self.tf = timeframe
        self.tf_equiv = TF_EQUIV[timeframe] * 1000
        self.balance_pct = balance_pct
        self.take_profit = take_profit
        self.stop_loss = stop_loss
        self.other_params = other_params
        self.stop_price = []
        self.stop_mov = []
        self.OrdemTelegram = {'trade':None,'id':''}

        self.strat_name = strat_name

        self.ongoing_position = False

        self.candles: List[Candle] = []
        self.trades: List[Trade] = []
        self.logs = []

    def parse_trades(self, candle:Candle) -> str:

        """
        Parse new trades coming in from the websocket and update the Candle list based on the timestamp.
        :param price: The trade price
        :param size: The trade size
        :param timestamp: Unix timestamp in milliseconds
        :return:
        """

        timestamp_diff = self.client.timestamp() - candle.timestamp
        if timestamp_diff >= self.tf_equiv*3:
            self.client._add_log(f"{self.exchange} {self.contract.symbol}: {timestamp_diff} milliseconds of difference between the current time and the trade time",tipo='ALERTA')

        last_candle = self.candles[-1]

        # Same Candle

        if candle.timestamp < last_candle.timestamp + self.tf_equiv:
            # Check Take profit / Stop loss
            if self.client.executar:
                for trade in self.trades:
                    if trade.status == "open" and trade.entry_price is not None:
                            self._check_tp_sl(trade)
            return "same_candle"

        # Missing Candle(s)

        elif candle.timestamp >= last_candle.timestamp + 2 * self.tf_equiv:

            missing_candles = int((candle.timestamp - last_candle.timestamp) / self.tf_equiv) - 1

            self.client._add_log(f"{self.exchange} missing {missing_candles} candles for {self.contract.symbol} {self.tf} ({candle.timestamp} {last_candle.timestamp})")


            for missing in range(missing_candles):
                new_ts = last_candle.timestamp + self.tf_equiv
                candle_info = {'ts': new_ts, 'open': last_candle.close, 'high': last_candle.close,
                               'low': last_candle.close, 'close': last_candle.close, 'volume': 0}
                new_candle = Candle(candle_info, self.tf, "parse_trade")

                self.candles.append(new_candle)

                last_candle = new_candle

            new_ts = last_candle.timestamp + self.tf_equiv
            candle_info = {'ts': new_ts, 'open': candle.open, 'high': candle.high, 'low': candle.low, 'close': candle.close, 'volume': candle.volume}
            new_candle = Candle(candle_info, self.tf, "parse_trade")

            self.candles.append(new_candle)
            return "new_candle"

        # New Candle

        elif candle.timestamp >= last_candle.timestamp + self.tf_equiv:
            self.candles.append(candle)
            self.client._add_log(f"{self.exchange} New candle for {self.contract.symbol} {self.tf}",tipo='ALERTA')
            return "new_candle"

    def _check_order_status(self, order_id):
        order_status = self.client.get_order_status(self.contract, order_id)
        if order_status is not None:

            self.client._add_log(f"{self.exchange} id:{order_id} order status: {order_status.status}",tipo="ALERTA")
            if order_status.status == "filled":
                for trade in self.trades:
                    if trade.entry_id == order_id:
                        trade.entry_price = order_status.avg_price
                        trade.quantity = order_status.executed_qty
                        threading.Thread(target=self.actualize_stop_loss, args=(trade,)).start() #ATUALIZA O STOPLOSS A PARTIR DO AVR
                        threading.Thread(target=self.enviarDadosTelegram, args=("NEW",)).start() #ENVIA DADOS PARA O TELEGRAM
                        break
                return

        t = Timer(2.0, lambda: self._check_order_status(order_id))
        t.start()


    def check_stop_loss_status(self):
        while True:
            try:
                if len(self.last_sl)>0:
                    status_stop = self.last_sl[-1]
                    status_stop_check = self.client.get_order_status(self.contract, status_stop.order_id)
                    if status_stop_check is not None:
                        if status_stop_check.status == "filled" or status_stop_check.status == "canceled":
                            for trade in self.trades:
                                if trade.status == "open":
                                    self.client._add_log(f"STOP LOSS ORDER ON {self.contract.symbol} {self.tf} FILLED/CANCELED")
                                    trade.status = "closed"
                                    self.ongoing_position = False
                                    threading.Thread(target=self.enviarDadosTelegram, args=("CLOSE",)).start()

            except Exception as e:
                self.client._add_log(f"ERRO: {traceback.format_exc()}",tipo="ERRO")
            time.sleep(20)


    def actualize_stop_loss(self,trade):
        if self.other_params['atr_period']==0 or self.other_params['atr_multiplier']==0 or not self.client.executar:
            return
        while True:
            if trade.status!="open": #PROBLEMA: ADICIONEI CONDIÇÂO DE PARADA PARA A THREAD
                return
            atr_period = self.other_params['atr_period']
            atr_multiplier = self.other_params['atr_multiplier']

            close_list = []
            high_list = []
            low_list = []

            for candle in self.candles:
                close_list.append(candle.close)  # Use only the close price of each candlestick for the calculations

            for candle in self.candles:
                high_list.append(candle.high)

            for candle in self.candles:
                low_list.append(candle.low)

            closes = pd.Series(close_list)  # Converts the close prices list to a pandas Series.

            high = pd.Series(high_list)

            low = pd.Series(low_list)

            # Calcular o True Range
            high_low = high - low
            high_close = abs(high - closes.shift())
            low_close = abs(low - closes.shift())

            # Função para encontrar o máximo entre três valores
            def max_entre_tres(valores):
                return max(valores)

            # Calcular o True Range como o máximo entre high_low, high_close e low_close para cada linha
            true_range = pd.DataFrame({'high_low': high_low, 'high_close': high_close, 'low_close': low_close})
            true_range['true_range'] = true_range.apply(max_entre_tres, axis=1)

            atr_series = true_range['true_range'].rolling(window=atr_period).mean()
            if trade.side.upper() == "BUY":
                if self.candles[-1].close > self.candles[-2].close:
                    self.stop_mov.append(self.candles[-1].close - (atr_multiplier * atr_series.iloc[-1]))
            else:
                if self.candles[-1].close < self.candles[-2].close:
                    self.stop_mov.append(self.candles[-1].close + (atr_multiplier * atr_series.iloc[-1]))
            if trade.status == "open":
                if (trade.side.upper() == "BUY" and self.stop_mov[-1] > self.stop_price[-1]) or (trade.side.upper() != "BUY" and self.stop_mov[-1] < self.stop_price[-1]):
                    cancelOrdem = self.client.cancel_order(self.contract, self.last_sl[-1].order_id)
                    if cancelOrdem!=None:
                        self.stop_price.append(self.stop_mov[-1])
                        order_sm = self.client.place_orderV2(self.contract, "STOP_MARKET",trade.quantity,trade.side.upper(), self.stop_price[-1], tipo='ATUALIZACAO STOPLOSS')
                        if order_sm!=None:
                            self.last_sl.append(order_sm)
                            threading.Thread(target=self.enviarDadosTelegram, args=("UPDATE",)).start()
            time.sleep(30)


    def enviarDadosTelegram(self,tipo):
        if not self.client.telegramOK:
            return 
        if tipo=='NEW':
            trade = self.trades[-1]
            SL = self.last_sl[-1]
            msg = f"NOVA POSIÇÃO {self.contract.symbol}\nTIPO: {trade.side}\nPREÇO: {trade.entry_price}\nQUANTIDADE: {trade.quantity}\nSTOPLOSS: {SL.stopPrice}"
            resp=self.client.telegram.sendFile(self.grafico(20).to_image('jpg'),msg,f"{trade.side}_{self.contract.symbol}.jpg")
            self.OrdemTelegram = {'trade':trade,'id':resp.message_id}
        elif tipo=='CLOSE':
            try:
                lucro = self.OrdemTelegram['trade'].pnl
                self.client.telegram.sendMessage(f'Fechando posição anterior {self.contract.symbol}\nLUCRO: {lucro}',self.OrdemTelegram['id'])
            except:
                self.client.telegram.sendMessage(f'Fechando posição anterior {self.contract.symbol}',self.OrdemTelegram['id'])
        elif tipo=='UPDATE':
            SL = self.last_sl[-1]
            self.client.telegram.sendMessage(f"Atualização SL:\nSTOPLOSS: {SL.stopPrice}",self.OrdemTelegram['id'])
        else:
            self.client.telegram.sendMessage(msg,self.OrdemTelegram['id'])
        return
    
    def _open_position(self, signal_result: int):
        order_side = "BUY" if signal_result == 1 else "SELL"
        if self.ongoing_position and self.trades[-1].side.lower() == order_side : #Não processar por que ja existe uma ordem anterior com essa posicao gerada ao iniciar o programa com ordens abertas na binance.
            return
        self.client._add_log(f"SINAL {order_side} NO {self.contract.symbol} {self.tf}")

        if self.ongoing_position: #CANCELANDO POSICAO ANTERIOR, SE JA ESTIVER EM UMA
            for trade in [x for x in self.trades if x.status=='open' and x.entry_price!=None]:
                
                order_status = self.client.place_orderV2(self.contract, "MARKET", trade.quantity, order_side, tipo="FINALIZAR POSICAO")

                threading.Thread(target=self.enviarDadosTelegram, args=("CLOSE",)).start() #ENVIA DADOS PARA O TELEGRAM
                if order_status!=None:
                    self.client._add_log(f"EXIT ORDER ON  {self.contract.symbol} {self.tf} PLACED SUCCESSFULLY")
                    trade.status = "closed"
                    self.ongoing_position = False

            #CANCELANDO ORDEM SL ANTERIOR
            order_cancel = self.client.cancel_order(self.contract, self.last_sl[-1].order_id)
     

        trade_size = self.client.get_trade_size(self.contract, self.candles[-1].close, self.balance_pct)
        if trade_size is None: return

        #ABRINDO NOVA POSICAO
        order_status = self.client.place_orderV2(self.contract, "MARKET", trade_size, order_side, tipo='NOVA POSICAO')


        if order_status is not None:
            self.ongoing_position = True
            exec_price = self.candles[-1].close
            if order_side == "BUY":
                self.stop_price.append(exec_price * (1 - self.stop_loss / 100))
            else:
                self.stop_price.append(exec_price * (1 + self.stop_loss / 100))
            orderContraria = 'SELL' if order_side=='BUY' else 'BUY'

            #CRIANDO SL DA NOVA POSICAO
            order_sl = self.client.place_orderV2(self.contract, "STOP_MARKET", trade_size, orderContraria,self.stop_price[-1], tipo='STOPLOSS NOVA POSICAO')        
            if order_sl!=None:
                self.last_sl.append(order_sl)

            avg_fill_price = None
            if order_status.status == "filled":
                avg_fill_price = order_status.avg_price
            else:
                t = Timer(2.0, lambda: self._check_order_status(order_status.order_id))
                t.start()

            new_trade = Trade({"time": self.client.timestamp(), "entry_price": avg_fill_price,
                                "contract": self.contract, "strategy": self.strat_name, "side": order_side,
                                "status": "open", "pnl": 0, "quantity": order_status.executed_qty,
                                "entry_id": order_status.order_id})

            self.trades.append(new_trade)
            if avg_fill_price!=None:
                threading.Thread(target=self.actualize_stop_loss, args=(new_trade,)).start()   #ATUALIZA O STOPLOSS A PARTIR DO AVR
                threading.Thread(target=self.enviarDadosTelegram, args=("NEW",)).start() #ENVIA DADOS PARA O TELEGRAM
            self.stop_mov.append(self.stop_price[-1]) #PROBLEMA: UMA HORA ESSA VARIAVEL VAI FICAR GIGANTE


    def _check_tp_sl(self, trade: Trade):

        """
        Based on the average entry price, calculates whether the defined stop loss or take profit has been reached.
        :param trade:
        :return:
        """

        tp_triggered = False

        price = self.candles[-1].close

        if trade.side.upper() == "BUY":

            if self.take_profit is not None:
                if price >= trade.entry_price * (1 + self.take_profit / 100):
                    tp_triggered = True

        elif trade.side.upper() == "SELL":

            if self.take_profit is not None:
                if price <= trade.entry_price * (1 - self.take_profit / 100):
                    tp_triggered = True

        if tp_triggered:
            self.client._add_log(f"{'Take profit'} for {self.contract.symbol} {self.tf} "
                          f"| Current Price = {price} (Entry price was {trade.entry_price})")

            order_status = self.client.place_orderV2(self.contract, "MARKET", trade.quantity, trade.side.upper(), tipo="SL/TP")            
            if order_status is not None:
                cancelOrdem = self.client.cancel_order(self.contract, self.last_sl[-1].order_id)
                self.client._add_log(f"Exit order on {self.contract.symbol} {self.tf} placed successfully")
                trade.status = "closed"
                self.ongoing_position = False
                threading.Thread(target=self.enviarDadosTelegram, args=("CLOSE",)).start() #ENVIA DADOS PARA O TELEGRAM



class MovingaverageStrategy(Strategy):
    def __init__(self, client, contract: Contract, exchange: str, timeframe: str, balance_pct: float,
                 take_profit: float,
                 stop_loss: float, other_params: Dict):
        super().__init__(client, contract, exchange, timeframe, balance_pct, take_profit, stop_loss, "Movingaverage",
                         other_params)

        self._ema_fast = other_params['ema_fast']
        self._ema_slow = other_params['ema_slow']
        self._atr_period = other_params['atr_period']
        self._atr_multiplier = other_params['atr_multiplier']
        self.lastData = pd.DataFrame()

    def _moving_average(self) -> Tuple[float, float, float, float]:

        closes = pd.Series([x.close for x in self.candles])  # Converts the close prices list to a pandas Series.
        datas = pd.Series([(pd.to_datetime(x.timestamp,unit='ms')-datetime.timedelta(hours=3)).strftime("%d/%m/%y %H:%M") for x in self.candles])  # Converts the close prices list to a pandas Series.
        ema_faster = closes.rolling(window=int(self._ema_fast)).mean()
        ema_slower = closes.rolling(window=int(self._ema_slow)).mean()

        DIFF = ema_faster - ema_slower
        CRUZAMENTOS = pd.Series((DIFF.shift(1) * DIFF < 0) & DIFF.notna())
        STATUS=pd.Series([('COMPRA' if (ema_faster[i]-ema_slower[i])>0 else 'VENDA')  if x==True else '' for i,x in enumerate(CRUZAMENTOS)])

        self.lastData = pd.DataFrame([ema_faster,ema_slower,datas,CRUZAMENTOS,STATUS],index=['FAST','SLOW','DATAS','CRUZAMENTOS','STATUS']).T.set_index("DATAS")
        return ema_faster.iloc[-1], ema_faster.iloc[-2], ema_slower.iloc[-1], ema_slower.iloc[-2] #TESTE


    def _check_signal(self):

        """
        Compute technical indicators and compare their value to some predefined levels to know whether to go Long,
        Short, or do nothing.
        :return: 1 for a Long signal, -1 for a Short signal, 0 for no signal
        """

        ema_fast1, ema_fast2, ema_slow1, ema_slow2 = self._moving_average()

        if ema_fast2 < ema_slow2 and ema_fast1 > ema_slow1:

            return 1  # Long signal

        elif ema_fast2 > ema_slow2 and ema_fast1 < ema_slow1:

            return -1  # Short signal
        else:
            return 0  # No signal

    def check_trade(self, tick_type: str):

        """
        To be triggered from the websocket _on_message() methods. Triggered only once per candlestick to avoid
        constantly calculating the indicators. A trade can occur only if the is no open position at the moment.
        :param tick_type: same_candle or new_candle
        :return:
        """

        if tick_type == "new_candle":
            signal_result = self._check_signal()
            if signal_result in [1, -1] and self.executar:
                self._open_position(signal_result)

    def grafico(self,ultimosDados = 10,candles=True):
        import plotly.express as px
        import plotly.graph_objects as go
        dadosFiltrados = self.lastData.iloc[-ultimosDados:]
        figOrig = px.line(dadosFiltrados[['FAST',"SLOW"]])

        COMPRA = dadosFiltrados[dadosFiltrados['STATUS']=='COMPRA']
        figOrig.add_trace(go.Scatter(
            x=COMPRA.index,
            y=COMPRA['FAST'],
            mode='markers',
            marker=dict(size=25, color='blue', symbol='arrow-up'),
            name='COMPRAS'
        ))
        VENDA = dadosFiltrados[dadosFiltrados['STATUS']=='VENDA']
        figOrig.add_trace(go.Scatter(
            x=VENDA.index,
            y=VENDA['FAST'],
            mode='markers',
            marker=dict(size=25, color='blue', symbol='arrow-down'),
            name='VENDAS'
        ))

        if candles:
            dadosCandles = self.candles[-ultimosDados:]
            fig = go.Figure(data=[go.Candlestick(x=[(pd.to_datetime(x.timestamp,unit='ms')-datetime.timedelta(hours=3)).strftime("%d/%m/%y %H:%M") for x in dadosCandles],
                        open=[x.open for x in dadosCandles],
                        high=[x.high for x in dadosCandles],
                        low=[x.low for x in dadosCandles],
                        close=[x.close for x in dadosCandles],name='Candles')])
            return figOrig.add_traces(fig.data).update_layout(plot_bgcolor='white',xaxis_title="Data",yaxis_title="Preço",font_size=15,height=700,xaxis_rangeslider_visible=False).update_layout(hovermode="x").update_layout(title=self.contract.symbol,yaxis=dict(gridcolor='gray'))
        return figOrig.update_layout(plot_bgcolor='white',xaxis_title="Data",yaxis_title="Preço",font_size=15,height=700,xaxis_rangeslider_visible=False).update_layout(hovermode="x").update_layout(title=self.contract.symbol,yaxis=dict(gridcolor='gray'))