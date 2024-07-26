import telebot
from io import BytesIO
from unidecode import unidecode
import traceback
import os

class BotTelegram:
	def __init__(self,ID="",CHAVE=""):
		self.CHAVE = '7353122123:AAFMZYugSdKE6ZcBYcJEN8JMVcJxgyuujKM' if CHAVE=='' else CHAVE
		self.bot = telebot.TeleBot(self.CHAVE)
		self.GRUPO_ID = ID

	def GetMessageUser(self,binance,i=0):
		try:
			comandos = ['/Grafico - Pode adicionar "-x" para pegar outros periodos x. EX: Grafico-50','/Posicoes - Ver posições abertas','/Acumulado - Mostra o acumulado da moeda','/Estrategias', '/Desativar', '/Ativar', '/Log', '/Comandos']
			binance._add_log("TELEGRAM PRONTO")
			if i==0:
				self.sendMessage('BOT BINANCE INICIADO\nLISTA DE COMANDOS DISPONIVEIS:\n'+"\n".join(comandos)+"\nEnvie um arquivo 'DadosEntrada.xlsx' para atualizar as estratégias")
			
			@self.bot.message_handler(func=lambda message: int(message.from_user.id)==int(self.GRUPO_ID))
			def getMessage(message):
				mensagem = unidecode(message.text.lower()).split(" ")[0]
				if "grafico" in mensagem:
					periodo = int(mensagem.rsplit("-",1)[-1]) if '-' in mensagem else 20
					for strategia in binance.strategies.values():
						self.sendFile(strategia.grafico(periodo).to_image('jpg'),strategia.contract.symbol,f"{strategia.contract.symbol}-{periodo}.jpg",message.message_id)
				
				elif "posicoes" in mensagem:
					controle=False
					for strategia in binance.strategies.values():
						trade = [x for x in strategia.trades if x.status=='open']
						if len(trade)>0:
							if not controle:
								self.sendMessage("Posições em aberto:",message.message_id)
							controle=True
							SL = strategia.last_sl[-1]
							msgID = strategia.OrdemTelegram['id'] if strategia.OrdemTelegram['id']!="" else message.message_id
							msg = f"MOEDA:{strategia.contract.symbol}\nTIPO: {trade[-1].side}\nPREÇO ENTRADA: {trade[-1].entry_price}\nQUANTIDADE: {trade[-1].quantity}\nPNL: {round(trade[-1].pnl,5)}\nSTOPLOSS: {SL.stopPrice}"
							self.sendMessage(msg,msgID)
					if not controle:
							self.sendMessage("Nenhuma posição em aberto",message.message_id)

				elif 'acumulado' in mensagem:
					for strategia in  binance.strategies.values():
						acumulado = 0
						for trade in strategia.trades:
							acumulado+=trade.pnl
						self.sendMessage(f"Acumulado {strategia.contract.symbol}:\n{round(acumulado,5)}")
				
				elif 'estrategias' in mensagem:
					try:
						self.sendFile(open("DadosEntrada.xlsx",'rb').read(),"Arquivo de estratégias","DadosEntrada.xlsx",message.message_id)
					except:
						self.sendFile(open("BotBinance/DadosEntrada.xlsx",'rb').read(),"Arquivo de estratégias","DadosEntrada.xlsx",message.message_id)
	
				elif 'desativar' in mensagem:
					binance.executar = False
					self.sendMessage("ROBO DESATIVADO COM SUCESSO",message.message_id)

				elif 'ativar' in mensagem:
					binance.executar = False
					self.sendMessage("ROBO ATIVADO COM SUCESSO",message.message_id)
				elif 'log' in mensagem:
					self.sendFile(open("info.log",'rb').read(),"Arquivo de logs","log.txt",message.message_id)
				elif 'comandos' in mensagem:
					self.sendMessage('LISTA DE COMANDOS DISPONIVEIS:\n'+"\n".join(comandos),message.message_id)
				else:
					self.sendMessage('COMANDO NÃO FOI ENTENDIDO.\nLISTA DE COMANDOS DISPONIVEIS:\n'+"\n".join(comandos),message.message_id)
			
			@self.bot.message_handler(content_types=['document'])
			def handle_document(message):
				file_info = self.bot.get_file(message.document.file_id)
				file_path = self.bot.get_file(file_info.file_id).file_path
				filename = message.document.file_name
				if filename=='DadosEntrada.xlsx':
					downloaded_file = self.bot.download_file(file_path)
					with open(os.path.join(filename), 'wb') as new_file:
						new_file.write(downloaded_file)
					binance._add_log("Arquivo de estratégias atualizado.",message.message_id)
					self.sendMessage("Arquivo de estratégias atualizado.",message.message_id)
					binance.strategies = {}
					binance.rodar(ignoreTelegram=True)
			self.bot.polling()
		except: 
			binance._add_log(f"ERRO NO TELEGRAM: {traceback.format_exc()}")
			return self.GetMessageUser(binance,2)


	def sendFile(self,dados,msgArquivo="",nomeArquivo="",reply=""):
		'''
		Envia arquivos locais ou em memoria para o chat do telegram
		dados: bytes or string - Caminho do arquivo ou Bytes do arquivo que se deseja enviar.
		msgArquivo: string - Texto que ficará vinculado ao arquivo enviado. Não é obrigatório.
		nomeArquivo: string - Nome do arquivo que será enviado. É obrigatório caso passe os bytes do arquivo, caso mande arquivo local, é enviado o nome do arquivo original. É necessário colocar a extensão do arquivo.
		'''
		if type(dados)!=bytes:
			if nomeArquivo=="":
				nomeArquivo = dados.rsplit("/",1)[-1]
			dados = open(dados,'rb').read()
		if reply=="":
			return self.bot.send_document(self.GRUPO_ID,dados,caption=msgArquivo,visible_file_name=nomeArquivo)
		else:
			return self.bot.send_document(self.GRUPO_ID,dados,caption=msgArquivo,visible_file_name=nomeArquivo,reply_to_message_id=reply)
		
	def getID(self):
		'''Pega o ID do grupo/chat para enviar as mensagens'''
		print("ABRA UMA CONVERSA COM O BOT NO LINK: http://t.me/tradeBaumgaertnerBot")
		print("DIGITE '/id' NA CONVERSA COM O BOT NO TELEGRAM QUE SE DESEJA OBTER O ID")
		print("DIGITE '/exit' NA CONVERSA COM O BOT NO TELEGRAM PARA FINALIZAR")

		@self.bot.message_handler(commands=['id'])
		def getID2(message):
			print('DADOS DA MENSAGEM: ')
			print(message)
			print("ID DO USUARIO:")
			print(message.from_user.id)
		
		@self.bot.message_handler(commands=['exit'])
		def EXIT(message):
			self.bot.stop_polling()
			self.bot.stop_bot()
		self.bot.polling()      

	def sendMessage(self,msg,reply=""):
		'''Mandar msg para o grupo do telegram'''
		if reply!="":
			return self.bot.send_message(chat_id=self.GRUPO_ID,text=msg,reply_to_message_id=reply)
		else:
			return self.bot.send_message(chat_id=self.GRUPO_ID,text=msg)

	
	def sendDF(self,df,nome,LarguraColunas = 2.8,AlturaLinhas = 0.8):
		'''
		***Necessário importar o matplotlib==3.5.1
		Envia diretamente um df do pandas para o chat do telegram no formato jpg
		df: pd.DataFrame - Dataframe do pandas que se quer enviar
		nome: string - Nome do arquivo que será enviado
		LarguraColunas: flaot - Largura da coluna do dataframe na imagem
		AlturaLinhas: flaot - Altura da linha do dataframe na imagem
		'''
		return self.sendFile(dados=dfToImage(df,LarguraColunas=LarguraColunas,AlturaLinhas=AlturaLinhas),msgArquivo=nome,nomeArquivo=f'{nome.replace("/","").replace(" ","")}.jpg')

def dfToImage(df,LarguraColunas = 2.8,AlturaLinhas = 0.8):
    import matplotlib.pyplot as plt
    from matplotlib.table import Table
    _ , ax = plt.subplots(figsize=(len(df.columns)*LarguraColunas, len(df.index)*AlturaLinhas))
    
    ax.axis('tight')
    ax.axis('off')
    tabela = Table(ax, bbox=[0, 0, 1, 1])

    for level in range(df.columns.nlevels):
        for i, col in enumerate(df.columns.get_level_values(level)):
            cell = tabela.add_cell(level, i, width=0.2, height=0.1, text=col, loc='center', facecolor='#002f4a')
            cell.set_text_props(color='white', weight='bold', fontsize=12)

    for i, linha in enumerate(df.values):
        for j, valor in enumerate(linha):
            cell = tabela.add_cell(i + df.columns.nlevels, j, width=0.2, height=0.1, text=valor, loc='center', facecolor='white')
            cell.set_fontsize(12)
            cell.set_text_props(color='black', weight='bold', fontsize=8)

    for (i, j), cell in tabela.get_celld().items():
        cell.set_edgecolor('black')

    tabela.auto_set_font_size(False)
    tabela.set_fontsize(12)
    tabela.scale(1.2, 1.2)

    ax.add_table(tabela)
    Bytes = BytesIO()
    plt.savefig(Bytes, bbox_inches='tight', pad_inches=0)
    return Bytes.getvalue()
