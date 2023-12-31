# flake8: noqa W293
from loguru import logger
import estoques, produtos, vendas, forca_de_vendas, clientes, envio_email
from conn import DatabaseConnection
import time
import ctypes
import threading

MB_OK = 0x0

def show_message_box():
    ctypes.windll.user32.MessageBoxW(0, "Envio de arquivos BRF realizado!", "Automação BRF", MB_OK)

class ReportSender:
    def __init__(self):
        self.connection = DatabaseConnection()
        self.estoque = estoques.Estoques()
        self.produtos = produtos.Produtos()
        self.vendas = vendas.Vendas()
        self.forca_de_vendas = forca_de_vendas.Forca_De_Venda()
        self.clientes = clientes.Clientes()
        self.envio_email = envio_email.Envio_Email()

    def send_reports(self):
        self._connection()
        self._send_estoque()
        self._send_produtos()
        self._send_vendas()
        self._send_forca_de_vendas()
        self._send_clientes()
        self._send_emails()
        
    def _connection(self):
        self.connection.initialize_logging()
        self.connection.log_data()
        self.connection.get_db_engine()

    def _send_estoque(self):
        self.estoque.estoques()
        logger.info('Estoques enviado!')

    def _send_produtos(self):
        self.produtos.produtos()
        logger.info('Produtos enviado!')

    def _send_vendas(self):
        self.vendas.vendas()
        logger.info('Vendas enviado!')

    def _send_forca_de_vendas(self):
        self.forca_de_vendas.forca_de_vendas()
        logger.info('Forca_de_vendas enviado!')

    def _send_clientes(self):
        self.clientes.clientes()
        logger.info('Clientes enviado!')

    def _send_emails(self):
        self.envio_email.envio_email()
        logger.info('E-mails enviado!')

    t = threading.Thread(target=show_message_box)
    t.start

    time.sleep(7200)

    hwnd = ctypes.windll.user32.FindWindowW(None, "Automação BRF")
    if hwnd != 0:
        ctypes.windll.user32.SendMessageW(hwnd, 0x0010, 0, 0)

if __name__ == "__main__":
    report_sender = ReportSender()
    report_sender.send_reports()