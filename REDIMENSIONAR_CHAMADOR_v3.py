#*****************************************************************************************************************************#
# VERSÃO 1.0 -----------------20.ago.2024
# VERSÃO 2.0 -----------------25.ago.2024
# VERSÃO 3.0 -----------------25.dez.2024
#
# REDIMENSIONADOR_CHAMADOR
#
# v2.0: implementação de conceito de transferencia de valores das variáveis entre as threads, substituindo o conceito antigo de...
# ... variáveis globais. Variáveis globais não são estáveis com threads tendo outras threads aninhadas.
# v3.0: implementacao do ffmpeg para redimensionamentos com e sem filtro de movimentos.
#****************************************************************************************************************************#

import time
import cv2
import os
#import torch
import sys
import numpy as np
import threading
from queue import Queue, SimpleQueue
import json
import pymysql.cursors
from importlib.machinery import SourceFileLoader

versao = '3.0'

# -------------------------------------------------------------------------------------------

versao_docker = '3.0' #resizer_geral 8

# -------------------------------------------------------------------------------------------

###classe---------------------------------------------------------------------------------------------------------classe###
###                                        CLASSE CHAMADORA DO OBJETO DE REDIMENSIONAMENTO
###classe---------------------------------------------------------------------------------------------------------classe###

class Red():
    def __init__(self, caminho_origem=None, caminho_destino=None, caminho_mascara=None, caminho_lib=None,
                 nome_thread=None, vet_bco=None):

        self.foo = SourceFileLoader("mod_ext", caminho_lib).load_module()
        self.caminho_origem = caminho_origem
        self.caminho_destino = caminho_destino
        self.caminho_mascara = caminho_mascara

        self.nome_thread = nome_thread

        # rotina que cria conexão com bco mysql e retorna objeto conexão
        self.conexao_bco = conectar_bco(host=vet_bco[0], user=vet_bco[1], password=vet_bco[2], database=vet_bco[3])
        #self.conexao_bco = None

        self.status_processamento = False

    def start(self):
        self.t = threading.Thread(target=self.exec_red, args=())
        self.t.daemon = True
        self.t.start()
        return self

    def exec_red(self):
        ret_foo = self.foo.RotinaPrincipal(conexao_bco=self.conexao_bco).start(arg=self.caminho_origem,
                                                        arg2=self.caminho_destino, arg3=self.caminho_mascara,
                                                        arg4=self.nome_thread)

        while True:
            time.sleep(0.1)
            if not ret_foo.t.is_alive():
                break
###----------------------------------------------------------------------------------------------------

# rotina conecta o bco mysql e retona o objeto de conexão
def conectar_bco(host=None, user=None, password=None, database=None):
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=password,
                                 database=database,
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

def selecionar_lojas_grupo (conexao_bco=None, grupo_lojas=None):

    # variaveis de retorno
    vet_lojas_grupo = []

    cursor = conexao_bco.cursor() # prepara bco para operações na tabela tb_esclacao_cliente

    sql = "SELECT cod_loja "
    sql = sql + " FROM tb_grupo_empresa_loja_n "
    sql = sql + " WHERE "
    sql = sql + " grupo_lojas ='" + grupo_lojas + "'"

    cursor.execute(sql)
    ret_query = cursor.fetchall()
    if ret_query is not None:
        for ii in ret_query:
            vet_lojas_grupo.append(ii['cod_loja'])


    else: #cursos retornou nenhum registro
        print('ATENCAO: IP_equipamento e/ou id_cam nao cadastrados na tabela tb_grupo_empresa_loja_n')
        cursor.close()
        exit() # sair do programa

    cursor.close()

    return vet_lojas_grupo


###########################################################################################################################
#-------------------------------------------------------------------------------------------------------------------------#
#
#                                        *** INÍCIO DO CÓDIGO DE EXECUÇÃO ***
#                                                ROTINA PRINCIPAL
#-------------------------------------------------------------------------------------------------------------------------#

# conectar com bco de dados
host='127.0.0.1' # WANDER SUBSTITUIR PARA O ENDEREÇO GCP
user='root'
password=''
database='flint_go'
# agrupar dados bco em um vetor com uso nas threads
vet_bco = [host, user, password, database]

# rotina que cria conexão com bco mysql e retorna objeto conexão
conexao_bco = conectar_bco(host=host, user=user, password=password, database=database)
#conexao_bco = None

# CAMINHOS -----------------------inicio
# caminho de origem dos videos .dav para processamento de redimensionamento, padronização de fps e conversão para .avi
caminho_origem = 'C:\\Users\\david\\OneDrive\\Documentos\\AUDITORIA\\PARME\\video' #WANDER
# caminho destino dos arquivos .avi
caminho_destino = 'C:\\Users\\david\\OneDrive\\Documentos\\AUDITORIA\\PARME\\saida_video'  # '/mnt/disks/output/' #'C:\\mnt\\disks\\output' #WANDER
# caminho de gravação da máscara. Obs: o nome do arquivo da máscara está associado a câmera (de cada loja)
caminho_mascara = '/' #WANDER
# caminho da lib .py
caminho_lib = 'C:\\Users\\david\\Downloads\\src\\src\\resizer_geral_v8_2.py' #WANDER
# CAMINHOS -----------------------fim

# identificar o cluster passado pelo script GCP
var_cluster = 'GRUPO5' #str(os.environ['CLUSTER_LOJAS']) #WANDER

# seleciona as lojas que fazem parte do grupo definido no workflow do GCP (ex: 'GRUPO1')
vet_lojas_grupo = selecionar_lojas_grupo (conexao_bco=conexao_bco, grupo_lojas=var_cluster)
#vet_lojas_grupo = []

#---INICIO
# condição inserida ÚNICA e EXCLUSIVAMENTE para testes com a pizzaria PARMÊ.
#vet_lojas_grupo.append('ch8')
#---FIM

# fecha a conexão com o banco
conexao_bco.close()

# LOOP PRINCIPAL DE SEPARAÇÃO DOS ARQUIVOS POR LOJA E INICIALIZAÇÃO DAS THREADS
contador_rodada = 0
while True:
    contador_rodada += 1
    var_dados_loja=[]
    vet_lote_lojas=[]
    qtde_arquivos=0
    for dados_loja in vet_lojas_grupo:
        vet_dados_loja=[]
        for rootdir, dirs, files in os.walk(caminho_origem):
            qtde_arquivos = len(files)
            for ii in files:
                dados_split = ii.split('_')  
                composicao_dados_loja2 = dados_split[0]
                if dados_loja == composicao_dados_loja2:
                    caminho_loja = os.path.join(rootdir, ii)
                    vet_dados_loja.append(caminho_loja)
        if len(vet_dados_loja)>0:
            vet_lote_lojas.append(vet_dados_loja)

    vet_objetos=[]
    cta_thread=0
    for loja_ind in vet_lote_lojas:
        
        cta_thread += 1
        nome_t = 'THREAD_' + str(cta_thread)

        obj_ = Red(caminho_origem=loja_ind, caminho_destino=caminho_destino,caminho_mascara=caminho_mascara,
                                caminho_lib=caminho_lib, nome_thread=nome_t, vet_bco=vet_bco).start()
        vet_objetos.append(obj_)

    tempo_inicial = time.time()
    while True:
        time.sleep(1)
        var_status_processamento = False
        for ii in vet_objetos:
            if ii.t.is_alive():
                var_status_processamento = True
                break
        if not var_status_processamento:
            break

    if qtde_arquivos==0: # não existem mais arquivos para serem processados
        break

    tempo_final = time.time() - tempo_inicial
    print('TEMPO TOTAL lote', str(contador_rodada), ':', tempo_final, 'segundos!!!')

print('RODADAS DE PROCESSAMENTO FINALIZADAS___________________________________')


        
     
