'''
Histórico das atualizaçoes e versionamento:
'''
#*******************************************************************************************************************#
# VERSÃO 2.1 -----------------08.mar.2024
# VERSÃO 3.0 -----------------15.mai.2024
# VERSÃO 4.0 -----------------26.jun.2024
# VERSÃO 5.0 -----------------19.jul.2024
# VERSÃO 5.2 -----------------21.ago.2024
# VERSÃO 5.3 -----------------08.nov.2024
# VERSÃO 6.0 -----------------15.nov.2024
# VERSÃO 7.0 -----------------12.dez.2024
# VERSÃO 8.0 -----------------25.dez.2024
#
# ROTINA COM O OBJETIVO DE CONVERTER VIDEO .DAV (H.265) PARA VIDEO .AVI (H.264) e REDIMENSIONAR LARGURA E ALTURA
#
#
# v2.1: faz a conversão, salva em tmp e posteriormente copia para diretório definitivo
# v3.0: resizer transformado em genérico com acesso a .json de configuração das lojas e coordenadas de máscara
# v4.0: recodificação com o objetivo de criar loop de 'resize' de videos conforme qtde configurada e tempo de scheduler
# v4.2: padronização da taxa de FPS = 10...com o objetivo de reduzir o tamanho do video aumentando a velocidade de proces
# v4.2.6: inclusão de rotina de exclusão do arquivo .dav após o resizer do mesmo
# v5.0: inclusao de lógica para filtrar arquivos .dav originários de diretórios com IPs específicos
# v5.1.2: arquivos .avi redimensionados formatados com o IP da loja no nome do arquivo + ajuste de erro
# v5.1.3: arquivos .avi redimensionados formatados com o IP da loja + cod loja no nome do arquivo
# v5.2: alteração da máscara preta para máscara branca, evitando detecções 'falso-positivo' em situações de confiança baixa
# v5.3: ajuste da config para H.264
# v6.0: carga de variáveis com acesso a bco de dados e não mais aos arquivos .json
# v6.1: 01/07/24 eliminação do arquivo de variáveis de ambiente (.env) que fica junto com este .py
# v6.2: 29/07/24 eliminação da necessidade de leitura de equipamentos com o IP e padronização do nome do arquivo original como:
#   exemplo: CGR_DVR1_ch4_[1,6]_20240410131000_20240410132001.avi
#          loja_equip_cam_subprocessos_datahorainicial_datahorafinal.avi
#   A configuração do nome do arquivo deve ser realizada no equipamento DVR com base em cadastro prévio em bco dados.
# v6.3: 24/10/24 ajustes no código permitindo ser chamado pelo 'REDIMENSIONAR_CHAMADOR' como uma classe.
# v7.0: 12/12/24 implementação de rotina de detecção de movimento, possibilitando a gravação apenas de frames com movimento.
# v8.0: 25/12/24 implementacao do ffmpeg para redimensionamentos com e sem filtro de movimentos.
# v8.1: 05/02/25 implementacao de lógica que suporta até 3 frames vazios no meio do video .dav, sem concluir o processo.
#*******************************************************************************************************************#

import os
import sys
import cv2
import numpy as np
import threading
import time
import subprocess
import shutil
import tempfile
import json
from queue import Queue
import ffmpeg

import pymysql.cursors
from contextlib import contextmanager


versao = '8.1'

@contextmanager
def tempfilename(extension):
    dir = tempfile.mkdtemp()
    yield os.path.join(dir, 'tempoutput' + extension)
    shutil.rmtree(dir)

def get_video_info(fileloc=None) :
    command = ['ffprobe',
               '-v', 'fatal',
               '-show_entries', 'stream=width,height,avg_frame_rate',
               '-of', 'default=noprint_wrappers=1:nokey=1',
               fileloc, '-sexagesimal']
    ffmpeg = subprocess.Popen(command, stderr=subprocess.PIPE ,stdout = subprocess.PIPE )
    out, err = ffmpeg.communicate()
    if(err):
        print(err)
    out = out.decode()
    print('Resultado do decode:', out)
    out = out.split('\n')[2].split('/')[0]
    return out

def montar_mascara(roi=None, tm_mask=None):
    # cria a mascara em modelo multiplas linhas
    tm_mask_v = tm_mask[1]
    tm_mask_h = tm_mask[0]

    mask_branca = np.ones((tm_mask_v, tm_mask_h, 3), dtype=np.uint8)  # imagem com 'uns'
    mask_branca = 255 * mask_branca  # converte 'uns' em brancos

    roi_corners = np.array(roi, dtype=np.int32)
    channel_count = 3

    ignore_mask_color = (0,) * channel_count  # criar mascara com 'zeros' (fundo preto)
    mask = cv2.fillPoly(mask_branca, roi_corners, ignore_mask_color)

    # obtem as dimensoes
    h, w, c = mask.shape
    # adiciona o canal alfa, necessário para BGRA (Blue, Green, Red, Alpha)
    image_bgra = np.concatenate([mask, np.full((h, w, 1), 255, dtype=np.uint8)], axis=-1)

    # cria a máscara onde os pixels são pretos ([0, 0, 0])
    white = np.all(mask == [0, 0, 0], axis=-1)
    image_bgra[white, -1] = 0

    return image_bgra

# rotina conecta o bco mysql e retona o objeto de conexão
def conectar_bco(host=None, user=None, password=None, database=None):
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=password,
                                 database=database,
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

def selecionar_dados_camera(conexao_bco=None, loja=None, equipamento=None, camera=None):

    # variaveis de retorno
    dimensao_resizer=None
    fps_camera=None
    coordenadas_mascara=None
    flg_movimento = None
    id_subprocesso_vet = []

    cursor = conexao_bco.cursor() # prepara bco para operações na tabela tb_esclacao_cliente

    sql = "SELECT dimensao_resizer, fps_camera, coordenadas_mascara, id_loja_equip_cam, flg_movimento "
    sql = sql + " FROM tb_grupo_empresa_loja_n a, tb_grupo_empresa_loja_equipamento_n b, tb_grupo_empresa_loja_equipamento_cam_n c"
    sql = sql + " WHERE "
    sql = sql + " a.cod_loja ='" + loja + "' and a.cnpj_loja = b.cnpj_loja and b.cod_equipamento ='" + equipamento + "'"
    sql = sql + " and b.id_loja_equip = c.id_loja_equip and c.id_cam =" + str(camera)

    cursor.execute(sql)
    ret_query = cursor.fetchone()
    if ret_query is not None:

        dimensao_resizer = ret_query['dimensao_resizer']
        fps_camera = ret_query['fps_camera']
        coordenadas_mascara  = ret_query['coordenadas_mascara']
        id_loja_equip_cam = ret_query['id_loja_equip_cam']
        flg_movimento = ret_query['flg_movimento']

    else: #cursos retornou nenhum registro
        print('ATENCAO: IP_equipamento e/ou id_cam nao cadastrados na tabela tb_grupo_empresa_loja_equipamento_cam_n')
        cursor.close()
        exit() # sair do programa

    # selecionar dados dos processos associados ao IP e camera
    sql = "SELECT id_subprocesso FROM tb_grupo_empresa_loja_equipamento_cam_processo_n a"
    sql = sql + " WHERE "
    sql = sql + " a.id_loja_equip_cam=" + str(id_loja_equip_cam)

    cursor.execute(sql)
    ret_query = cursor.fetchall()
    if ret_query is not None:
        for ii in ret_query:
            id_subprocesso_vet.append(ii['id_subprocesso'])

    else: #cursos retornou nenhum registro
        print('ATENCAO: IP_equipamento e/ou id_cam nao cadastrados na tabela tb_grupo_empresa_loja_equipamento_cam_processo_n')
        cursor.close()
        exit() # sair do programa

    cursor.close()

    return dimensao_resizer, fps_camera, coordenadas_mascara, id_subprocesso_vet, flg_movimento


###classe---------------------------------------------------------------------------------------------------------classe###
###                                        CLASSE DE INFERÊNCIA
###classe---------------------------------------------------------------------------------------------------------classe###
class ler_video_redimensionar:
    def __init__(self, escala_conversao=None, mask=None, backSub=None):

        self.QQ_video = Queue(maxsize=500)

        self.escala_conversao = escala_conversao
        self.fps = None
        self.status_termino = False
        self.status_loop = False

        self.loja = ''
        self.hr_inicio = ''
        self.mask = mask
        self.backSub = backSub

    def start(self, arg=None):
        self.t = threading.Thread(target=lambda: self.exec_lvr(caminho=arg))
        self.t.daemon = True
        self.t.start()
        return self

    def exec_lvr(self, caminho=None):

        cap = cv2.VideoCapture(caminho)

        # armazenar a taxa de fps do video
        self.fps = cap.get(5)
        split_geral = caminho.split('\\') # WANDER
        arq_inteiro = split_geral[-1]
        arq_split = arq_inteiro.split('_')
        self.loja = arq_split[0]
        self.hr_inicio = arq_split[4]

        if not cap.isOpened():
            print("Erro na abertura do video!!!")

        num_frame=0
        contador_frame_vazio=0
        while True:

            # verificar se a fila de frames redimensionados foi totalmente consumida pelos processos seguintes.
            if self.status_termino:
                print('Thread que executa a leitura de videos finalizando...')
                break

            ret, frame = cap.read()
            num_frame += 1

            if not ret:
                # verifica se passou uma vez de tal forma que mantem os loops mas sem processar mais nada, esperando o
                # self.status_termino como True para sair em definitivo da thread.
                contador_frame_vazio += 1
                if contador_frame_vazio < 6: #qtde de vezes (5) que o loop deve ter frame vazio para se certificar do fim do video e não apenas uma falha  no meio do video
                    continue

                if self.status_loop:
                    continue

                # fechar o objeto video
                cap.release()

                self.QQ_video.put([1, None, None])

                self.status_loop = True
                continue

            # inicializa ou reinicializa
            contador_frame_vazio = 0

            # redimensionar
            frame = cv2.resize(frame, (self.escala_conversao[0], self.escala_conversao[1]))

            frame = cv2.bitwise_and(frame, frame, mask=self.mask)
            frame_out = frame.copy()
            #frame_out = cv2.putText(frame_out, 'frame orig ' + str(num_frame), (550, 280), 0, 0.3, [255, 255, 255], thickness=1,lineType=cv2.LINE_AA)

            # backgroud_substraction
            frame = self.backSub.apply(frame)

            # armazena frame redimensionado na fila
            self.QQ_video.put([0, frame, frame_out])

class ler_video_redimensionar_COMPLETO:
    def __init__(self, escala_conversao=None, mask=None):

        self.QQ_video_completo = Queue(maxsize=500)

        self.escala_conversao = escala_conversao
        self.fps = None
        self.status_termino = False
        self.status_loop = False

        self.loja = ''
        self.hr_inicio = ''
        self.mask = mask

    def start(self, arg=None):
        self.t = threading.Thread(target=lambda: self.exec_lvr(caminho=arg))
        self.t.daemon = True
        self.t.start()
        return self

    def exec_lvr(self, caminho=None):

        cap = cv2.VideoCapture(caminho)

        # armazenar a taxa de fps do video
        self.fps = cap.get(5)
        split_geral = caminho.split('\\') # WANDER
        arq_inteiro = split_geral[-1]
        arq_split = arq_inteiro.split('_')
        self.loja = arq_split[0]
        self.hr_inicio = arq_split[4]

        if not cap.isOpened():
            print("Erro na abertura do video!!!")

        contador=0
        while True:

            # verificar se a fila de frames redimensionados foi totalmente consumida pelos processos seguintes.
            if self.status_termino:
                print('Thread que executa a leitura de videos finalizando...')
                break

            ret, frame = cap.read()

            if not ret:
                # verifica se passou uma vez de tal forma que mantem os loops mas sem processar mais nada, esperando o
                # self.status_termino como True para sair em definitivo da thread.
                if self.status_loop:
                    continue

                # fechar o objeto video
                cap.release()

                self.QQ_video_completo.put([1, None, contador])

                self.status_loop = True
                continue

            contador+=1

            # redimensionar
            frame = cv2.resize(frame, (self.escala_conversao[0], self.escala_conversao[1]))

            frame = cv2.bitwise_and(frame, frame, mask=self.mask)
            #cv2.putText(frame, str(contador), (600, 340), 0, 0.3, [255, 255, 255],thickness=1, lineType=cv2.LINE_AA)

            # armazena frame redimensionado na fila
            self.QQ_video_completo.put([0, frame, contador])


#####################################################################################################################
#                                           INICIO DA ROTINA PRINCIPAL
#
#
#####################################################################################################################

class RotinaPrincipal:
    def __init__(self, conexao_bco=None):

        self.conexao_bco = conexao_bco
        self.objeto_gravador=None

        # configuracao ffmpg-------------------------------------------------------------
        self.codec_video = '-c:v libx264'
        self.crf = '-crf 23'
        self.preset = '-preset ultrafast'
        self.codec_audio = '-c:a aac'
        self.bitrate_audio = '-b:a 320k'

        self.arquivo_saida=None

    def start(self, arg=None, arg2=None, arg3=None, arg4=None):
        self.t = threading.Thread(target=lambda: self.looprotinaprincipal(caminho_origem=arg, caminho_destino=arg2,
                                                                          caminho_mascara=arg3, nome_thread=arg4))
        self.t.daemon = True
        self.t.start()
        return self

    def looprotinaprincipal(self, caminho_origem=None, caminho_destino=None, caminho_mascara=None, nome_thread=None):

        contador_arquivos = 0
        #escala_conversao = (640, 360)
        for arquivo in caminho_origem: # encontrou arquivo i: 'NVR_ch1_main_20221220114000_20221220115000.dav'

            tmp_original = time.time()

            contador_arquivos += 1

            # seleciona apenas o nome do arquivo
            i = arquivo.split("\\")[-1] #/   \\   #WANDER

            if os.stat(arquivo).st_size == 0: # verificar se o arquivo tem tamanho 'zero' (inicio de cópia)
                continue

            # dados de configuração
            split_pto = i.split(".")
            nome_sem_extencao = split_pto[0]
            split_geral = nome_sem_extencao.split("_")

            # ---INICIO
            # condição 'if' inserida ÚNICA e EXCLUSIVAMENTE para testes com a pizzaria PARMÊ.
            if split_geral[0][:2] == 'ch':
                loja = 'PMENSH'
                equipamento = 'DVR2'
                camera = 8
                data_hora_inicio = split_geral[2]
                data_hora_fim = split_geral[3]
            # ---FIM
            else:
                loja = split_geral[0]
                equipamento = split_geral[1]
                camera = int(split_geral[2][2:])
                data_hora_inicio = split_geral[4]
                data_hora_fim = split_geral[5]

            print('[INFO] ', nome_thread + '_' + str(contador_arquivos), ' Arquivo entrada:', arquivo)

            # selecionar dados da camera
            dimensao_resizer, fps_camera, coordenadas_mascara, id_subprocesso_vet, flg_movimento = selecionar_dados_camera(
                                            conexao_bco=self.conexao_bco, loja=loja, equipamento=equipamento, camera=camera)
            #dimensao_resizer='[640, 360]'
            #fps_camera=10
            #coordenadas_mascara='{"coordenadas":[[[5,5],[635,5],[635,355],[5,355]]]}'
            #id_subprocesso_vet=[1]
            #flg_movimento=1


            # montagem do novo nome do arquivo com dados de referencia
            # ex: CGR_DVR1_ch4_[1,6]_20240410131000_20240410132001.avi
            nome_arquivo_loja = loja + "_" + equipamento + "_" + 'ch' + str(camera) + "_" + str(id_subprocesso_vet) + "_" + data_hora_inicio + "_" + data_hora_fim
            # adiciona a extensão .avi
            nome_novo_arquivo = nome_arquivo_loja + '.avi' #'.avi'
            self.arquivo_saida = os.path.join(caminho_destino, nome_novo_arquivo)

            if not os.path.exists(caminho_destino):
              print('[INFO] Diretório destino:', caminho_destino, 'NÃO EXISTE!')
              break

            print('[INFO] ', nome_thread + '_' + str(contador_arquivos), ' Arquivo saida:', self.arquivo_saida)

            # criar máscara de acordo com coordenadas armazenadas no banco
            #caminho_mascara_join = os.path.join(caminho_mascara, loja + '_mascara.png') #/SDXTTE_mascara.png

            roi_A_tmp = json.loads(coordenadas_mascara)
            roi_A = roi_A_tmp['coordenadas'] # coordenadas para a máscara
            tm_mask = json.loads(dimensao_resizer) # tamanho w x h para a máscara

            # caso a config seja para NÃO filtrar movimentos
            if flg_movimento==0:

                tempo_inicial = time.time()

                print('[INFO] ', nome_thread + '_' + str(contador_arquivos),' inicio do redimensionamento modo SEM FILTRO DE MOV com a versao', versao, '...')

                # define a máscara ----------------------
                mask_branca = np.zeros((tm_mask[1], tm_mask[0]), dtype=np.uint8)  # imagem com 'uns'

                roi_corners = np.array(roi_A, dtype=np.int32)
                channel_count = 3

                ignore_mask_color = (255,) * channel_count  # criar mascara com 'zeros' (fundo preto)
                mask = cv2.fillPoly(mask_branca, roi_corners, ignore_mask_color)
                # ----------------------------------------

                # executar a classe video-redim
                video_inferencia = ler_video_redimensionar_COMPLETO(escala_conversao=tm_mask, mask=mask).start(arg=arquivo)

                # trecho de código que aguarda a classe carregar totalmente
                while True:
                    time.sleep(0.1)
                    if not video_inferencia.t.is_alive() or video_inferencia.fps is None:
                        continue
                    else:
                        break

                loja = video_inferencia.loja
                hr_inicio = video_inferencia.hr_inicio
                fps = video_inferencia.fps

                num_frame = 0
                num_frame_movi = 0
                contador_lote = 0
                detected_motion = False
                vetor_frames = []

                p_w = int(tm_mask[0])
                p_h = int(tm_mask[1])
                p_fps = int(fps)
                self.process = (
                    ffmpeg
                    .input('pipe:', format='rawvideo', pix_fmt='rgb24' , s='{}x{}'.format(p_w, p_h))
                    .output(self.arquivo_saida, pix_fmt='yuv420p', vcodec='libx264', crf=21, loglevel="quiet")
                    .overwrite_output()
                    .run_async(pipe_stdin=True)
                )

                while True:

                    # fila aux_QQ armazenada na GPU assim como o registro obtido pelo comando 'get'
                    status_termino, fg_mask, contad = video_inferencia.QQ_video_completo.get()

                    if status_termino:
                        video_inferencia.status_termino = True
                        break

                    num_frame += 1
                    contador_lote += 1

                    frame_c=cv2.cvtColor(fg_mask, cv2.COLOR_BGR2RGB).astype(np.uint8).tobytes()
                    self.process.stdin.write(frame_c)
                    #print('Fila frames', video_inferencia.QQ_video_completo.qsize())

                    #cv2.imshow("Output Frame", fg_mask)
                    #cv2.waitKey(1)

                self.process.stdin.close()
                self.process.wait()
                self.process.kill()

                tempo_final = time.time() - tempo_inicial
                print('Tmp processamento', round(tempo_final, 2), 'segundos')

            else: #filtra apenas frames com movimento

                tempo_inicial = time.time()

                print('[INFO] ', nome_thread + '_' + str(contador_arquivos), ' inicio do redimensionamento modo FILTRO DE MOV com a versao', versao, '...')

                # background modo MOG...aparentemente melhor que o KNN
                backSub = cv2.createBackgroundSubtractorMOG2()

                # define a máscara ----------------------
                mask_branca = np.zeros((tm_mask[1], tm_mask[0]), dtype=np.uint8)  # imagem com 'uns'

                roi_corners = np.array(roi_A, dtype=np.int32)
                channel_count = 3

                ignore_mask_color = (255,) * channel_count  # criar mascara com 'zeros' (fundo preto)
                mask = cv2.fillPoly(mask_branca, roi_corners, ignore_mask_color)
                # ----------------------------------------

                # executar a classe video-redim
                video_inferencia = ler_video_redimensionar(escala_conversao=tm_mask, mask=mask, backSub=backSub).start(arg=arquivo)

                # trecho de código que aguarda a classe carregar totalmente
                while True:
                    time.sleep(0.1)
                    if not video_inferencia.t.is_alive() or video_inferencia.fps is None:
                        continue
                    else:
                        break

                loja = video_inferencia.loja
                hr_inicio = video_inferencia.hr_inicio
                fps = video_inferencia.fps

                num_frame = 0
                num_frame_movi = 0
                contador_lote = 0
                detected_motion = False
                vetor_frames = []
                carga_obj_gravacao = True
                while True:

                    # fila aux_QQ armazenada na GPU assim como o registro obtido pelo comando 'get'
                    status_termino, fg_mask, frame_out = video_inferencia.QQ_video.get()

                    if status_termino:
                        video_inferencia.status_termino=True
                        break

                    num_frame += 1
                    contador_lote += 1

                    vetor_frames.append(frame_out)

                    # limite para diminuir as sombras
                    retval, mask_thresh = cv2.threshold(fg_mask, 180, 255, cv2.THRESH_BINARY)
                    # configurar o modulo (kernel)
                    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
                    # tecnica de erosão para modelar melhor o contorno
                    mask_eroded = cv2.morphologyEx(mask_thresh, cv2.MORPH_OPEN, kernel)
                    # cv2.imshow('Frame_erosao', mask_eroded)
                    # cv2.waitKey(1)

                    # encontrar os contornos
                    contours, hierarchy = cv2.findContours(mask_eroded, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

                    # limite definido por área mínima dos contornos
                    min_contour_area = 250
                    large_contours = [cnt for cnt in contours if cv2.contourArea(cnt) > min_contour_area]

                    if len(large_contours) > 0 and num_frame > 1: # o 1o frame é utilizado como base para o background
                        detected_motion = True

                    # regra que determina que qq movimento dentro de um lote de 10 frames, considera gravar os 10 frames => efeito
                    # garantidor para não perdermos movimento algum.
                    if contador_lote == 10:
                        contador_lote = 0
                        if detected_motion:
                            if carga_obj_gravacao: # carregar o objeto de gravacao apenas uma vez por video lido.
                                # criar objeto ffmpeg de gravação frame a frame
                                p_w=int(tm_mask[0])
                                p_h=int(tm_mask[1])
                                p_fps=int(fps)
                                self.process = (
                                    ffmpeg
                                    .input('pipe:', format='rawvideo', pix_fmt='rgb24', s='{}x{}'.format(p_w, p_h))
                                    .output(self.arquivo_saida, pix_fmt='yuv420p', vcodec='libx264', crf=21, loglevel="quiet")
                                    .overwrite_output()
                                    .run_async(pipe_stdin=True)
                                )
                                carga_obj_gravacao=False

                            for i in vetor_frames:
                                num_frame_movi += 1
                                #i = cv2.putText(i, 'frame mov ' + str(num_frame_movi), (550, 340), 0, 0.3, [255, 255, 255], thickness=1,lineType=cv2.LINE_AA)
                                self.process.stdin.write(
                                    cv2.cvtColor(i, cv2.COLOR_BGR2RGB)
                                    .astype(np.uint8)
                                    .tobytes()
                                )
                                print(nome_thread, ' Num frame mov', num_frame_movi, ' Fila frames', video_inferencia.QQ_video.qsize())
                            detected_motion = False
                        vetor_frames = []

                # fechar objeto de gravação, liberando memoria, caso tenha sido carregado.
                if detected_motion:
                    if carga_obj_gravacao: # carregar o objeto de gravacao apenas uma vez por video lido.
                        # criar objeto ffmpeg de gravação frame a frame
                        p_w=int(tm_mask[0])
                        p_h=int(tm_mask[1])
                        p_fps=int(fps)
                        self.process = (
                            ffmpeg
                            .input('pipe:', format='rawvideo', pix_fmt='rgb24', s='{}x{}'.format(p_w, p_h))
                            .output(self.arquivo_saida, pix_fmt='yuv420p', vcodec='libx264', crf=21, loglevel="quiet")
                            .overwrite_output()
                            .run_async(pipe_stdin=True)
                        )
                        carga_obj_gravacao=False
                    # antes de fechar o objeto de gravação, verificamos se o vetor tem frames e caso sim gravamos, eliminando a possibilidade de deixarmos movimentos de fora.
                    for i in vetor_frames:
                        num_frame_movi += 1
                        i = cv2.putText(i, 'frame mov ' + str(num_frame_movi), (550, 340), 0, 0.3, [255, 255, 255], thickness=1,lineType=cv2.LINE_AA)
                        self.process.stdin.write(
                                    cv2.cvtColor(i, cv2.COLOR_BGR2RGB)
                                    .astype(np.uint8)
                                    .tobytes()
                                )
                        print(nome_thread, ' Num frame mov', num_frame_movi, ' Fila frames', video_inferencia.QQ_video.qsize())
                # verificar se houve gravação
                if num_frame_movi > 0:
                  self.process.stdin.close()
                  self.process.wait()
                  self.process.kill()

                # calculo do percentual de redução na qtde total de frames
                red_x = round(((num_frame - num_frame_movi) / num_frame) * 100, 2)

                tempo_final = time.time() - tempo_inicial
                print('Tmp processamento', round(tempo_final, 2), 'segundos')
                print('Redução qtde frames processados', loja, hr_inicio, red_x, '%')

            # remover o arquivo .dav para não ser reprocessado na próxima execução em lote, que por sua vez 'enxerga' sempre o mesmo diretório.
            os.remove(arquivo)

            #print(round(time.time()-tmp_original,2), 'seg')
            print('[INFO] ', nome_thread + '_' + str(contador_arquivos), ' fim redimensionamento em ', str(round(time.time()-tmp_original,2)), 'seg')
