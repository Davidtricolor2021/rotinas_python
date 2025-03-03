import os

# Caminho da pasta onde os arquivos estão
caminho_pasta = r'C:\\Users\\david\\OneDrive\\Documentos\\AUDITORIA\\PARME\\video'  # Substitua com o caminho correto

# Percorre todos os arquivos na pasta
for nome_arquivo in os.listdir(caminho_pasta):
    # Verifica se o arquivo tem o nome que começa com 'SDXESPMM_ch1_main_'
    if nome_arquivo.startswith("SDXESPMM_ch1_main_") and nome_arquivo.endswith(".dav"):
        # Cria o novo nome, substituindo 'SDXESPMM_ch1_main_' por 'SDXESPMM_NVR2_ch1_main_'
        novo_nome = nome_arquivo.replace("SDXESPMM_ch1_main_", "SDXESPMM_NVR2_ch1_main_")
        
        # Define os caminhos completos para o arquivo antigo e o novo
        caminho_antigo = os.path.join(caminho_pasta, nome_arquivo)
        caminho_novo = os.path.join(caminho_pasta, novo_nome)
        
        # Renomeia o arquivo
        os.rename(caminho_antigo, caminho_novo)
        print(f'Renomeado: {nome_arquivo} -> {novo_nome}')
