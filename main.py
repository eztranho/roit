# -*- coding: utf-8 -*-

from data_lake import criar_camadas
from extract import extrair_um, mock_extrair_um
from transform import de_raw_para_standardized
from config import DATA_LAKE_PATH, NOMES_RAW, NOMES_STANDARDIZED, URLS, CAMADAS
from os import sep

def criar_caminhos_raw():
    caminhos = [DATA_LAKE_PATH + sep + CAMADAS[0] + sep + nome for nome in NOMES_RAW]
    return caminhos

def criar_caminhos_standardized():
    caminhos = [DATA_LAKE_PATH + sep + CAMADAS[1] + sep + nome for nome in NOMES_STANDARDIZED]
    return caminhos

if __name__ == '__main__':
    
    # cria pastas para cada camada do data lake, se elas não existirem
    criar_camadas()
    
    # cria full path dos arquivos zip na camada raw e standardized do data lake
    caminhos_raw = criar_caminhos_raw()
    caminhos_stand = criar_caminhos_standardized()
    
    for url, caminho_raw, caminho_stand in zip(URLS, caminhos_raw, caminhos_stand):
        try:
            mock_extrair_um(url, caminho_raw) # !!! apenas para evitar downloads demorados
                                              # na fase de desenvolvimento
        except:
            continue # se houver erro em um arquivo, avança para o próximo
                     # a exceção já é printada pela função
        
        try:
            de_raw_para_standardized(caminho_raw, caminho_stand)
        except:
            continue # a exceção já é printada pela função
        
        print('load goes here')
    
    
    

