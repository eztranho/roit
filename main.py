# -*- coding: utf-8 -*-

from data_lake import criar_camadas
from extract import extrair_um
from config import DATA_LAKE_PATH, NOMES, URLS, CAMADAS
from os import sep

def criar_caminhos_raw():
    caminhos = [DATA_LAKE_PATH + sep + CAMADAS[0] + sep + nome for nome in NOMES]
    return caminhos

if __name__ == '__main__':
    # cria pastas para cada camada do data lake, se elas não existirem
    criar_camadas()
    
    # cria full path dos arquivos zip na camada raw do data lake
    caminhos_raw = criar_caminhos_raw()
    
    for url, caminho in zip(URLS, caminhos_raw):
        try:
            extrair_um(url, caminho)
        except:
            continue # se houver erro em um arquivo, avança para o próximo
        
        print('transform goes here')
        
        print('load goes here')
    
    
    

