# -*- coding: utf-8 -*-

from data_lake import criar_camadas
from extract import extrair_um, mock_extrair_um
from transform import (de_raw_para_standardized, de_standardized_para_conformed,
                       de_csv_para_csv_transformado)
from config import DATA_LAKE_PATH, NOMES, EXTENSOES, URLS, CAMADAS
from os import sep

"!!! cf chdir"
"!!! arquivos são substituídos em vez de acumulados com versoes diferentes no data lake"

def criar_caminhos(camada):
    '''
    assume que dentro de uma mesma camada, os arquivos terão sempre a mesma extensão
    '''
    enum_camada = [*enumerate(CAMADAS)]
    enum_extensao = [*enumerate(EXTENSOES)]
    
    # map cria {'raw': 0, 'standardized': 1, 'conformed': 2, 'aplicacao': 3}
    # se mudarem as camadas em config.py, esta parte se acerta sozinha
    map_camada = {camada: index for index, camada in enum_camada}
     
    index = map_camada.get(camada)
    
    if index is None:
        raise ValueError("input errado para camada: {camada}\nesperava um desses: {CAMADAS}")
    else:
        caminhos = [DATA_LAKE_PATH + sep + CAMADAS[index] + sep + nome for nome in NOMES] 
                    
    # adicionando a extensão em cada caminho
    ext = enum_extensao[index][1]
    caminhos = [caminho + ext for caminho in caminhos]
    
    return caminhos

if __name__ == '__main__':
    # cria pastas para cada camada do data lake, se elas não existirem
    criar_camadas()
    
    # cria full path dos arquivos zip na camada raw e standardized do data lake
    caminhos_raw   = criar_caminhos('raw')
    caminhos_stand = criar_caminhos('standardized')
    caminhos_conformed = criar_caminhos('conformed')
    
    for url, caminho_raw, caminho_stand, caminho_conformed  in zip(URLS, 
                                                                   caminhos_raw, 
                                                                   caminhos_stand, 
                                                                   caminhos_conformed):
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
            
        de_standardized_para_conformed(caminho_stand, caminho_conformed)
        
        de_csv_para_csv_transformado(caminho_conformed)
        
        print('load goes here')
    
    
    

