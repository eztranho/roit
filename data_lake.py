# -*- coding: utf-8 -*-

from config import DATA_LAKE_PATH, CAMADAS, EXTENSOES, NOMES
from pathlib import Path
from os import sep
    
def criar_camadas():   
    caminhos = [DATA_LAKE_PATH + sep + camada for camada in CAMADAS]
    
    for caminho in caminhos:
        Path(caminho).mkdir(parents = True, exist_ok = True)
        print(f'pasta criada ou já existente em {caminho}')
        
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