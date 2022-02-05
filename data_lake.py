# -*- coding: utf-8 -*-

from config import DATA_LAKE_PATH, CAMADAS
from pathlib import Path
from os import sep
    
def criar_camadas():   
    caminhos = [DATA_LAKE_PATH + sep + camada for camada in CAMADAS]
    
    for caminho in caminhos:
        Path(caminho).mkdir(parents = True, exist_ok = True)
        print(f'pasta criada ou já existente em {caminho}')