# -*- coding: utf-8 -*-

from requests import get
from sys import exc_info

def extrair_um(url, caminho):
    print(f'começando a baixar o arquivo em {url}\npode ir tomar café porque vai demorar')
    
    try:
        request = get(url)
        
        with open(caminho, 'wb') as f:
            f.write(request.content)
            print(f'arquivo baixado de {url}\npara{caminho}')
    
    except Exception:
        print(f'erro ao baixar {url}')
        print(f'{exc_info[1]}')
        

