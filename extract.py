# -*- coding: utf-8 -*-
'''
extrai arquivos
'''
#==============================================================================
from requests import get
from sys import exc_info
#==============================================================================
def extrair_um(url, caminho):
    '''
    baixa o arquivo em urlpara o filepath em caminho
    mode = 'wb' está hardcoded
    ''' 
    print(f'começando a baixar o arquivo em {url}\npode ir tomar café porque vai demorar')
    
    try:
        request = get(url)
        
        with open(caminho, 'wb') as f:
            f.write(request.content)
            print(f'arquivo baixado de {url}\npara {caminho}')      
    except Exception as e:
        print(f'erro ao baixar {url}')
        print(f'classe: {exc_info()[0]}')
        print(f'mensagem: {exc_info()[1]}')
        raise e

def mock_extrair_um(url, caminho):
    '''
    simula extrair_um(url, caminho) para não ter que esperar o download
    durante o desenvolvimento
    '''
    print(f'fingindo que está baixando o arquivo em {url}')
    print(f'arquivo baixado de {url}\npara{caminho}')
