# -*- coding: utf-8 -*-
from zipfile import ZipFile
from os import path, chdir, remove
from glob import glob
from shutil import move
'''

'''

def listar_arquivos_em_raw(caminho_pasta):
    chdir(caminho_pasta)
    
    arquivos_em_raw = glob('*')
    return arquivos_em_raw

def diff_entre_listas(lista_menor, lista_maior):
    s1 = set(lista_menor)
    s2 = set(lista_maior)
    
    diff = s2 - s1
    return list(diff)
    
def de_raw_para_standardized(caminho_raw, caminho_stand):
    print(f'começando a extrair o zip em {caminho_raw}')
    
    caminho_pasta   = path.dirname(caminho_raw)
    arquivos_em_raw = listar_arquivos_em_raw(caminho_pasta)
    
    # pegar um .zip de raw e extrair dentro de raw mesmo
    with ZipFile(caminho_raw, 'r') as zip_obj:
        zip_obj.extractall(caminho_pasta) # assume que só tem 1 csv dentro de zip
    
    # como extractall não deixa escolher o nome do csv, vamos ter que descobrir
    # pegando os novos arquivos que apareceram em raw
    novos_arquivos_em_raw = listar_arquivos_em_raw(caminho_pasta)
    
    arquivos_csv = diff_entre_listas(arquivos_em_raw, novos_arquivos_em_raw)
    
    '''
    este script assume que só tem 1 csv dentro de cada zip!
    dentro de config.py, a variável NOMES_STANDARDIZED só tem 1 valor
    então, fail fast
    '''
    
    if len(arquivos_csv) != 1:
        # primeiro, apague todos os arquivos
        [*map(remove, arquivos_csv)]
        
        # quebre tudo
        raise ValueError('o arquivo .zip em {caminho_raw} deve ter exatamente 1 .csv')
    else:
        arquivo_csv = arquivos_csv[0]
        print(f'arquivo csv extraído com sucesso: {arquivo_csv}')
    
    # mover o csv para pasta standardized
    move(arquivo_csv, caminho_stand)
    print(f'arquivo csv movido para {caminho_stand}')