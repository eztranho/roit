# -*- coding: utf-8 -*-
'''
faz todo o ETL do site da Receita até o banco de dados local
faz um setup inicial criando as pastas escritas em config.py
processa um arquivo (Empresa, Socio, Estabelecimento) por vez
em caso de erro na etapa de Extract, avança para o próximo arquivo
em caso de erro na etapa de Load, quebra tudo
etapa de dump não está implementada ainda
'''
#==============================================================================
from data_lake import criar_camadas, criar_caminhos
from extract import extrair_um
from transform import (de_raw_para_standardized, de_standardized_para_conformed,
                       de_csv_para_csv_transformado, deletar_csvs_que_nao_terminam_em_numero)
from dump import dump
from load import load
from config import URLS
from os import path
#==============================================================================
if __name__ == '__main__':
    # cria pastas para cada camada do data lake, se elas não existirem
    criar_camadas()
    
    # cria full path dos arquivos zip na camada raw e standardized do data lake
    caminhos_raw   = criar_caminhos('raw')
    caminhos_stand = criar_caminhos('standardized')
    caminhos_conformed = criar_caminhos('conformed')
    
    for url, caminho_raw, caminho_stand, caminho_conformed in zip(URLS,
                                                                  caminhos_raw, 
                                                                  caminhos_stand, 
                                                                  caminhos_conformed):
        
        # EXTRACT
        try:
            extrair_um(url, caminho_raw) 
        except:
            continue # se houver erro em um arquivo, avança para o próximo
                     # a exceção já é printada pela função
        
        # MOVE
        try:
            de_raw_para_standardized(caminho_raw, caminho_stand)
        except:
            continue # a exceção já é printada pela função
            
        # MOVE
        de_standardized_para_conformed(caminho_stand, caminho_conformed)
        
        # TRANSFORM
        obj = de_csv_para_csv_transformado(caminho_conformed)
        
        # LOAD
        load(obj)
        
        # disponibilizar para a aplicacao
        dump()
        
    #==========================================================================    
    # deletar csvs sem número que ficaram em caminho_conformed
    pasta = path.dirname(caminho_conformed)
    deletar_csvs_que_nao_terminam_em_numero(pasta)
    
'''
uau! você foi visitado(a) pela capivara princesa!
       /)─👑ヘ
　 　＿／　　　　  ＼
 ／　　　　   ●　　●丶
 |　　　　　　　  ▼　  |
 | 　　　　   亠  ノ
　U￣U￣ ￣ U￣U
vai ter sorte, dinheiro, fama e poder pelos próximos 7 anos!
'''