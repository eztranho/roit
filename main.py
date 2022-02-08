# -*- coding: utf-8 -*-

from data_lake import criar_camadas, criar_caminhos
from extract import extrair_um, mock_extrair_um
from transform import (de_raw_para_standardized, de_standardized_para_conformed,
                       de_csv_para_csv_transformado, deletar_csvs_que_nao_terminam_em_numero)
from load import load
from config import URLS
from os import path

"!!! cf chdir"
"!!! arquivos são substituídos em vez de acumulados com versoes diferentes no data lake"
"!!! criar log em vez de print"
"!!! colocar um design pattern para tirar spaguetti do codigo"

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
        
        obj = de_csv_para_csv_transformado(caminho_conformed)
        
        # !!!  verificar consistência tem que vir antes de load
        
        load(obj)
        
    #==========================================================================    
    # deletar csvs sem número de caminho_conformed
    pasta = path.dirname(caminho_conformed)
    deletar_csvs_que_nao_terminam_em_numero(pasta)
    
    # acertar erro to type numeric --> precisa transformar o dataframe primeiro
    # '1000,00'
    

