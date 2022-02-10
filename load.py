# -*- coding: utf-8 -*-
'''
pega um objeto Empresa, Socio ou Estabelecimento e alimenta o banco de dados
usando os parâmetros de config.py
adaptador psycopg2 está hardcoded
schema public está hardcoded
'''
#==============================================================================
from config import DB_NAME, USER, PASSWORD, HOST, PORT
#==============================================================================
# GLOBAL VARIABLES
URI = f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}'

#==============================================================================
def load(obj, uri = URI):
    tabela = obj.nome_tabela
    df = obj.get_df()
    
    print(f'começando a alimentar a tabela {tabela} da database {DB_NAME}')
    df.to_sql(name = tabela, 
              uri = URI, 
              if_exists = 'append', # na próxima iteração, deve usar 'replace'
              schema = 'public',
              index = False)
    print(f'tabela {tabela} da database {DB_NAME} carregada')
