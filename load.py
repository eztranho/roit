# -*- coding: utf-8 -*-

#from sqlalchemy import create_engine
from config import DB_NAME, USER, PASSWORD, HOST, PORT

#ENGINE = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}')
URI = f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}'

def load(obj, uri = URI):
    # bem lento
    tabela = obj.nome_tabela
    df = obj.get_df()
    
    print(f'começando a alimentar a tabela {tabela} da database {DB_NAME}')
    df.to_sql(name = tabela, 
              uri = URI, 
              if_exists = 'append', # checar se deveria ser replace ou se deveria ser criado um UPSERT
              schema = 'public',
              index = False)
    print(f'tabela {tabela} da database {DB_NAME} carregada')
