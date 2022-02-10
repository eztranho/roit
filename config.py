# -*- coding: utf-8 -*-
'''
arquivo de configuração do pipeline
'''

#==============================================================================
# DATA LAKE
'''
cada camada é implementada como uma pasta em DATA_LAKE_PATH

CAMADAS diz o nome de cada pasta no data lake. caso haja alteração nas
camadas, CAMADAS deverá ser atualizada

EXTENSOES diz qual será a extensão dos arquivos em cada camada 
--> e.g. em raw --> zip; em aplicacao --> sql

NOMES diz qual é o tipo de arquivo que irá passar pelo ETL, caso a Receita
crie mais tabelas, NOMES deverá ser estendido

'''
DATA_LAKE_PATH = '' # 'C:\\Users\\Tales\\Desktop\\data_lake'
CAMADAS        = ('raw','standardized', 'conformed', 'aplicacao')
EXTENSOES      = ('.zip', '.csv', '.csv', '.sql')
NOMES          = ('empresa', 'socio', 'estabelecimento')

#==============================================================================
# EXTRACT
'''
cada url aponta para um link do site da Receita Federal
'''
URLS = ('http://200.152.38.155/CNPJ/K3241.K03200Y0.D20108.EMPRECSV.zip',
        'http://200.152.38.155/CNPJ/K3241.K03200Y0.D20108.SOCIOCSV.zip',
        'http://200.152.38.155/CNPJ/K3241.K03200Y0.D20108.ESTABELE.zip')

#==============================================================================
# LOAD
DB_NAME = ''    # 'roit'
USER =  ''      # 'postgres'
PASSWORD = ''   # 'superuserpass'
HOST = ''       # 'localhost'
PORT = ''       # '5432'

#==============================================================================
# DUMP --> não implementado
CAMINHO_PG_DUMP = '' # 'C:\\Program Files\\PostgreSQL\14\\bin'

