# -*- coding: utf-8 -*-
'''
arquivo de configuração do pipeline

'''

#==============================================================================
# DATA LAKE

'''
cada camada é implementada como uma pasta em DATA_LAKE_PATH
'''

CAMADAS = ('raw','standardized', 'conformed', 'aplicacao')
DATA_LAKE_PATH = r'C:Users/Tales/Desktop/data_lake'

#==============================================================================
# EXTRACT

'''
cada url aponta para a localização do recurso na web
cada nome em NOMES diz como o arquivo vai ser chamado em raw
'''

URLS = ('https://200.152.38.155/CNPJ/K3241.K03200Y0.D20108.EMPRECSV.zip',
        'https://200.152.38.155/CNPJ/K3241.K03200Y0.D20108.SOCIOCSV.zip',
        'https://200.152.38.155/CNPJ/K3241.K03200Y0.D20108.ESTABELE.zip')

NOMES = ('empresa.zip',
         'socio.zip',
         'estabelecimento.zip')



#==============================================================================
# TRANSFORM



#==============================================================================
# LOAD
