# -*- coding: utf-8 -*-
'''
arquivo de configuração do pipeline
'''

#==============================================================================
# DATA LAKE

'''
cada camada é implementada como uma pasta em DATA_LAKE_PATH
'''
DATA_LAKE_PATH = 'C:\\Users\\Tales\\Desktop\\data_lake'
CAMADAS        = ('raw','standardized', 'conformed', 'aplicacao')
EXTENSOES      = ('.zip', '.csv', '.csv', '.sql')
NOMES          = ('empresa', 'socio', 'estabelecimento')

#==============================================================================
# EXTRACT

'''
cada url aponta para a localização do recurso na web
cada nome em NOMES_RAW diz como o arquivo vai ser chamado
'''

URLS = ('http://200.152.38.155/CNPJ/K3241.K03200Y0.D20108.EMPRECSV.zip',
        'http://200.152.38.155/CNPJ/K3241.K03200Y0.D20108.SOCIOCSV.zip',
        'http://200.152.38.155/CNPJ/K3241.K03200Y0.D20108.ESTABELE.zip')




#==============================================================================
# TRANSFORM
'''
cada nome em NOMES_STANDARDIZED diz como o arquivo vai ser chamado
'''




#==============================================================================
# LOAD
