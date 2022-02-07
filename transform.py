# -*- coding: utf-8 -*-
from zipfile import ZipFile
from os import path, chdir, remove, sep
from glob import glob
from shutil import move, copyfile
from pathlib import Path
import dask.dataframe as dd

'''
!!! alterar campos de data para separar com hífen
!!! juntar estabelecimento como um único cnpj
!!! tabela socio deve ser estendida e separada em 2 tipos: pf e pj
'''
#=====================================================================================
# HELPER FUNCTIONS

def get_nome_do_arquivo(caminho_conformed):
    nome = Path(caminho_conformed).stem
    return nome

def listar_arquivos_em_raw(caminho_pasta):
    chdir(caminho_pasta)
    
    arquivos_em_raw = glob('*')
    return arquivos_em_raw

def diff_entre_listas(lista_menor, lista_maior):
    s1 = set(lista_menor)
    s2 = set(lista_maior)
    
    diff = s2 - s1
    return list(diff)

def deletar_csvs_que_nao_terminam_em_numero(pasta):
    numeros_str = [*map(str, [*range(10)])] # ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    
    chdir(pasta)
    
    todos_csvs = glob('*.csv')
    
    csvs_sem_numero = [get_nome_do_arquivo(um_csv) + '.csv' # pegue socio + '.csv
                       for um_csv in todos_csvs             
                       if get_nome_do_arquivo(um_csv)[-1] not in numeros_str] # se o último char não for um número
    
    [*map(remove, csvs_sem_numero)]

#=====================================================================================
# CLASSES
# usar specification design pattern
    
class Arquivo(): # dask recomenda não estender dd.DataFrame, então df será atributo
    '''
    base class. tem 3 tipos: empresa, socio e estabelecimento
    as colunas mudam, o dtype muda e processamento muda
    getter e setter não mudam
    '''
    
    def __init__(self, caminho_conformed):
        self.caminho_conformed = caminho_conformed
        
    def _tirar_backslash(self):
        '''
        operacao I/O custosa por causa de um \ N
        '''
        with open(self.caminho_conformed, 'r') as f:
            string = f.read()
            string = string.replace('\\', '')
            
        with open(self.caminho_conformed, 'w') as f:
            f.write(string)
        
    def set_df(self):
        self._tirar_backslash() 
        self.df = dd.read_csv(self.caminho_conformed, 
                              sep = ';',
                              header = 0,
                              encoding = 'unicode_escape', # !!! gambiarra para um caractere esotérico que empresa.csv tem --> byte 0xc7. checar 
                              dtype = str) # por simplicidade
        self.df.columns = self.columns
        
        # !!! considerar interface fluida --> return self
        
    def get_df(self):
        return self.df
  
class Empresa(Arquivo): 
    columns = ['cnpj_basico', 'razao_social', 'natureza_juridica_id', 
               'qualificacao_do_resp_id', 'capital_social', 'porte_id', 'ente federativo']
    
    def __init__(self, caminho_conformed):
        super().__init__(caminho_conformed)
        
class Socio(Arquivo):
    columns = ['cnpj_basico', 'ident_socio_id', 'nome_ou_razao_social', 
               'cnpj_ou_cpf', 'qualificacao_socio_id', 'data_entrada', 
               'pais_id', 'cpf_representante_legal', 
               'nome_representante', 'qualificacao_representante_id',
               'faixa_etaria_id']
    
    def __init__(self, caminho_conformed):
        super().__init__(caminho_conformed)
        
class Estabelecimento(Arquivo):
    columns = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'ident_matriz_filial_id',
               'nome_fantasia', 'sit_cadastral_id', 'data_sit_cadastral',
               'motivo_sit_cadastral', 'nome_cidade_exterior', 'pais_id',
               'data_inicio_atividade', 'cnae_fiscal_principal_id', 
               'cnae_fiscal_secundario_id', 'tipo_logradouro',
               'logradouro', 'numero', 'complemento', 'bairro',
               'cep', 'uf', 'municipio', 'ddd_1', 'tel_1',
               'ddd_2', 'tel_2', 'ddd_fax', 'tel_fax',
               'email', 'sit_especial_id', 'data_sit_especial']
    
    def __init__(self, caminho_conformed):
        super().__init__(caminho_conformed)

#=====================================================================================
# HIGH-LEVEL FUNCTION
    
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

def de_standardized_para_conformed(caminho_stand, caminho_conformed):
    print(f'copiando csv "as is" de {caminho_stand} para {caminho_conformed}')
    copyfile(caminho_stand, caminho_conformed)

def de_csv_para_csv_transformado(caminho_conformed):
    print(f'começando a transformar o arquivo {caminho_conformed}')
    
    # nome_do_arquivo é socio, empresa ou estabelecimento
    nome_do_arquivo = get_nome_do_arquivo(caminho_conformed)
    
    # usando o nome do arquivo, vamos instanciar a classe certa
    mapping = dict(empresa = Empresa,
                   socio   = Socio,
                   estabelecimento = Estabelecimento)
    
    class_ = mapping.get(nome_do_arquivo) # pega a classe Socio, Empresa ou Estabelecimento 
    obj = class_(caminho_conformed)       # instancia 
    obj.set_df()                          # de fato cria o DataFrame
    
    # !!! transformações de coluna vêm aqui --> fazer primeiro a modelagem do banco de dados
    # algo como obj.transform()
    
    df = obj.get_df()                     # pega a versão processada final apenas
                                          # vai ter vários métodos private
                                          
    nome_final = path.dirname(caminho_conformed) + sep + nome_do_arquivo + '_*.csv'
    # e.g. C:\\Users\\Tales\\Desktop\\data_lake\\conformed\\empresa_0.csv
    
    df.to_csv(nome_final, sep = ';', index = False)
    
    pasta = path.dirname(caminho_conformed)
    deletar_csvs_que_nao_terminam_em_numero(pasta)
                                          
    return

