# -*- coding: utf-8 -*-
'''
transforma e move arquivos
futuramente deve ser separado para fazer apenas uma coisa
'''
#==============================================================================
from zipfile import ZipFile
from os import path, chdir, remove, sep
from glob import glob
from shutil import move, copyfile
from pathlib import Path
import dask.dataframe as dd
#==============================================================================
# HELPER FUNCTIONS
def get_nome_do_arquivo(caminho_conformed):
    '''
    pega o nome do arquivo sem extensão. usado para saber qual classe
    será instanciada: 
        se nome é 'empresa' --> Empresa()
        se nome é 'socio'   --> Socio()
        e por aí vai
    
    e.g.
    >> get_nome_do_arquivo('C:\\Users\\Tales\\Desktop\\data_lake\\standardized\\empresa.csv')
    
    'empresa'
    
    '''
    nome = Path(caminho_conformed).stem
    return nome

def listar_arquivos_em_pasta(caminho_pasta):
    '''
    retorna lista de filepaths em caminho_pasta
    '''
    arquivos = glob(caminho_pasta + sep + '*')
    return arquivos

def diff_entre_listas(lista_menor, lista_maior):
    '''
    retorna elementos em lista_maior que não estão em lista_menor
    usado para saber quais arquivos novos foram criados em uma pasta
    '''
    s1 = set(lista_menor)
    s2 = set(lista_maior)
    
    diff = s2 - s1
    return list(diff)

def deletar_csvs_que_nao_terminam_em_numero(pasta):
    '''
    apaga todos arquivos que terminam em .csv em pasta se nao terminarem em 
    número
    
    pasta\\socio.csv   --> será apagado
    pasta\\socio_0.csv --> não será apagado
    
    '''
    numeros_str = [*map(str, [*range(10)])] # ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    
    chdir(pasta)
    todos_csvs = glob('*.csv')
    
    csvs_sem_numero = [get_nome_do_arquivo(um_csv) + '.csv' # pegue socio + '.csv
                       for um_csv in todos_csvs             
                       if get_nome_do_arquivo(um_csv)[-1] not in numeros_str] # se o último char não for um número
    
    print(f'apagando os arquivos {csvs_sem_numero}')
    [*map(remove, csvs_sem_numero)]

#=====================================================================================
# CLASSES
class Arquivo():
    '''
    representa um arquivo no data lake que será processado. é basicamente um
    dask.dataframe (a documentação recomenda não estender diretamente o dataframe)
    
    base class que tem 3 tipos: empresa, socio e estabelecimento
    as colunas mudam, o dtype muda e processamento muda
    getter, setter não mudam
    '''
    def __init__(self, caminho_conformed):
        self.caminho_conformed = caminho_conformed
        
    def _process_df(self):
        pass # override 
        
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
        '''
        pré-processa o arquivo csv,
        lê o csv como um dask.dataframe
        executa o processamento e salva em cima do dataframe antigo
        
        futuramente separar essas funções usando Pipes and Filters Pattern
        '''
        # pré-processar
        self._tirar_backslash() 
        
        # ler o csv e salvar como dataframe
        self.df = dd.read_csv(self.caminho_conformed, 
                              sep = ';',
                              header = 0,
                              encoding = 'unicode_escape', # gambiarra para um caractere esotérico que empresa.csv tem --> byte 0xc7 
                              dtype = str) 
        self.df.columns = self.columns
        
        # processar o dataframe
        self._process_df()
        
    def get_df(self):
        return self.df
  
class Empresa(Arquivo):
    '''
    representa um arquivo de empresa vindo da base de dados da receita
    '''
    columns = ['cnpj_basico', 'razao_social', 'natureza_juridica_id', 
               'qualificacao_do_resp_id', 'capital_social', 'porte_id', 'ente_federativo']
    nome_tabela = 'empresa' # isso deveria ser gerado a partir de NOMES em config.py
    
    def __init__(self, caminho_conformed):
        super().__init__(caminho_conformed)
        
    def _process_df(self):
        '''
        aplica transformacao específica aos arquivos empresa
        neste momento, troca coluna capital_social para ser float em vez de string
        '''
        # capital_social vem como '1.000,00' --> substituir ',' por ponto
        sem_virgula = self.df.capital_social.map(lambda string: string.replace(',', '.'))
        
        # deixar dtype como float
        self.df['capital_social'] = sem_virgula.astype('float64')
        
        
class Socio(Arquivo):
    '''
    representa um arquivo de socio vindo da base de dados da receita
    '''
    columns = ['cnpj_basico', 'ident_socio_id', 'nome_ou_razao_social', 
               'cnpj_ou_cpf', 'qualificacao_socio_id', 'data_entrada', 
               'pais_id', 'cpf_representante_legal', 
               'nome_representante', 'qualificacao_representante_id',
               'faixa_etaria_id']
    nome_tabela = 'socio'  # isso deveria ser gerado a partir de NOMES em config.py
    
    def __init__(self, caminho_conformed):
        super().__init__(caminho_conformed)
        
    def _process_df(self):
        '''
        aplica transformacao específica aos arquivos socio
        neste momento, troca coluna data_entrada para ser date em vez de string
        '''
        # muda data_entrada de '20110809' (object) para 2011-08-09 (datetime64[ns])
        self.df['data_entrada'] = dd.to_datetime(self.df.data_entrada, errors = 'coerce') # parse_dates seria melhor?
        
class Estabelecimento(Arquivo):
    '''
    representa um arquivo de estabelecimento vindo da base de dados da receita
    '''
    columns = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'ident_matriz_filial_id',
               'nome_fantasia', 'sit_cadastral_id', 'data_sit_cadastral',
               'motivo_sit_cadastral_id', 'nome_cidade_exterior', 'pais_id',
               'data_inicio_atividade', 'cnae_fiscal_principal_id', 
               'cnae_fiscal_secundario_id', 'tipo_logradouro',
               'logradouro', 'numero', 'complemento', 'bairro',
               'cep', 'uf', 'municipio', 'ddd_1', 'tel_1',
               'ddd_2', 'tel_2', 'ddd_fax', 'tel_fax',
               'email', 'sit_especial_id', 'data_sit_especial']
    nome_tabela = 'estabelecimento' # isso deveria ser gerado a partir de NOMES em config.py
    
    def __init__(self, caminho_conformed):
        super().__init__(caminho_conformed)
        
    def _process_df(self):
        '''
        aplica transformacao específica aos arquivos socio
        neste momento, troca colunas de data_entrada de string para date
        '''
        # filter pode ser criado e reusado para todas colunas de 
        # data de todas arquivos. por enquanto, enquanto está repetitivo
        self.df['data_sit_cadastral'] = dd.to_datetime(self.df.data_sit_cadastral, errors = 'coerce')
        self.df['data_inicio_atividade'] = dd.to_datetime(self.df.data_inicio_atividade, errors = 'coerce')
        self.df['data_sit_especial'] = dd.to_datetime(self.df.data_sit_especial, errors = 'coerce')
        
#=====================================================================================
# HIGH-LEVEL FUNCTIONS
def de_raw_para_standardized(caminho_raw, caminho_stand):
    '''
    pega arquivos .zip em raw e move para standardized
    '''
    
    print(f'começando a extrair o zip em {caminho_raw}')
    caminho_pasta   = path.dirname(caminho_raw)
    arquivos_em_raw = listar_arquivos_em_pasta(caminho_pasta)
    
    # pegar um .zip de raw e extrair dentro de raw mesmo
    with ZipFile(caminho_raw, 'r') as zip_obj:
        zip_obj.extractall(caminho_pasta) # assume que só tem 1 csv dentro de zip
    
    # como extractall não deixa escolher o nome do csv, vamos ter que descobrir
    # pegando os novos arquivos que apareceram em raw
    novos_arquivos_em_raw = listar_arquivos_em_pasta(caminho_pasta)
    
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
        print('o arquivo .zip em {caminho_raw} deve ter exatamente 1 .csv')
        raise ValueError('o arquivo .zip em {caminho_raw} deve ter exatamente 1 .csv')
    else:
        arquivo_csv = arquivos_csv[0]
        print(f'arquivo csv extraído com sucesso: {arquivo_csv}')
    
    # mover o csv para pasta standardized
    move(arquivo_csv, caminho_stand)
    print(f'arquivo csv movido para {caminho_stand}')

def de_standardized_para_conformed(caminho_stand, caminho_conformed):
    '''
    move um arquivo no data lake, de standardized para conformed
    '''
    print(f'copiando csv "as is" de {caminho_stand} para {caminho_conformed}')
    copyfile(caminho_stand, caminho_conformed)

def de_csv_para_csv_transformado(caminho_conformed):
    '''
    lê um arquivo csv, cria dataframe a partir dele, 
    aplica transformações no dataframe, salva versão processada
    em conformed, e no fim retorna um objeto Empresa, Socio ou Estabelecimento
    
    esta função faz muita coisa e futuramente vai ser separada
    '''   
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
    
    df = obj.get_df()                     # pega a versão processada final apenas                                          
                                          
    nome_final = path.dirname(caminho_conformed) + sep + nome_do_arquivo + '_*.csv'
    # e.g. C:\\Users\\Tales\\Desktop\\data_lake\\conformed\\empresa_0.csv
    
    # salva versão processada com números no final, a versão bruta continua na pasta
    # e é apagada depois
    df.to_csv(nome_final, sep = ';', index = False) 
                                          
    return obj # retorna Empresa(), Socio() ou Estabelecimento()