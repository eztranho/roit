#  ROIT :-)

ETL de dados da Receita Federal criado em Python e PostgreSQL. Projeto proposto pela [Roit](https://roit.ai/).

___

### Índice
+ [Introdução](#Introdução)
+ [E o que foi feito?](#E-o-que-foi-feito)
  - [E funciona?](#E-funciona)
  - [E tá bonito?](#E-tá-bonito)
+ [O que não foi feito?](#O-que-não-foi-feito)
+ [Surpresas no meio do caminho](#Surpresas-no-meio-do-caminho)
+ [O que pode ser melhorado em uma próxima sprint?](#O-que-pode-ser-melhorado-um-uma-próxima-sprint)
+ [Tecnologias usadas](#Tecnologias-usadas)
+ [Como usar?](#Como-usar)
+ [Qual é o volume de dados no Data lake?](#Qual-é-o-volume-de-dados-no-Data-lake)
+ [E o que você aprendeu?](#E-o-que-você-aprendeu)
___
### Introdução

Esse projeto foi criado pela ROIT, para a vaga de estagiário em Engenharia de dados. A ideia é automaticamente baixar dados dos arquivos Empresa, Sócio e Estabelecimento do site da Receita Federal (https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj), fazer transformações e alimentar um banco de dados de escolha do programador. Deve também ser criado um data lake na própria máquina com as camadas "raw", "standardized", "conformed" e "aplicação" em nível crescente de padronização. O programa deve ser agendado para execução periódica.

### E o que foi feito?

Foi criado um arquivo config.py para separar configuração de implementação (não está perfeito ainda, mas está funcional). 
Com uma amostra dos dados das tabelas da Receita, foi criada uma primeira modelagem em PostgreSQL. Os dados e restrições foram implementados em 2 scripts: /sql/create_tables.sql (define o schema) e /sql/insert_intos.sql (povoa o banco de dados com informações iniciais). 
Na camada raw do data lake, foi colocado o arquivo em .zip; na camada standardized, o .csv extraído do .zip; na camada conformed, o .csv com dados transformados, prontos para entrada no PostgreSQL; na camada aplicação, foi colocado o full backup dos dados em .sql. O agendamento da tarefa foi feito com Windows Scheduler (usando a GUI própria para isso).

#### E funciona?
Sim.

#### E tá bonito?
Não. O Transform do ETL está spaghetti, tem partes repetidas e o arquivo é uma afronta ao Single Responsability Principle. Faltam unit tests também, mas eles provavelmente ficam mais bem feitos depois que um design pattern for escolhido pro Transform.

### E o que não foi feito?
O backup não foi feito programaticamente, foi feito manualmente usando pgAdmin. A versão automatizada ficará para uma próxima sprint.

### Surpresas no meio do caminho
Os dados da Receita estavam com muitas inconsistências: empresa que foi aberta no dia 00, países não cadastrados, caracteres inválidos etc. Algumas restrições foram anotadas no arquivo sql/fonte_de_dados.xlsx. Mas eram tantas inconsistências que eu parei de anotar e simplesmente relaxei as exigências e deixei a modelagem para a próxima sprint. Muitos campos foram colocados como VARCHAR(255) e FOREIGN KEYS foram tiradas para que os dados pudessem entrar no BD. O memory footprint está altíssimo. 

![receita_golden](https://lh3.googleusercontent.com/YK64i50hYcC6bmfoQE4ztqxbu_4i7YVVzNnFdt5dKoQ-YJ7m5_7o_ULhMYZLREtW7gBqFOZygQltXdIbeMKzI8rqXRYMGWDHLr6G3iwylvgK5uJ-1EZxE8bO1jOJBIruCX13D2RCI5KFasTQabBXgixGlKHg3GmPY3e1eeE5obRqeXu-8SdiACTZRVn_w6eAf9g1LmDghYAdkndgj0eLD3mnt2bGnVrtHuZBN5Rg2hIOgNFMiTLX9BhAdyxJ-6dsn-laLdFe1pTZdgfhSi_PTXpXM--A9KoAt0jfgIvYkf7pgg4yy9lJx-McbQFwkSUg01M1NS27EOaZzyqZgCNgY6CFbVd09hlNDwG1s77-JKYm-VAgKHP74oS5LomL7MCabxCikEbdNJE1bVIRcBm-LdAvt2Iw5BCRO7P1JS1jhWwOFeRL53iWIWn1i40zlMfP6hx4fL6As-5LTe0_AsW5NxGtWD4FEjxTJgTbA1MpiT0OcYnmOBzg6nP1pEhd1Ga8FSmGsguKNq-ZIitRcrDjtRZdq2gxPabnqBbbYPsutenWyFEiXQrTTjec9PM0jv12Goxt6vJPz9xTbudq8rM2hykpSE8fMV7XEdAzevhFAXwcnP7J8d_pQ9CdTNQ9oCIQC2FDiMLbpgAOqU-qGiC3szZ85HfAF57drm7u3Oeo03uuTuXrkIPT4EjlsqzC00ZP1WU4Ss4c-qgIl7na326xbB0=w501-h500-no?authuser=0)

### O que pode ser melhorado em uma próxima sprint?
Em ordem de urgência:
1. Criar um módulo para ORM e forçar restrições de banco de dados antes de fazer Load;
2. Implementar um design pattern para o Transform. Pipes and Filters é um design sensacional! (veja [aqui](https://docs.microsoft.com/en-us/previous-versions/msp-n-p/dn568100(v=pandp.10)?redirectedfrom=MSDN)). Daria pra criar um módulo só para Filters, onde os testes devem ser bem robustos. Cada filter usa a mesma interface para input e output (ou seja, pega um dataframe retorna um dataframe; ou pega uma series e retorna uma series). Assim, eles podem ser reusados em tabelas diferentes (muito útil para colunas de datas). E também podem usados em sequência. Esse módulo ficaria fácil de estender e de fazer plug-and-play para transformações mais complexas (até arrepiei!);
3. Trocar prints por logging com timestamp.
### Tecnologias usadas
+ python=3.8.8
+ postgresql=14.1.1
### Como rodar o programa?
1. Criar banco de dados local e colocar os parâmetros em config.py (nome da database, username, password, host e port);
2. Rodar os 2 scripts: sql/create_tables.sql e sql/insert_intos.sql;
3. Escrever full_path do pasta onde ficará o data lake e colocar em DATA_LAKE_PATH em config.py (como exemplo, eu deixei o caminho que eu usei comentado).
### Qual é o volume de dados exigido no data lake?
Na rodada atual (fev/2022):
+ Raw: 807 MB
+ Standardized: 2.65 GB
+ Conformed: 2.17GB
+ Aplicação: 11.8 GB (foi usado dump com insert intos e arquivo final com extensão em .sql por ser mais geral que as outras formas de backup. Uma versão compactada poderia ter sido usada, diminuindo esse tamanho)
### E o que você aprendeu?
+ A não assumir que os dados vão vir com as restrições de integridade prometidas. Criar um modelo de dados e rejeitar todas as linhas que o violem é muito mais eficiente que ficar criando exceções e tentando tratar entradas ruins;
+ Não procurar a perfeição logo no primeiro momento. Vai ficar feio antes de ficar bonito!
+ Dask é muito mais rápido que pandas! (quando o volume de dados é grande);
+ Esse Pipes and Filters é uma puta ideia inteligente!
