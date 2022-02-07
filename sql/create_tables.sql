-- script para criar a primeira versão do banco de dados

DROP TABLE IF EXISTS Empresa, Socio, Estabelecimento CASCADE;
DROP TABLE IF EXISTS NaturezaJuridica, Qualificacao, Porte, IdentSocio, Pais, FaixaEtaria, IdentMatrizFilial, SitCadastral, MotivoSitCadastral, CnaeFiscal, SitEspecial;

DROP TYPE IF EXISTS ident_socio_id_enum, faixa_etaria_id_enum, ident_matriz_filial_id_enum, sit_cadastral_id_enum;

-- cria ENUMs

CREATE TYPE ident_socio_id_enum AS ENUM ('1', '2', '3');
CREATE TYPE faixa_etaria_id_enum AS ENUM ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9');
CREATE TYPE ident_matriz_filial_id_enum AS ENUM ('1', '2');
CREATE TYPE sit_cadastral_id_enum AS ENUM('01', '02', '03', '04', '08');

-- cria tabelas pequenas

CREATE TABLE NaturezaJuridica (
    natureza_juridica_id CHAR(4) NOT NULL PRIMARY KEY,
	natureza_juridica_nome VARCHAR(255) -- !!! pegar o maior valor
);

CREATE TABLE Qualificacao (
    qualificacao_id CHAR(2) NOT NULL PRIMARY KEY,
	qualificacao_descricao VARCHAR(255) -- !!! pegar o maior valor
);

CREATE TABLE Porte (
    porte_id CHAR(2) NOT NULL PRIMARY KEY,
	porte_descricao VARCHAR(255) -- !!! pegar o maior valor
);

CREATE TABLE IdentSocio (
    ident_socio_id ident_socio_id_enum NOT NULL PRIMARY KEY,
	ident_socio_descricao VARCHAR(20) -- !!! pegar o maior valor
);

CREATE TABLE Pais (
    pais_id CHAR(3) NOT NULL PRIMARY KEY,
	pais_nome VARCHAR(100) -- pegar o maior valor
);

CREATE TABLE FaixaEtaria (
    faixa_etaria_id faixa_etaria_id_enum NOT NULL PRIMARY KEY,
	faixa_etaria_descricao VARCHAR(50)
);

CREATE TABLE IdentMatrizFilial (
    ident_matriz_filial_id ident_matriz_filial_id_enum NOT NULL PRIMARY KEY,
	ident_matriz_filiar_descricao VARCHAR(15) -- pegar o maior valor
);

CREATE TABLE SitCadastral (
    sit_cadastral_id sit_cadastral_id_enum NOT NULL PRIMARY KEY,
	sit_cadastral_descricao VARCHAR(255)
);

CREATE TABLE MotivoSitCadastral (
    motivo_sit_cadastral_id CHAR(2) NOT NULL PRIMARY KEY,
	motivo_sit_cadastral_descricao VARCHAR(255) -- pegar o maior valor
);

CREATE TABLE CnaeFiscal (
    cnae_fiscal_id CHAR(7) NOT NULL PRIMARY KEY,
	cnae_fiscal_descricao VARCHAR(255) -- pegar o maior valor
);

CREATE TABLE SitEspecial (
    sit_especial_id CHAR(3) NOT NULL PRIMARY KEY,
	sit_especial_descricao VARCHAR(255)
);

-- cria tabelas grandes

CREATE TABLE Empresa (
    cnpj_basico CHAR(8) NOT NULL PRIMARY KEY,
	razao_social VARCHAR(255),  -- !!! pegar o maior valor
	natureza_juridica_id CHAR(4), 
    qualificacao_do_resp_id CHAR(2), 
    capital_social NUMERIC(9,2), 
    porte_id CHAR(2), 
    ente_federativo VARCHAR(255),
	FOREIGN KEY (natureza_juridica_id) REFERENCES NaturezaJuridica(natureza_juridica_id),
	FOREIGN KEY (qualificacao_do_resp_id) REFERENCES Qualificacao(qualificacao_id)
);

CREATE TABLE Socio (
    cnpj_basico CHAR(8) NOT NULL PRIMARY KEY, 
	ident_socio_id ident_socio_id_enum, 
	nome_ou_razao_social VARCHAR(255), 
    cnpj_ou_cpf CHAR(14), -- tem * nos dados 
	qualificacao_socio_id CHAR(2), 
	data_entrada DATE, 
    pais_id CHAR(3), 
	cpf_representante_legal CHAR(11), 
    nome_representante VARCHAR (255), -- sempre tem um Dom Pedro no meio com 20 sobrenomes 
	qualificacao_representante_id CHAR(2),
    faixa_etaria_id faixa_etaria_id_enum,
	FOREIGN KEY (ident_socio_id) REFERENCES IdentSocio(ident_socio_id),
	FOREIGN KEY (qualificacao_socio_id) REFERENCES Qualificacao(qualificacao_id),
	FOREIGN KEY (pais_id) REFERENCES Pais(pais_id),
	FOREIGN KEY (qualificacao_representante_id) REFERENCES Qualificacao(qualificacao_id)
);

CREATE TABLE Estabelecimento (
    cnpj_basico CHAR(8) NOT NULL, 
	cnpj_ordem CHAR(4) NOT NULL, 
	cnpj_dv CHAR(2) NOT NULL, 
	ident_matriz_filial_id ident_matriz_filial_id_enum,
    nome_fantasia VARCHAR(255), 
	sit_cadastral_id sit_cadastral_id_enum, 
	data_sit_cadastral DATE,
    motivo_sit_cadastral_id CHAR(2), 
	nome_cidade_exterior VARCHAR(255), 
	pais_id CHAR(3),
    data_inicio_atividade DATE, 
	cnae_fiscal_principal_id CHAR(7), 
    cnae_fiscal_secundario_id TEXT [], 
	tipo_logradouro VARCHAR(255), -- poderia ser ENUM?
    logradouro VARCHAR(255), 
	numero VARCHAR (10), 
	complemento VARCHAR(255), 
	bairro VARCHAR(255),
    cep CHAR(8), 
	uf CHAR(2), -- tem um EX no meio, possivelmente um ES digitado errado 
	municipio VARCHAR(255), 
	ddd_1 CHAR(2), -- tem telefone de SP com 8 dígitos, sendo que lá se usam 9
	tel_1 CHAR(9), 
	ddd_2 CHAR(2), 
	tel_2 CHAR(9), 
	ddd_fax CHAR(2), 
	tel_fax CHAR(9), 
	email VARCHAR(255), 
	sit_especial_id CHAR(3), 
	data_sit_especial DATE,
	PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv),
	FOREIGN KEY (ident_matriz_filial_id) REFERENCES IdentMatrizFilial(ident_matriz_filial_id),
	FOREIGN KEY (sit_cadastral_id) REFERENCES SitCadastral(sit_cadastral_id),
	FOREIGN KEY (motivo_sit_cadastral_id) REFERENCES MotivoSitCadastral(motivo_sit_cadastral_id),
	FOREIGN KEY (pais_id) REFERENCES Pais(pais_id),
	FOREIGN KEY (cnae_fiscal_principal_id) REFERENCES CnaeFiscal(cnae_fiscal_id),
	FOREIGN KEY (sit_especial_id) REFERENCES SitEspecial(sit_especial_id)
);

