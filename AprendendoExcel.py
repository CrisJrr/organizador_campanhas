import pandas as pd
from datetime import datetime as date
import duckdb


# script feito pelo Eliton para transformar colunas em linhas. Fiz algumas alterações para o script trazer todas informações do cliente
df = pd.read_csv(r'C:\Users\crist\OneDrive\Documentos\Robs\Robs\EXPERIMENTANDO - 01.csv', sep=";", encoding="iso-8859-1", low_memory=False, dtype="O")
df.to_csv("temp.csv")
rr = duckdb.read_csv("temp.csv", normalize_names=True, parallel=True, all_varchar=True)


__sql__ = """
with cte as (
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #50, contrato := #51, banco := #52, data_inicio := #53, data_desconto := #54, valor_emprestimo := #55, valor_parcela := #56, prazos := #57, taxa := #58, saldo_quitacao := #59, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #47, contrato := #48, banco := #49, data_inicio := #50, data_desconto := #51, valor_emprestimo := #52, valor_parcela := #53, prazos := #54, taxa := #55, saldo_quitacao := #56, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #60, contrato := #61, banco := #62, data_inicio := #63, data_desconto := #64, valor_emprestimo := #65, valor_parcela := #66, prazos := #67, taxa := #68, saldo_quitacao := #69, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14 )
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #57, contrato := #58, banco := #59, data_inicio := #60, data_desconto := #61, valor_emprestimo := #62, valor_parcela := #63, prazos := #64, taxa := #65, saldo_quitacao := #66, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14 )
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #70, contrato := #71, banco := #72, data_inicio := #73, data_desconto := #74, valor_emprestimo := #75, valor_parcela := #76, prazos := #77, taxa := #78, saldo_quitacao := #79, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #67, contrato := #68, banco := #69, data_inicio := #70, data_desconto := #71, valor_emprestimo := #72, valor_parcela := #73, prazos := #74, taxa := #75, saldo_quitacao := #76, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #80, contrato := #81, banco := #82, data_inicio := #83, data_desconto := #84, valor_emprestimo := #85, valor_parcela := #86, prazos := #87, taxa := #88, saldo_quitacao := #89, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #77, contrato := #78, banco := #79, data_inicio := #80, data_desconto := #81, valor_emprestimo := #82, valor_parcela := #83, prazos := #84, taxa := #85, saldo_quitacao := #86, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #90, contrato := #91, banco := #92, data_inicio := #93, data_desconto := #94, valor_emprestimo := #95, valor_parcela := #96, prazos := #97, taxa := #98, saldo_quitacao := #99, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #87, contrato := #88, banco := #89, data_inicio := #90, data_desconto := #91, valor_emprestimo := #92, valor_parcela := #93, prazos := #94, taxa := #95, saldo_quitacao := #96, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #100, contrato := #101, banco := #102, data_inicio := #103, data_desconto := #104, valor_emprestimo := #105, valor_parcela := #106, prazos := #107, taxa := #108, saldo_quitacao := #109, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #97, contrato := #98, banco := #99, data_inicio := #100, data_desconto := #101, valor_emprestimo := #102, valor_parcela := #103, prazos := #104, taxa := #105, saldo_quitacao := #106, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #110, contrato := #111, banco := #112, data_inicio := #113, data_desconto := #114, valor_emprestimo := #115, valor_parcela := #116, prazos := #117, taxa := #118, saldo_quitacao := #119, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #107, contrato := #108, banco := #109, data_inicio := #110, data_desconto := #111, valor_emprestimo := #112, valor_parcela := #113, prazos := #114, taxa := #115, saldo_quitacao := #116, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #120, contrato := #121, banco := #122, data_inicio := #123, data_desconto := #124, valor_emprestimo := #125, valor_parcela := #126, prazos := #127, taxa := #128, saldo_quitacao := #129, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #117, contrato := #118, banco := #119, data_inicio := #120, data_desconto := #121, valor_emprestimo := #122, valor_parcela := #123, prazos := #124, taxa := #125, saldo_quitacao := #126, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #130, contrato := #131, banco := #132, data_inicio := #133, data_desconto := #134, valor_emprestimo := #135, valor_parcela := #136, prazos := #137, taxa := #138, saldo_quitacao := #139, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #127, contrato := #128, banco := #129, data_inicio := #130, data_desconto := #131, valor_emprestimo := #132, valor_parcela := #133, prazos := #134, taxa := #135, saldo_quitacao := #136, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #140, contrato := #141, banco := #142, data_inicio := #143, data_desconto := #144, valor_emprestimo := #145, valor_parcela := #146, prazos := #147, taxa := #148, saldo_quitacao := #149, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #137, contrato := #138, banco := #139, data_inicio := #140, data_desconto := #141, valor_emprestimo := #142, valor_parcela := #143, prazos := #144, taxa := #145, saldo_quitacao := #146, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #150, contrato := #151, banco := #152, data_inicio := #153, data_desconto := #154, valor_emprestimo := #155, valor_parcela := #156, prazos := #157, taxa := #158, saldo_quitacao := #159, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #147, contrato := #148, banco := #149, data_inicio := #150, data_desconto := #151, valor_emprestimo := #152, valor_parcela := #153, prazos := #154, taxa := #155, saldo_quitacao := #156, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #160, contrato := #161, banco := #162, data_inicio := #163, data_desconto := #164, valor_emprestimo := #165, valor_parcela := #166, prazos := #167, taxa := #168, saldo_quitacao := #169, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #157, contrato := #158, banco := #159, data_inicio := #160, data_desconto := #161, valor_emprestimo := #162, valor_parcela := #163, prazos := #164, taxa := #165, saldo_quitacao := #166, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
union all by name
select
    beneficio,
    case 
        when #50 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #170, contrato := #171, banco := #172, data_inicio := #173, data_desconto := #174, valor_emprestimo := #175, valor_parcela := #176, prazos := #177, taxa := #178, saldo_quitacao := #179, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
        when #47 == '98 - Emprestimo' then struct_pack(tipo_emprestimo := #167, contrato := #168, banco := #169, data_inicio := #170, data_desconto := #171, valor_emprestimo := #172, valor_parcela := #173, prazos := #174, taxa := #175, saldo_quitacao := #176, cpf := #2, nome := #4, data_nascimento := #5, valor_do_beneficio := #10, especie := #13, ddb := #14)
    end contratos
from rr
)
select * from (select beneficio, unnest(contratos) from cte) where contrato is not null
"""

duckdb.sql(__sql__).to_csv(r"./testecrislindo.csv", header= True)






# Fim do script do Eliton-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



# declara "hoje" como variavel e le arquivo preparado do Eliton
hoje = date.today().year
df = pd.read_csv('./testecrislindo.csv', sep = ',', header=0 )

# primeira organizada
df = df[['beneficio', 'contrato', 'banco', 'valor_emprestimo', 'valor_parcela', 'prazos', 'taxa', 'saldo_quitacao', 'cpf', 'nome', 'data_nascimento', 'valor_do_beneficio', 'especie' ]]


# separa 'prazos' em 2, define oque cada campo separado é, cria novas colunas com as informações e exclui oque é inválido
split_prazos = df['prazos'].str.split('/')
df['PRAZO_ORIGINAL'] = pd.to_numeric(split_prazos.str.get(1), errors='coerce')
df['PARCELAS_RESTANTES'] = pd.to_numeric(split_prazos.str.get(0), errors='coerce')
df.dropna(subset=['PRAZO_ORIGINAL', 'PARCELAS_RESTANTES'], inplace=True)


# renomeia as colunas para se adequar ao padrão fatban e cria colunas faltantes
df['PARCELAS_PAGAS'] = df['PRAZO_ORIGINAL'] - df['PARCELAS_RESTANTES']
df['COD_BANCO_DEVEDOR'] = df['banco']
df['NUMERO_CONTRATO'] = df['contrato']
df['MATRICULA'] = df['beneficio']
df['VALOR_BENEFICIO'] = df['valor_do_beneficio']

# variavel 'hoje' usada aqui para descobrir a idade exata de cada pessoa
df['DATA_NASCIMENTO'] = pd.to_datetime(df["data_nascimento"], errors='coerce')
df['ANO_NASCIMENTO'] = df['DATA_NASCIMENTO'].dt.year
df['IDADE'] = hoje - df['ANO_NASCIMENTO']

# renomeia as colunas para se adequar ao padrão fatban e cria colunas faltantes
df['BENEFICIO'] = df['beneficio']
df['CPF'] = df['cpf']
df['NOME'] = df['nome']
df['ESPECIE'] = df['especie']
df['VALOR_DO_BENEFICIO'] = df['valor_do_beneficio']
df['BANCO'] = df['banco']
df['VALOR_EMPRESTIMO'] = df['valor_emprestimo']
df['SALDO_DEVEDOR'] = df['saldo_quitacao']
df['VALOR_PARCELA'] = df['valor_parcela']
df['TAXA'] = ''
df['NOME_BANCO_DEVEDOR'] = ''
df['DDD1'] = ''
df['TEL1'] = ''
df['DDD2'] = ''
df['TEL2'] = ''
df['DDD3'] = ''
df['TEL3'] = ''

# separa 'ESPECIE' em 2, define oque cada campo separado é e cria 2 colunas novas com cada informação
div_especie = df['ESPECIE'].str.split('-')
cod = div_especie.str.get(0)
nome = div_especie.str.get(1)
df['COD_ESPECIE'] = cod
df['NOME_ESPECIE'] = nome

# organização final para se adequar ao padrão fatban
df = df[['BENEFICIO', 'CPF', 'NOME', 'DATA_NASCIMENTO', 'IDADE', 'COD_ESPECIE', 'NOME_ESPECIE', 'VALOR_BENEFICIO', 'VALOR_EMPRESTIMO', 'SALDO_DEVEDOR', 'VALOR_PARCELA', 'PRAZO_ORIGINAL', 'PARCELAS_RESTANTES', 'PARCELAS_PAGAS', 'TAXA', 'COD_BANCO_DEVEDOR', 'NOME_BANCO_DEVEDOR', 'NUMERO_CONTRATO', 'DDD1', 'TEL1', 'DDD2', 'TEL2', 'DDD3', 'TEL3' ]]

# gera o arquivo '.xlsx' final
df.to_excel(r"G:\.shortcut-targets-by-id\1c2g3j1w1xlOHMV8MGJ7gxvzm0LtArH1l\➽ S.I\07. Python\01. Arquivos para o Gabs\EXPERIMENTANDO - 01 teste(1203).xlsx", index=False)

# confirmação do arquivo organizado
print ("Arquivo organizado")
