# Databricks notebook source
# MAGIC %sql
# MAGIC -- Célula 1: Ingestão de Dados - Criação da Tabela Bronze
# MAGIC -- Objetivo: Carregar os dados brutos de um arquivo de origem para dentro do Databricks.
# MAGIC -- Esta tabela, 'bronze_ifood', representa a primeira etapa da arquitetura Medallion,
# MAGIC -- servindo como uma cópia fiel e imutável dos dados originais para garantir a rastreabilidade.
# MAGIC SELECT * FROM `bronze_ifood` LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Célula 2: Preparação dos Dados - Criação da Tabela Silver
# MAGIC -- Objetivo: Ler os dados brutos da tabela 'bronze_ifood', realizar a limpeza,
# MAGIC -- conversão de tipos e renomear colunas para criar uma tabela intermediária
# MAGIC -- confiável e pronta para análise 
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_ifood
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC   -- === SEÇÃO DE IDENTIFICADORES ===
# MAGIC
# MAGIC   CAST(`N DO PEDIDO` AS BIGINT) AS id_pedido_loja, -- Converte a coluna de ID do pedido da loja para BIGINT
# MAGIC
# MAGIC   `RESTAURANTE` AS nome_restaurante,  -- Mantém o nome original do restaurante
# MAGIC
# MAGIC   CAST(`ID DO RESTAURANTE` AS BIGINT) AS id_restaurante, -- Converte o ID do restaurante para BIGINT
# MAGIC
# MAGIC   -- === SEÇÃO DE DATAS ===
# MAGIC
# MAGIC   -- Converte a coluna de data, que vem como um número do Excel, para um formato de data real.
# MAGIC   DATE_ADD(DATE '1899-12-30', CAST(`DATA` AS INT)) AS data_convertida,
# MAGIC
# MAGIC   -- === SEÇÃO DE VALORES MONETÁRIOS ===
# MAGIC   -- Convertendo todas as colunas de valores para o tipo DECIMAL(10, 2)
# MAGIC   -- A função REPLACE troca a vírgula do decimal brasileiro (ex: '10,50') 
# MAGIC   -- por ponto ('10.50'), que é o padrão do SQL.
# MAGIC
# MAGIC   CAST(REPLACE(`TAXA DE ENTREGA`, ',', '.') AS DECIMAL(10, 2)) AS taxa_entrega,
# MAGIC
# MAGIC   CAST(REPLACE(`VALOR DOS ITENS`, ',', '.') AS DECIMAL(10, 2)) AS total_pedido,
# MAGIC
# MAGIC   CAST(REPLACE(`INCENTIVO PROMOCIONAL DO IFOOD`, ',', '.') AS DECIMAL(10, 2)) AS incentivo_ifood,
# MAGIC
# MAGIC   CAST(REPLACE(`INCENTIVO PROMOCIONAL DA LOJA`, ',', '.') AS DECIMAL(10, 2)) AS incentivo_loja,
# MAGIC
# MAGIC   -- === SEÇÃO DE TEXTOS E CATEGORIAS ===
# MAGIC   -- Limpando e padronizando colunas de texto
# MAGIC   -- COALESCE preenche valores nulos com um texto padrão para evitar erros na análise.
# MAGIC
# MAGIC   COALESCE(`PAGAMENTO`, 'Não Informado') AS forma_pagamento,
# MAGIC
# MAGIC   COALESCE(`MOTIVO DO CANCELAMENTO`, 'Sem Cancelamento') AS motivo_cancelamento,
# MAGIC
# MAGIC   `TIPO DE ENTREGA DOS PEDIDOS` AS tipo_entrega,
# MAGIC
# MAGIC   `CANAL DE VENDAS` AS canal_vendas
# MAGIC
# MAGIC FROM bronze_ifood; -- -- Tabela de origem com os dados brutos.
# MAGIC
# MAGIC SELECT * FROM `hive_metastore`.`default`.`silver_ifood` LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Célula 3: Análise e Transformação dos Dados - Criação da Tabela Gold
# MAGIC -- Objetivo: A partir da tabela Silver, aplicar as regras de negócio para enriquecer os dados,
# MAGIC -- criando uma tabela final (Gold) pronta para ser consumida por dashboards e relatórios.
# MAGIC -- A CTE 'teste_filtro' realiza a primeira camada de transformação e agregação.
# MAGIC
# MAGIC With teste_filtro AS (SELECT 
# MAGIC   nome_restaurante AS nome_original,
# MAGIC
# MAGIC   forma_pagamento,
# MAGIC
# MAGIC   id_pedido_loja, --  ID do pedido individual
# MAGIC
# MAGIC   data_convertida AS data_venda,
# MAGIC
# MAGIC   --  Lógica para Renomear as lojas para nomes fictícios usando o CASE.
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato -  Shopping Eldorado'     THEN 'Doce Vórtice  -  Açai -  Shopping Eldorado'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Amélia'   THEN 'Doce Vórtice  -  Açai - Amélia'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Beira Mar'   THEN 'Doce Vórtice  -  Açai - Beira Mar'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Boa Viagem'   THEN 'Doce Vórtice  -  Açai - Boa Viagem'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Boulevard Shopping'   THEN 'Doce Vórtice  -  Açai - Boulevard Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Caruaru'  THEN 'Doce Vórtice  -  Açai - Caruaru'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Casa Forte' THEN 'Doce Vórtice  -  Açai - Casa Forte'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Cohama'  THEN 'Doce Vórtice  -  Açai - Cohama'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Design mall'  THEN 'Doce Vórtice  -  Açai - Design mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Dirceu' THEN 'Doce Vórtice  -  Açai - Dirceu'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Dom Severino' THEN 'Doce Vórtice  -  Açai - Dom Severino'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Eusebio' THEN 'Doce Vórtice  -  Açai - Eusebio'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Fátima' THEN 'Doce Vórtice  -  Açai - Fátima'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Flores' THEN 'Doce Vórtice  -  Açai - Flores'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Horto Florestal' THEN 'Doce Vórtice  -  Açai - Horto Florestal'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Jardins' THEN 'Doce Vórtice  -  Açai - Jardins'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Kennedy' THEN 'Doce Vórtice  -  Açai - Kennedy'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Riomar Kennedy ' THEN 'Doce Vórtice  -  Açai - Riomar Kennedy'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Lagoa Mall'  THEN 'Doce Vórtice  -  Açai - Lagoa Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Lagoa Seca'  THEN 'Doce Vórtice  -  Açai - Lagoa Seca'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Manaíra Shopping'  THEN 'Doce Vórtice  -  Açai - Manaíra Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Meireles' THEN 'Doce Vórtice  -  Açai - Meireles'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Midway Mall'  THEN 'Doce Vórtice  -  Açai - Midway Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Morumbi Shopping'  THEN 'Doce Vórtice  -  Açai - Morumbi Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Natal Shopping'  THEN 'Doce Vórtice  -  Açai - Natal Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Oitava Mall'  THEN 'Doce Vórtice  -  Açai - Oitava Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Origens (pb)'  THEN 'Doce Vórtice  -  Açai - Origens (pb)'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Parangaba' THEN 'Doce Vórtice  -  Açai - Parangaba'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Parque Shopping' THEN 'Doce Vórtice  -  Açai - Parque Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Pátio Paulista' THEN 'Doce Vórtice  -  Açai - Pátio Paulista'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Pituba'  THEN 'Doce Vórtice  -  Açai - Pituba'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Ponta Negra' THEN 'Doce Vórtice  -  Açai - Ponta Negra'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Riomar Aracaju'  THEN 'Doce Vórtice  -  Açai - Riomar Aracaju'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Riomar Fortaleza' THEN 'Doce Vórtice  -  Açai - Riomar Fortaleza'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Riverside'  THEN 'Doce Vórtice  -  Açai - Riverside'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Salvador Shopping'  THEN 'Doce Vórtice  -  Açai - Salvador Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - São Luís Shopping'  THEN 'Doce Vórtice  -  Açai - São Luís Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopp. Riomar Recife' THEN 'Doce Vórtice  -  Açai - Shopp. Riomar Recife'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping da Bahia' THEN 'Doce Vórtice  -  Açai - Shopping da Bahia'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping Grão Pará'  THEN 'Doce Vórtice  -  Açai - Shopping Grão Pará'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato- Shopping Guararapes'   THEN 'Doce Vórtice  -  Açai - Shopping Guararapes'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping Iguatemi'  THEN 'Doce Vórtice  -  Açai - Shopping Iguatemi'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato  - Shopping Paralela'  THEN 'Doce Vórtice  -  Açai - Shopping Paralela'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping Recife'  THEN 'Doce Vórtice  -  Açai - Shopping Recife'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping Rio Poty' THEN 'Doce Vórtice  -  Açai - Shopping Rio Poty'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping Tacaruna' THEN 'Doce Vórtice  -  Açai - Shopping Tacaruna'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Sobral' THEN 'Doce Vórtice  -  Açai - Sobral'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Sul'   THEN 'Doce Vórtice  -  Açai - Sul'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Umarizal'   THEN 'Doce Vórtice  -  Açai - Umarizal'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Varjota'  THEN 'Doce Vórtice  -  Açai - Varjota'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Vilas do Atlântico'   THEN 'Doce Vórtice  -  Açai - Vilas do Atlântico'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Amélia'  THEN 'Doce Vórtice  -  Salgados  -  Amélia'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Boa Viagem'   THEN 'Doce Vórtice  -  Salgados  -  Boa Viagem'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Casa Forte'  THEN 'Doce Vórtice  -  Salgados  -  Casa Forte'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Cohama'   THEN 'Doce Vórtice  -  Salgados  -  Cohama'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Pituba'   THEN 'Doce Vórtice  -  Salgados  -  Pituba'
# MAGIC
# MAGIC     WHEN LOWER(TRIM(nome_restaurante)) = 'san paolo - cookie&brownie - design mall' THEN 'Doce Vórtice  -  Salgados  -  Design Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Dirceu'   THEN 'Doce Vórtice  -  Salgados  -  Dirceu'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Dom Severino'   THEN 'Doce Vórtice  -  Salgados  -  Dom Severino'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Eusebio'   THEN 'Doce Vórtice  -  Salgados  -  Eusebio'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Fátima'   THEN 'Doce Vórtice  -  Salgados  -  Fátima'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Grão Pará'   THEN 'Doce Vórtice  -  Salgados  -  Grão Pará'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Haddock Lobo'   THEN 'Doce Vórtice  -  Salgados  -  Haddock Lobo'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Horto Florestal'   THEN 'Doce Vórtice  -  Salgados  -  Horto Florestal'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Lagoa Mall'   THEN 'Doce Vórtice  -  Salgados  -  Lagoa Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie&brownie - Lagoa Seca'  THEN 'Doce Vórtice  -  Salgados  -  Lagoa Seca'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Meireles'   THEN 'Doce Vórtice  -  Salgados  -  Meireles'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Morumbi'   THEN 'Doce Vórtice  -  Salgados  -  Morumbi'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Natal Shopping'  THEN 'Doce Vórtice  -  Salgados  -  Natal Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Oitava Mall'   THEN 'Doce Vórtice  -  Salgados  -  Oitava Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Origens' THEN 'Doce Vórtice  -  Salgados  -  Origens'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Paralela'  THEN 'Doce Vórtice  -  Salgados  -  Paralela'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Parangaba'  THEN 'Doce Vórtice  -  Salgados  -  Parangaba'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Patio Paulistas'  THEN 'Doce Vórtice  -  Salgados  -  Patio Paulistas'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie -  Pituba' THEN 'Doce Vórtice  -  Salgados  -  Pituba'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - São Luis'   THEN 'Doce Vórtice  -  Salgados  -  São Luis'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Sul'  THEN 'Doce Vórtice  -  Salgados  -  Sul'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Umarizal'  THEN 'Doce Vórtice  -  Salgados  -  Umarizal'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Vilas do Atlantico'   THEN 'Doce Vórtice  -  Salgados  -  Vilas do Atlantico'
# MAGIC
# MAGIC     ELSE nome_restaurante
# MAGIC
# MAGIC   END AS novo_nome_restaurante,
# MAGIC
# MAGIC   (id_restaurante),
# MAGIC
# MAGIC   -- Lógica para mapear o ID do restaurante para um Estado (UF) usando o CASE.
# MAGIC
# MAGIC   CASE
# MAGIC
# MAGIC     WHEN id_restaurante IN (2450485) THEN 'AM'
# MAGIC
# MAGIC     WHEN id_restaurante IN (53018, 1317158, 265251, 2293788, 1567771, 2415393, 55565, 1057605, 1244269, 279255, 90398, 87751, 
# MAGIC     2422733, 2422769, 2422804, 2443005, 2422785)   THEN 'CE'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2393924) THEN 'CE2'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2350391, 
# MAGIC     2493699) THEN 'CE3'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2435778, 294437, 1050423 , 278052, 65761, 294436, 
# MAGIC     2493761, 2493743,2493739, 2493750 ) THEN 'BA'
# MAGIC
# MAGIC     WHEN id_restaurante IN (298595, 298596,
# MAGIC     177551, 2496196) THEN 'MA'
# MAGIC
# MAGIC     WHEN id_restaurante IN (1057604, 302167, 256794,
# MAGIC     2496377, 2452500 ) THEN 'PA'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2384791, 1644202, 686435,
# MAGIC     2496214, 2496220 ) THEN 'PB'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2435142, 2730333, 142174, 64703, 203481, 1644303, 57495, 93534,
# MAGIC     2493716,2493706, 2493709 ) THEN 'PE'
# MAGIC
# MAGIC     WHEN id_restaurante IN (200811, 546079, 182153, 211341,
# MAGIC     2493688, 2493674 ) THEN 'PI'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2436995, 638786, 2078267, 559465,
# MAGIC     2496245, 2496265, 2496278 ) THEN 'RN'
# MAGIC
# MAGIC     WHEN id_restaurante IN (295855,579477, 90997, 202628,
# MAGIC     2496355, 2496372, 2496291) THEN 'SP'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2795981) THEN 'SE'
# MAGIC
# MAGIC     -- Esta é uma boa prática para pegar códigos que não foram mapeados
# MAGIC
# MAGIC     ELSE 'Estado não identificado'
# MAGIC
# MAGIC   END AS estado,
# MAGIC
# MAGIC -- Colunas originais de valor, sem agregação aqui. A agregação virá depois se necessário.
# MAGIC
# MAGIC   sum(total_pedido) as receita,
# MAGIC
# MAGIC   sum(incentivo_loja) as desconto,
# MAGIC
# MAGIC   (receita - desconto) as lucro,
# MAGIC
# MAGIC   TRIM(forma_pagamento),
# MAGIC
# MAGIC   motivo_cancelamento,
# MAGIC
# MAGIC   TRIM(tipo_entrega),
# MAGIC
# MAGIC   TRIM(canal_vendas)
# MAGIC
# MAGIC   From silver_ifood
# MAGIC
# MAGIC   WHERE nome_restaurante LIKE '%San Paolo%'
# MAGIC
# MAGIC   GROUP BY nome_restaurante, id_restaurante, forma_pagamento, motivo_cancelamento, tipo_entrega, canal_vendas, data_venda, id_pedido_loja
# MAGIC ),
# MAGIC
# MAGIC -- A vírgula depois do parêntese é a chave para encadear
# MAGIC -- A CTE 'dados_renomeados' realiza a segunda camada de transformação.
# MAGIC -- Ela usa o resultado da CTE anterior para criar a coluna de categoria.
# MAGIC
# MAGIC dados_renomeados AS (
# MAGIC     SELECT
# MAGIC       id_pedido_loja,
# MAGIC
# MAGIC       novo_nome_restaurante,
# MAGIC
# MAGIC       id_restaurante,
# MAGIC
# MAGIC       estado,
# MAGIC
# MAGIC       data_venda, 
# MAGIC
# MAGIC       receita,
# MAGIC
# MAGIC       desconto,
# MAGIC
# MAGIC       lucro,
# MAGIC
# MAGIC       CASE
# MAGIC         WHEN (novo_nome_restaurante) LIKE '%Açai%' THEN 'Açai'
# MAGIC
# MAGIC         WHEN (novo_nome_restaurante) LIKE '%Salgados%' THEN 'Salgados'
# MAGIC
# MAGIC         ELSE 'Não Categoria'
# MAGIC
# MAGIC       END AS categoria,
# MAGIC
# MAGIC       motivo_cancelamento
# MAGIC
# MAGIC -- Lógica para criar a categoria do produto ('Açai' ou 'Salgados')
# MAGIC -- baseada no novo nome fictício do restaurante.
# MAGIC
# MAGIC     FROM teste_filtro -- Usa a CTE anterior como fonte de dados.
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM
# MAGIC   dados_renomeados;
# MAGIC  
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Célula Final: Materialização da Tabela Gold
# MAGIC -- Objetivo: Executar todo o pipeline de transformação e salvar o resultado
# MAGIC -- em uma tabela de análise final (camada Gold). Esta tabela será a fonte
# MAGIC -- de dados para relatórios, dashboards e outras análises.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold_ifood
# MAGIC AS
# MAGIC With teste_filtro AS (SELECT
# MAGIC
# MAGIC   nome_restaurante AS nome_original,
# MAGIC
# MAGIC   forma_pagamento,
# MAGIC
# MAGIC   id_pedido_loja,
# MAGIC
# MAGIC   data_convertida AS data_venda,
# MAGIC
# MAGIC   CASE
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato -  Shopping Eldorado'     THEN 'Doce Vórtice  -  Açai -  Shopping Eldorado'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Amélia'   THEN 'Doce Vórtice  -  Açai - Amélia'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Beira Mar'   THEN 'Doce Vórtice  -  Açai - Beira Mar'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Boa Viagem'   THEN 'Doce Vórtice  -  Açai - Boa Viagem'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Boulevard Shopping'   THEN 'Doce Vórtice  -  Açai - Boulevard Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Caruaru'  THEN 'Doce Vórtice  -  Açai - Caruaru'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Casa Forte' THEN 'Doce Vórtice  -  Açai - Casa Forte'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Cohama'  THEN 'Doce Vórtice  -  Açai - Cohama'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Design mall'  THEN 'Doce Vórtice  -  Açai - Design mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Dirceu' THEN 'Doce Vórtice  -  Açai - Dirceu'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Dom Severino' THEN 'Doce Vórtice  -  Açai - Dom Severino'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Eusebio' THEN 'Doce Vórtice  -  Açai - Eusebio'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Fátima' THEN 'Doce Vórtice  -  Açai - Fátima'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Flores' THEN 'Doce Vórtice  -  Açai - Flores'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Horto Florestal' THEN 'Doce Vórtice  -  Açai - Horto Florestal'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Jardins' THEN 'Doce Vórtice  -  Açai - Jardins'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Kennedy' THEN 'Doce Vórtice  -  Açai - Kennedy'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Riomar Kennedy ' THEN 'Doce Vórtice  -  Açai - Riomar Kennedy'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Lagoa Mall'  THEN 'Doce Vórtice  -  Açai - Lagoa Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Lagoa Seca'  THEN 'Doce Vórtice  -  Açai - Lagoa Seca'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Manaíra Shopping'  THEN 'Doce Vórtice  -  Açai - Manaíra Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Meireles' THEN 'Doce Vórtice  -  Açai - Meireles'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Midway Mall'  THEN 'Doce Vórtice  -  Açai - Midway Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Morumbi Shopping'  THEN 'Doce Vórtice  -  Açai - Morumbi Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Natal Shopping'  THEN 'Doce Vórtice  -  Açai - Natal Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Oitava Mall'  THEN 'Doce Vórtice  -  Açai - Oitava Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Origens (pb)'  THEN 'Doce Vórtice  -  Açai - Origens (pb)'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Parangaba' THEN 'Doce Vórtice  -  Açai - Parangaba'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Parque Shopping' THEN 'Doce Vórtice  -  Açai - Parque Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Pátio Paulista' THEN 'Doce Vórtice  -  Açai - Pátio Paulista'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Pituba'  THEN 'Doce Vórtice  -  Açai - Pituba'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Ponta Negra' THEN 'Doce Vórtice  -  Açai - Ponta Negra'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Riomar Aracaju'  THEN 'Doce Vórtice  -  Açai - Riomar Aracaju'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Riomar Fortaleza' THEN 'Doce Vórtice  -  Açai - Riomar Fortaleza'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Riverside'  THEN 'Doce Vórtice  -  Açai - Riverside'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Salvador Shopping'  THEN 'Doce Vórtice  -  Açai - Salvador Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - São Luís Shopping'  THEN 'Doce Vórtice  -  Açai - São Luís Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopp. Riomar Recife' THEN 'Doce Vórtice  -  Açai - Shopp. Riomar Recife'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping da Bahia' THEN 'Doce Vórtice  -  Açai - Shopping da Bahia'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping Grão Pará'  THEN 'Doce Vórtice  -  Açai - Shopping Grão Pará'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato- Shopping Guararapes'   THEN 'Doce Vórtice  -  Açai - Shopping Guararapes'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping Iguatemi'  THEN 'Doce Vórtice  -  Açai - Shopping Iguatemi'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato  - Shopping Paralela'  THEN 'Doce Vórtice  -  Açai - Shopping Paralela'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping Recife'  THEN 'Doce Vórtice  -  Açai - Shopping Recife'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping Rio Poty' THEN 'Doce Vórtice  -  Açai - Shopping Rio Poty'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Shopping Tacaruna' THEN 'Doce Vórtice  -  Açai - Shopping Tacaruna'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Sobral' THEN 'Doce Vórtice  -  Açai - Sobral'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Sul'   THEN 'Doce Vórtice  -  Açai - Sul'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Umarizal'   THEN 'Doce Vórtice  -  Açai - Umarizal'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Varjota'  THEN 'Doce Vórtice  -  Açai - Varjota'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo Gelato - Vilas do Atlântico'   THEN 'Doce Vórtice  -  Açai - Vilas do Atlântico'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Amélia'  THEN 'Doce Vórtice  -  Salgados  -  Amélia'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Boa Viagem'   THEN 'Doce Vórtice  -  Salgados  -  Boa Viagem'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Casa Forte'  THEN 'Doce Vórtice  -  Salgados  -  Casa Forte'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Cohama'   THEN 'Doce Vórtice  -  Salgados  -  Cohama'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Pituba'   THEN 'Doce Vórtice  -  Salgados  -  Pituba'
# MAGIC
# MAGIC     WHEN LOWER(TRIM(nome_restaurante)) = 'san paolo - cookie&brownie - design mall' THEN 'Doce Vórtice  -  Salgados  -  Design Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Dirceu'   THEN 'Doce Vórtice  -  Salgados  -  Dirceu'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Dom Severino'   THEN 'Doce Vórtice  -  Salgados  -  Dom Severino'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Eusebio'   THEN 'Doce Vórtice  -  Salgados  -  Eusebio'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Fátima'   THEN 'Doce Vórtice  -  Salgados  -  Fátima'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Grão Pará'   THEN 'Doce Vórtice  -  Salgados  -  Grão Pará'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Haddock Lobo'   THEN 'Doce Vórtice  -  Salgados  -  Haddock Lobo'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Horto Florestal'   THEN 'Doce Vórtice  -  Salgados  -  Horto Florestal'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Lagoa Mall'   THEN 'Doce Vórtice  -  Salgados  -  Lagoa Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie&brownie - Lagoa Seca'  THEN 'Doce Vórtice  -  Salgados  -  Lagoa Seca'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Meireles'   THEN 'Doce Vórtice  -  Salgados  -  Meireles'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Morumbi'   THEN 'Doce Vórtice  -  Salgados  -  Morumbi'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Natal Shopping'  THEN 'Doce Vórtice  -  Salgados  -  Natal Shopping'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Oitava Mall'   THEN 'Doce Vórtice  -  Salgados  -  Oitava Mall'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Origens' THEN 'Doce Vórtice  -  Salgados  -  Origens'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Paralela'  THEN 'Doce Vórtice  -  Salgados  -  Paralela'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Parangaba'  THEN 'Doce Vórtice  -  Salgados  -  Parangaba'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Patio Paulistas'  THEN 'Doce Vórtice  -  Salgados  -  Patio Paulistas'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie -  Pituba' THEN 'Doce Vórtice  -  Salgados  -  Pituba'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - São Luis'   THEN 'Doce Vórtice  -  Salgados  -  São Luis'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Sul'  THEN 'Doce Vórtice  -  Salgados  -  Sul'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Umarizal'  THEN 'Doce Vórtice  -  Salgados  -  Umarizal'
# MAGIC
# MAGIC     WHEN nome_restaurante = 'San Paolo - Cookie e Brownie - Vilas do Atlantico'   THEN 'Doce Vórtice  -  Salgados  -  Vilas do Atlantico'
# MAGIC
# MAGIC     ELSE nome_restaurante
# MAGIC
# MAGIC   END AS novo_nome_restaurante,
# MAGIC
# MAGIC   (id_restaurante),
# MAGIC
# MAGIC   CASE
# MAGIC
# MAGIC     WHEN id_restaurante IN (2450485) THEN 'AM'
# MAGIC
# MAGIC     WHEN id_restaurante IN (53018, 1317158, 265251, 2293788, 1567771, 2415393, 55565, 1057605, 1244269, 279255, 90398, 87751,
# MAGIC
# MAGIC     2422733, 2422769, 2422804, 2443005, 2422785)   THEN 'CE'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2393924) THEN 'CE2'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2350391,
# MAGIC
# MAGIC     2493699) THEN 'CE3'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2435778, 294437, 1050423 , 278052, 65761, 294436,
# MAGIC
# MAGIC     2493761, 2493743,2493739, 2493750 ) THEN 'BA'
# MAGIC
# MAGIC     WHEN id_restaurante IN (298595, 298596,
# MAGIC
# MAGIC     177551, 2496196) THEN 'MA'
# MAGIC
# MAGIC     WHEN id_restaurante IN (1057604, 302167, 256794,
# MAGIC
# MAGIC     2496377, 2452500 ) THEN 'PA'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2384791, 1644202, 686435,
# MAGIC
# MAGIC     2496214, 2496220 ) THEN 'PB'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2435142, 2730333, 142174, 64703, 203481, 1644303, 57495, 93534,
# MAGIC
# MAGIC     2493716,2493706, 2493709 ) THEN 'PE'
# MAGIC
# MAGIC     WHEN id_restaurante IN (200811, 546079, 182153, 211341,
# MAGIC
# MAGIC     2493688, 2493674 ) THEN 'PI'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2436995, 638786, 2078267, 559465,
# MAGIC
# MAGIC     2496245, 2496265, 2496278 ) THEN 'RN'
# MAGIC
# MAGIC     WHEN id_restaurante IN (295855,579477, 90997, 202628,
# MAGIC
# MAGIC     2496355, 2496372, 2496291) THEN 'SP'
# MAGIC
# MAGIC     WHEN id_restaurante IN (2795981) THEN 'SE'
# MAGIC
# MAGIC     -- Esta é uma boa prática para pegar códigos que não foram mapeados
# MAGIC
# MAGIC     ELSE 'Estado não identificado'
# MAGIC
# MAGIC   END AS estado,
# MAGIC
# MAGIC   sum(total_pedido) as receita,
# MAGIC
# MAGIC   sum(incentivo_loja) as desconto,
# MAGIC
# MAGIC   (receita - desconto) as lucro,
# MAGIC
# MAGIC   TRIM(forma_pagamento),
# MAGIC
# MAGIC   motivo_cancelamento,
# MAGIC
# MAGIC   TRIM(tipo_entrega),
# MAGIC
# MAGIC   TRIM(canal_vendas)
# MAGIC
# MAGIC   From silver_ifood
# MAGIC
# MAGIC   WHERE nome_restaurante LIKE '%San Paolo%'
# MAGIC
# MAGIC   GROUP BY nome_restaurante, id_restaurante, forma_pagamento, motivo_cancelamento, tipo_entrega, canal_vendas, data_venda, id_pedido_loja
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC -- A vírgula depois do parêntese é a chave para encadear
# MAGIC
# MAGIC
# MAGIC
# MAGIC   -- PASSO 1: Categorização dos dados
# MAGIC dados_renomeados AS (
# MAGIC
# MAGIC     SELECT
# MAGIC
# MAGIC       id_pedido_loja,
# MAGIC
# MAGIC       novo_nome_restaurante,
# MAGIC
# MAGIC       id_restaurante,
# MAGIC
# MAGIC       estado,
# MAGIC
# MAGIC       data_venda,
# MAGIC
# MAGIC       receita,
# MAGIC
# MAGIC       desconto,
# MAGIC
# MAGIC       lucro,
# MAGIC
# MAGIC       CASE
# MAGIC
# MAGIC         WHEN (novo_nome_restaurante) LIKE '%Açai%' THEN 'Açai'
# MAGIC
# MAGIC         WHEN (novo_nome_restaurante) LIKE '%Salgados%' THEN 'Salgados'
# MAGIC
# MAGIC         ELSE 'Não Categoria'
# MAGIC
# MAGIC       END AS categoria,
# MAGIC
# MAGIC       motivo_cancelamento
# MAGIC
# MAGIC     FROM
# MAGIC
# MAGIC       teste_filtro
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM
# MAGIC   dados_renomeados;
# MAGIC -- Célula Final: Materialização da Tabela Gold
# MAGIC -- Objetivo: Executar todo o pipeline de transformação e salvar o resultado
# MAGIC -- em uma tabela de análise final (camada Gold). Esta tabela será a fonte
# MAGIC -- de dados para relatórios, dashboards e outras análises.
# MAGIC
# MAGIC SELECT * from gold_ifood;
