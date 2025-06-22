# databricks-medallion-ifood
# Projeto de análise de dados de vendas do iFood utilizando Azure Databricks, SQL e a arquitetura Medallion.

## Objetivo

Este projeto realiza uma análise de ponta a ponta sobre um conjunto de dados de vendas da plataforma iFood. O objetivo principal é transformar dados brutos e complexos em uma base de dados limpa, confiável e otimizada para gerar insights de negócio através de uma pipeline de dados e, finalmente, apresentar os resultados em um dashboard interativo.

## Dashboard Interativo

Uma versão interativa do dashboard final, que consolida os principais KPIs e análises deste projeto, pode ser acessada no link abaixo:


## Ferramentas e Tecnologias

* **Cloud:** Microsoft Azure
* **Plataforma de Dados:** Azure Databricks
* **Linguagem Principal:** SQL (Spark SQL)
* **Versionamento de Código:** Git & GitHub

## Arquitetura de Dados

A solução foi desenvolvida seguindo as melhores práticas da **Arquitetura Medallion** para garantir a qualidade e governança dos dados em todas as etapas do processo.

* Camada Bronze (`bronze_ifood`):
    * Recebe os dados brutos de vendas no seu formato original, sem nenhuma alteração.
    * Funciona como um repositório histórico e ponto de partida para a pipeline.

* Camada Silver (`silver_ifood`):
    * Nesta etapa, os dados da camada Bronze são limpos, filtrados e enriquecidos.
    * **Principais transformações:** Conversão de tipos de dados (texto para número, texto para data), renomeação de colunas para um padrão consistente, e tratamento de valores nulos.
    * Esta tabela serve como a nossa "fonte única da verdade" para análises.

* Camada Gold (`gold_ifood`):
    * A camada final, focada em negócio. Os dados da camada Silver são agregados e transformados para responder perguntas específicas.
    * **Principais enriquecimentos:** Criação de colunas categóricas (ex: 'Açai', 'Salgados'), mapeamento de IDs de loja para Estados (UF), e criação de nomes fictícios para as lojas para fins de portfólio.
    * Esta tabela é otimizada para consumo direto por ferramentas de BI como o Power BI.

## Autor

* **LinkedIn:** [https://www.linkedin.com/in/mateus-viana-25a44b198/]
