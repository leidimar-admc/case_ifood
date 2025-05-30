# case_ifood
# 🏍️Case Técnico de Data Analysis - iFood 🍕

Case técnico, que tem como objetivo desenvolver uma solução baseada em dados para
direcionar uma estratégia de cupons como alavanca de crescimento.
Contém tratamento de dados, Notebooks de processamento e análise de dados, instruções de execução e relatorio final com resultados.

---

## 🛠️ Tecnologias e Bibliotecas

- Databricks Community
- Python/PySpark
- Microsoft PowerPoint
- requests
- tqdm
- pandas
  
---

## 🔧 Instalação de dependências:
%pip install requests tqdm pandas

- import os
- import requests
- import tarfile
- from pyspark.sql.functions import lit
- import pandas as pd
- from tqdm import tqdm
- from pyspark.sql.functions import col, to_date, to_timestamp,import avg, sum, countDistinct, stddev
- from pyspark.sql.types import StringType, BooleanType, IntegerType, DoubleType
- import matplotlib.pyplot as plt
- import numpy as np
- import seaborn as sns
- from pyspark.sql import functions as F
- from scipy.stats import ttest_ind
- import matplotlib.ticker as mtick
- import math
- from pyspark.sql.functions import sum as spark_sum

## Como Executar

Processamento e análise realizados em Python/PySpark e desenvolvido no Databricks Community Edition.

Abra Databricks Community Edition: https://community.cloud.databricks.com/

Faça login

Importe o arquivo .py em File>Import

E com cluster já configurado, pressione Run all.

## Análise dos Dados
Utilizado função para baixar os arquivos http diretamente na pasta do Databricks.

Funções de extração e leitura atraves do spark.

Tratamento para alteração dos tipos de dados e resolução de nulos.

análise exploratória dos campos existentes nas bases.

cálculo de métricas com gráficos

Verificação Significância Estatística

Viabilidade Financeira


## Estrutura do Repositório

Notebooks relacionados ao tratamento de dados e analises.

Arquivo do relatorio com resultado em PDF




---




