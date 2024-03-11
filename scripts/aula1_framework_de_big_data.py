# %% [markdown]
# ## PySpark - Instalando a biblioteca PySpark

# %% [markdown]
# - `pip install pyspark` para instalar a biblioteca mais recente do PySpark no projeto.
# - `pip install findspark` para instalar a biblioteca mais recente do FindSpak

# %%
import findspark # importa a biblioteca para o notebook
findspark.init() #inicializa a biblioteca
from pyspark.sql import SparkSession # essa função inicializa a sessão de uso do PySpark dentro do notebook

spark = SparkSession.builder.master('local[*]').getOrCreate()

# %%
# importa os dados para o projeto
df = spark.sql('''select 'Sucesso total, estamos online!' as hello''')
df.show()

# %%
# instalação  das principais bibliotecas do PySpark
# referência para funções específicas para tratamento de dados
from pyspark.sql import Row, DataFrame
from pyspark.sql.types import StringType, StructType, StructField, IntegerType # 
from pyspark.sql.functions import col, expr, lit, substring, concat, concat_ws, when, coalesce
from pyspark.sql import functions as F  # for more sql functions
from functools import reduce
import pandas as pd

# %% [markdown]
# # Data Manipulation using Spark

# %%
# importa base de dados banklist
# inferSchema - força a inferência do schema quando o arquivo for importado
# header - informa o spark que há um cabeçalho no arquivo que está sendo importado

url_github = 'https://raw.githubusercontent.com/camimq/big_data/main/bases/banklist.csv'

pd_df = pd.read_csv(url_github)
df_banklist = spark.createDataFrame(pd_df)
# df_banklist = spark.read.csv(r'file_path_here', sep=',', inferSchema=True, header=True)

print('df.count: ', df_banklist.count())
print('df.col ct: ', len(df_banklist.columns))
print('df.columns: ', df_banklist.columns)

# %% [markdown]
# ### Workaround para leitura de arquivo
# 
# Para que eu não exponha o caminho de arquivo no meu computador, faço o _upload_ das bases no GitHub. Contudo, com o PySpark, por alguma razão, não consigo puxar o arquivo como fiz até o momento. Pesquisando na internet, descobri o _workaround_ acima no [StackOverflow](https://stackoverflow.com/questions/71251538/use-csv-from-github-in-pyspark) onde, basicamente, declaro uma variável com a URL onde o arquivo está e, posteriormente, crio outra variável de leitura do `csv` no pandas que, por último é utilizada para função `create` do Spark.

# %% [markdown]
# # Using SQL in PySpark

# %%

df_banklist.createOrReplaceTempView("banklist")

df_check = spark.sql('''select `Bank Name`, City, `Closing Date` from banklist''') # cria o dataframe utilizando um query SQL
df_check.show()
# df_check.show(4, truncate=False)

# %% [markdown]
# # DataFrame Basic Operations

# %%
df_banklist.describe().show()

# %%
# mostra a coluna Cidade e Estado
df_banklist.describe('City', 'ST').show()

# %% [markdown]
# # Count, Columns and Schema

# %%
print('Total de linhas', df_banklist.count())
print('Total de colunas:', len(df_banklist.columns))
print('Colunas:', df_banklist.columns)
print('Tipo de dados:', df_banklist.dtypes)
print('Schema:', df_banklist.schema)

# %%
df_banklist.printSchema()

# %% [markdown]
# # Remove Duplicates

# %%
df_banklist = df_banklist.dropDuplicates()
print('df.count: ', df_banklist.count())
print('df.columns: ', df_banklist.columns)

# %% [markdown]
# Como não há mudança na contagem de linhas, é possível confirmar que não há dado duplicado.

# %% [markdown]
# # Select Specific columns

# %%
df2 = df_banklist.select(*['Bank Name', 'City'])
df2.show(2)

# %% [markdown]
# # Select Multiple Columns

# %%
# seleciona todas as colunas, exceto CERT e ST
col_1 = list(set(df_banklist.columns) - {'CERT', 'ST'})
df2 = df_banklist.select(*col_1)
df2.show(2) 

# %% [markdown]
# # Rename Columns

# %%
# renomeando as colunas dentro do dataset
df2 = df_banklist \
    .withColumnRenamed('Bank Name', 'bank_name')\
    .withColumnRenamed('Acquiring Institution', 'acq_institution')\
    .withColumnRenamed('Closing Date', 'closing_date')\
    .withColumnRenamed('ST', 'state')\
    .withColumnRenamed('CERT', 'cert')\
    .withColumnRenamed('City', 'city')\
    .withColumnRenamed('Updated Date', 'updated_date')#\
df2.show()

# %% [markdown]
# # Add Columns

# %%
# insere uma coluna ST, usando como referência o código da coluna state
df2 = df_banklist.withColumn('state', col('ST'))
df2.show()

# %% [markdown]
# # Add constant column

# %% [markdown]
# Uma coluna constante é uma coluna com valor fixo (o mesmo para todas as linhas).

# %%
df2=df_banklist.withColumn('country', lit('US'))
df2.show()

# %% [markdown]
# # Drop Columns

# %%
df2 = df_banklist.drop('CERT')
df2.show()

# %% [markdown]
# # Drop Multiple Columns

# %%
df2 = df_banklist.drop(*['CERT', 'ST', 'Closing Date'])
df2.show()

# %%
df2 = reduce(DataFrame.drop, ['CERT', 'ST'], df_banklist)
df2.show()

# %% [markdown]
# # Filter Data

# %% [markdown]
# Filtra valores dentro do dataset.

# %%
# Equal to values
df2 = df_banklist.where(df_banklist['ST'] == 'NE')

# Between values
df3 = df_banklist.where(df_banklist['CERT'].between('1000', '2000'))

# Is inside multiple values
df4 = df_banklist.where(df_banklist['ST'].isin('NE', 'IL'))

print('df_banklist.count :', df_banklist.count())
print('df2.count :', df2.count())
print('df3.count :', df3.count())
print('df4.count :', df4.count())

# %% [markdown]
# # Filter data using logical operator

# %%
df2 = df_banklist.where((df_banklist['ST'] == 'NE') & (df_banklist['City'] == 'Ericson'))
df2.show(3)

# %% [markdown]
# # Replace values in DataFrame

# %%
# Pre replace
df_banklist.show(10)

# Post replace
print('Replace 7 in the above dataframe with 17 at all instances')
df_banklist.na.replace(7,17).show(10)


