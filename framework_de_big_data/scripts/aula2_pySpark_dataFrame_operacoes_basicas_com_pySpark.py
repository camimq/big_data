# %% [markdown]
# # Creating and Manipulating in PySpark DataFrame

# %%
#!pip install pyspark
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pysparkdf').getOrCreate()

# %% [markdown]
# # Importing ata

# %%
df = spark.read.csv('/content/cereal.csv', sep=',', inferSchema = True, header = True)

# %% [markdown]
# ## Reading Schema

# %%
df.printSchema()

# %% [markdown]
# ## `select()`

# %%
# Seleciona as colunas que serão exibidas
df.select('name', 'mfr', 'rating').show()

# %% [markdown]
# ## `withColumn()`

# %%
# renomeia a coluna Colories para clories
df.withColumn('Calories', df['calories'].cast('Integer')).printSchema()

# %% [markdown]
# ## `groupBy()`

# %%
df.groupBy('name', 'calories').count().show()

# %% [markdown]
# ## `orderBy()`

# %%
df.orderBy('protein').show()

# %% [markdown]
# ## Case When

# %%
from pyspark.sql.functions import when

# %%
df.select('name', df.vitamins, when(df.vitamins >= '25', 'rich in vitamins')).show(50)

# %% [markdown]
# ## filter()

# %%
df.filter(df.calories == '100').show()

# %% [markdown]
# ## `isNull()` / `isNotNull()`

# %%
from pyspark.sql.functions import *

# %%
df.filter(df.name.isNotNull()).show()

# %%
df.filter(df.name.isNull()).show()

# %%



