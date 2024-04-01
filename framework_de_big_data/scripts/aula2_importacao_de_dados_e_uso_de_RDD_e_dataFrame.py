# %% [markdown]
# # Importação de dados e Uso de RDD e DataFrame

# %% [markdown]
# ## Configuração de biblioteca do PysPark

# %% [markdown]
# ## Criando a sessão do SparkContext e SparkSession

# %%
from pyspark import SparkContext
from pyspark.sql import SparkSession

# %%
sc = SparkContext.getOrCreate()

# %%
spark = SparkSession.builder.appName('PySpark DataFrame From RDD').getOrCreate()

# %% [markdown]
# ## Create PySpark Dataframe from an Existing RDD

# %%
rdd = sc.parallelize([('C',85,76,87,91), ('B',85,76,87,91), ("A", 85,78,96,92), ("A", 92,76,89,96)], 4)

# %%
print(type(rdd))

# %%
sub = ['id_person','value_1','value_2','value_3','value_4']

# %%
marks_df = spark.createDataFrame(rdd, schema=sub)

# %%
print(type(marks_df))

# %%
marks_df.printSchema()

# %%
marks_df.show()


