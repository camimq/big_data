# %% [markdown]
# # Instalação do PySpark

# %%#!!pip install pyspark

# %%
#!pip install findspark

# %%
from pyspark import SparkContext
from pyspark.sql import SparkSession

# %%
sc = SparkContext.getOrCreate()

# %%
spark = SparkSession.builder.appName('PySpark Dataframe').getOrCreate()

# %% [markdown]
# # Transformations

# %% [markdown]
# ## `map()`

# %%
data = [1,2,3,4,5]
myRDD = sc.parallelize(data)
newRDD = myRDD.map(lambda x: x*2)

# collect() é uma forma de visualização de dados
print(newRDD.collect())

# %% [markdown]
# ## `filter()`

# %%
data = [1,2,3,4,5,6,7,8,9,10]
myRDD = sc.parallelize(data)
newRDD = myRDD.filter(lambda x: x%2 == 0)

print(newRDD.collect())

# %% [markdown]
# ## `distinct()`

# %%
data = [1,1,1,2,2,2,3,3,3,3]
myRDD = sc.parallelize(data)
newRDD = myRDD.distinct()

print(newRDD.collect())

# %%
print(newRDD.count())

# %%
print(newRDD)

# %%
print(type(newRDD))

# %% [markdown]
# ## `groupByKey()`

# %% [markdown]
# Agrupa valores de um DataFrame dentro de uma chave.

# %%
myRDD = sc.parallelize([('a',1), ('a',2), ('a',3), ('b',1)])

# print result as list
resultList = myRDD.groupByKey().mapValues(list)
resultList.collect()

# %% [markdown]
# ## `reduceByKey()`

# %%
from operator import add
myRDD = sc.parallelize([('a',1), ('a',2), ('a',3), ('b',1)])
# adds the values by keys

newRDD = myRDD.reduceByKey(add)
newRDD.collect()

# %% [markdown]
# ## `sortByKey()` - o OrderBy do SQL

# %%
myRDD = sc.parallelize([('c',1), ('d',2), ('a',3), ('b',4)])
# adds the values by keys

newRDD = myRDD.sortByKey()
newRDD.collect()

# %% [markdown]
# ## `union()`

# %%
myRDD1 = sc.parallelize([1,2,3,4])
myRDD2 = sc.parallelize([3,4,5,6,7])
# union of myRDD1 and myRDD2
newRDD = myRDD1.union(myRDD2)
newRDD.collect()

# %% [markdown]
# # Actions

# %% [markdown]
# ## `count()`

# %%
data = ['Scala', 'Python', 'Java', 'R']
myRDD = sc.parallelize(data)
# returns 4 as optout
myRDD.count()

# %% [markdown]
# ## `reduce()`

# %%
data = [1,2,3,4,5]
myRDD = sc.parallelize(data)
# returns the product of all the elements
myRDD.reduce(lambda x, y: x*y)

# %% [markdown]
# ## `forEach()`

# %%
def fun(x):
  print(x)
data = ['Scala', 'Python', 'Java', 'R']
myRDD = sc.parallelize(data)
# function applied to all the elements
myRDD.foreach(fun)

# %% [markdown]
# ## `countByValue()`

# %%
data = ['Python', 'Scala', 'Python', 'R', 'Python', 'Java', 'R']
myRDD = sc.parallelize(data)
# items() returns a list with all the dictionary keys and values returned by countByValue()
myRDD.countByValue().items()

# %% [markdown]
# ## `countByKey()`

# %%
data = [('a', 1), ('b', 1), ('c', 1), ('a', 1)]
myRDD = sc.parallelize(data)
myRDD.countByKey().items()

# %% [markdown]
# ## `take(n)` - SELECT TOP ou LIMIT do SQL

# %%
data = [2, 5, 3, 8, 4]
myRDD = sc.parallelize(data)
# return the first 3 elements
myRDD.take(3)

# %% [markdown]
# ## `top()`

# %%
data = [2, 5, 3, 8, 4]
myRDD = sc.parallelize(data)
# return the first 3 elements
myRDD.top(3)


