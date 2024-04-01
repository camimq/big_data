# %%
# !pip install pyspark

# %%
# !pip install findspark

# %%
# instalar as dependências
# !apt-get install openjdk-8-jdk-headless -qq > /dev/null
# !wget -q https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
# !tar xf spark-2.4.4-bin-hadoop2.7.tgz

# %%
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator #evaluation é a biblioteca para verificação da qualidade do modelo
from pyspark.ml.recommendation import ALS # ALS é o modelo de recomendação que será utilizadp
from pyspark.sql import Row #row é o formato que o ALS trabalha, row conterá o id do usuario, id filme, nota e timestamp

# %%
spark = SparkSession.builder.master('local[*]').getOrCreate()

# %%
lines = spark.read.text("/content/sample_movielens_ratings.txt").rdd

# %%
parts = lines.map(lambda row: row.value.split("::"))

# %%
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), \
                                     movieId=int(p[1]), \
                                     rating=float(p[2]), \
                                     timestamp=int(p[3])))

# %%
ratings = spark.createDataFrame(ratingsRDD)

# %%
lines.collect()

# %%
ratings.show()

# %%
(training, test) = ratings.randomSplit([0.8, 0.2]) #divide o df em porções para treinamento e teste

# %%
als = ALS(maxIter=5, \
          regParam=0.01, \
          userCol="userId", \
          itemCol="movieId", \
          ratingCol="rating", \
          coldStartStrategy="drop")

# %%
model = als.fit(training) #treina o modelo com o dataset de treinamento

# %%
predictions = model.transform(test) #aplica o modelo no conjunto de teste para fazer predições
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                               predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Erro médio quadrático = " + str(rmse))

# %%
userRec = model.recommendForAllUsers(10)

# %%
userRec.show()

# %%
movieRecs = model.recommendForAllItems(10) #faz a transposta da matriz de ratings, a fim de recomendar usuários em potencial para itens específicos

# %%
movieRecs.show()

# %%
users = ratings.select(als.getUserCol()).distinct() #selecina os usuários que existem nesse universo

# %%
users.show()

# %%
UserRecsOnlyItemId = userRec.select(userRec['userId'], \
                                    userRec['recommendations']['movieid'])

# %%
UserRecsOnlyItemId.show(10, False) #mostra somente as recomendações por usuário


