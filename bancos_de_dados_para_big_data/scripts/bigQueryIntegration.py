# %%
# !pip install google-cloud-bigquery

# %%
import google.cloud.bigquery as bq

# instalando bibliotecas necessárias para importação de dados do BigQuery
from google.colab import auth
from google.cloud import bigquery
from google.colab import data_table

# autenticando usuáiro
project = 'crested-setup-419021'
location = 'US'
client = bigquery.Client(project = project, location = location)
data_table.enable_dataframe_formatter()
auth.authenticate_user()

# %%
job = client.get_job('bquxjob_3588b65a_18f01fa6260', project = 'crested-setup-419021')
results = job.to_dataframe()
results

# %%
results.describe()


