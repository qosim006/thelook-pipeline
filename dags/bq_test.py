# from google.cloud import bigquery

# client = bigquery.Client(project="de-portfolio-515206")

# sql = """
# SELECT *
# FROM `bigquery-public-data.thelook_ecommerce.orders`
# LIMIT 10
# """

# df = client.query(sql).to_dataframe()
# print(df.shape)
# print(df.head())


from google.cloud import bigquery

client = bigquery.Client(project="de-portfolio-515206")
sql = "SELECT * FROM `bigquery-public-data.thelook_ecommerce.orders` LIMIT 10"
df = client.query(sql).to_dataframe()
print(df.shape)
print(df.head())