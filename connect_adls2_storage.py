# Azure Blob Storage using WASBS protocol
# configuration to access with protocol wasbs
# note = accessing blob storage without dbfs moun point
# not recommended since you will have to change location every time
# using mount point you can mount in any storage - s3, gcs, abfss

spark.conf.set(
    "fs.azure.account.key.<storageaccount>.blob.core.windows.net", 
    dbutils.secrets.get(scope="key-vault-secrets", key="key-access-datalake"))


df = spark.read.format("delta").load("wasbs://<container>@<storageaccount>.blob.core.windows.net/path/file")


# Azure Blob Storage using mount point
# create mount point using dbfs

dbutils.fs.mount(
        source = "wasbs://<container>@<storageaccount>.blob.core.windows.net",
        mount_point = "/mnt/<container>",
        extra_configs = {"fs.azure.account.key.<storageaccount>.blob.core.windows.net": 
                         dbutils.secrets.get(scope='key-vault-secrets',key='key-access-datalake')}
        )


df = spark.read.format("delta").load("/mnt/<container>/path/file")

# Azure Data Lake Storage Gen2 using ABFSS protocol
# adls2 (abfss) fastest protocol to access data
# designed for big data workloads
# uses hierarchical namespace

spark.conf.set(
    "fs.azure.account.key.<storageaccount>.dfs.core.windows.net",
    dbutils.secrets.get(scope="key-vault-secrets", key="key-access-datalake"))


df = spark.read.format("delta").load("abfss://<container>@<storageaccount>.dfs.core.windows.net/path/file")


# Azure Data Lake Storage Gen2 using mount point
# create mount point using Abfss

configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}


dbutils.fs.mount(
        source = "abfss://<container>@<storageaccount>.dfs.core.windows.net/",
        mount_point = "/mnt/<container>",
        extra_configs = configs
        )


df = spark.read.format("delta").load("/mnt/<container>/path/file")