The code to save files to the S3 bucket is a bit weird - it creates the folder then saves the file inside it. It works okay, but have to reference the actual file when calling it in Databricks
