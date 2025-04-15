# databricks-test

```
from datetime import datetime, timedelta
from pyspark.sql.functions import col, concat, lit
import requests

api_key = "DEMO_KEY"
end_date = datetime.now()
start_date = end_date - timedelta(days=30)
url = f"https://api.nasa.gov/planetary/apod?api_key={api_key}&start_date={start_date.strftime('%Y-%m-%d')}&end_date={end_date.strftime('%Y-%m-%d')}"
response = requests.get(url)
data = response.json()

df = spark.createDataFrame(data)
#df.cache()
df = df.withColumn("image", concat(lit('<img src="'), col("url"), lit('" width="100"/>')))
display(df.select(col("date"), col("title"), col("image")))
```
