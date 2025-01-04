from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# Створюємо сесію Spark
spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

# 1. Завантажте та прочитайте кожен CSV-файл як окремий DataFrame.
products_df = spark.read.csv("./data/products.csv", header=True)
purchases_df = spark.read.csv("./data/purchases.csv", header=True)
users_df = spark.read.csv("./data/users.csv", header=True)

products_df.describe().show()
products_df.show(5)

purchases_df.describe().show()
purchases_df.show(5)

users_df.describe().show()
users_df.show(5)

# 2. Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями.
products_df = products_df.dropna()
purchases_df = purchases_df.dropna()
users_df = users_df.dropna()

products_df.describe().show()
purchases_df.describe().show()
users_df.describe().show()

# 3. Визначте загальну суму покупок за кожною категорією продуктів.
purch_prod_df = purchases_df.join(products_df, on="product_id")
purch_prod_df = purch_prod_df.withColumn("total_price", col("quantity") * col("price"))
category_totals = purch_prod_df.groupBy("category").sum("total_price")
category_totals.show()

# 4. Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.
purch_user_df = purch_prod_df.join(users_df, on="user_id")
filtered_purch_user_df = purch_user_df.filter((col("age") >= 18) & (col("age") <= 25))
category_totals_18_25 = filtered_purch_user_df.groupBy("category").sum("total_price")
category_totals_18_25.show()

# 5. Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.
total_18_25 = category_totals_18_25.agg({"sum(total_price)": "sum"}).collect()[0][0]
category_percentages = category_totals_18_25.withColumn(
    "percentage", round((col("sum(total_price)") / total_18_25) * 100, 2)
)
category_percentages.show()

# 6. Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.
top_categories = category_percentages.orderBy(col("percentage").desc()).limit(3)
top_categories.show()

spark.stop()
