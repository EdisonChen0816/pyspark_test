# encoding=utf-8
from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext('local', 'my_app')
spark = SparkSession.builder.appName('my_app').getOrCreate()

stringJsonRDD = sc.parallelize(
    (
        {
            'id': 1,
            'name': 'Alice',
            'age': 22,
            'gender': 'female'
        },
        {
            'id': 2,
            'name': 'Eric',
            'age': 30,
            'gender': 'male'
        },
        {
            'id': 3,
            'name': 'Lily',
            'age': 18,
            'gender': 'female'
        },
        {
            'id': 4,
            'name': 'Tom',
            'age': 27,
            'gender': 'male'
        }
    )
)

sparkJson = spark.read.json(stringJsonRDD)

# 创建临时表
sparkJson.createOrReplaceTempView('sparkJson')
sparkJson.show()

print(spark.sql('select * from sparkJson').collect())
print(sparkJson.count())

sparkJson.select('id', 'age').filter('age = 30').show()
sparkJson.select('id', 'age', 'gender').filter('gender like "f%"').show()
spark.sql('select * from sparkJson where age > 25').show()
