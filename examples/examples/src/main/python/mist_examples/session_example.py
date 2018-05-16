from mistpy.decorators import *

@with_args(arg('path', type_hint=str))
@on_spark_session
def session_example(spark, path):
    df = spark.read.json(path)
    df.printSchema()
    df.registerTempTable("people")

    df.show()
    df2 = spark.sql("SELECT AVG(age) AS avg_age FROM people")
    df2.show()

    result = df2.first().asDict(True)

    return {"result": result}
