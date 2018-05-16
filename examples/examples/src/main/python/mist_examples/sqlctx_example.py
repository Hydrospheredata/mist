from mistpy.decorators import *
@with_args(
  arg('path', type_hint=str)
)
@on_sql_context
def sqlctx_example(sql_context, path):
    df = sql_context.read.json(path)
    df.printSchema()
    sql_context.registerDataFrameAsTable(df, "people")

    df.show()
    df2 = sql_context.sql("SELECT AVG(age) AS avg_age FROM people")
    df2.show()

    result = df2.first().asDict(True)

    return {"result": result}

