

from mist.decorators import *

@with_args(
    arg('numbers', type_hint=list),
    arg('multiplier', type_hint=int, default=2)
)
@on_spark_context
def simple_context(sc, numbers, multiplier):
    rdd = sc.parallelize(numbers)
    result = rdd.map(lambda s: multiplier * s).collect()
    return {"result": result}
