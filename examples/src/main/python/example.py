import getscalaspark

sc = getscalaspark.getSparkContext()

#sqlc = getscalaspark.getSqlContext()

#hc = getscalaspark.getHiveContext()

parameters = getscalaspark.getParameters()

val = parameters.values()
list = val.head()
size = list.size()
pylist = []
count = 0
while count < size:
    pylist.append(list.head())
    count = count + 1
    list = list.tail()

rdd = sc.parallelize(pylist)
result = rdd.map(lambda s: 2 * s).collect()
getscalaspark.sendResult(result)