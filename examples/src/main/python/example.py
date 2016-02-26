import sys
sys.path.append('/vagrant/src/main/python')
import getscalaspark

sc = getscalaspark.getSparkContext()
print(sc.startTime)
l = getscalaspark.getNumbers()

rdd = sc.parallelize(l)
print(l)
l2 = rdd.map(lambda s: 2 * s).collect()
print(l2)
getscalaspark.sendResult(l2)
