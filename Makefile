MIST_HDFS_VERSION=experimental
MIST_VERSION=experimental
SPARK_VERSION=2.0.0
HADOOP_VERSION=2.7.2

all: hdfs mist

hdfs:
	docker build \
        --build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
        -f ./Dockerfile.hdfs -t hydrosphere/hdfs:$(MIST_HDFS_VERSION) .

mist:
	docker build \
         --build-arg SPARK_VERSION=$(SPARK_VERSION) \
         -f ./Dockerfile.mist -t hydrosphere/mist:$(MIST_VERSION)-${SPARK_VERSION} .
