FROM frolvlad/alpine-scala:2.10

ARG SPARK_VERSION

ENV MIST_HOME=/usr/share/mist \
    SPARK_HOME=/usr/share/spark

COPY . ${MIST_HOME}

RUN apk update && \
    apk add python procps && \
    wget http://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}-bin-hadoop2.6.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop2.6.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop2.6 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop2.6.tgz && \
    cd ${MIST_HOME} && \
    ./sbt/sbt package && \
    ./sbt/sbt assembly
