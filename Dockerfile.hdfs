FROM frolvlad/alpine-scala:2.10

ARG HADOOP_VERSION
ARG SPARK_VERSION

ENV MIST_HOME=/usr/share/mist \
    SPARK_VERSION=${SPARK_VERSION:-1.5.2} \
    HADOOP_HOME=/usr/share/hadoop \
    HADOOP_VERSION=${HADOOP_VERSION:-2.7.2} \
    HADOOP_CONF_DIR=/usr/share/hadoop/etc/hadoop

RUN apk update && \
    apk add python curl jq coreutils openrc openssh wget bash vim procps && \
    sed -i s/#PermitRootLogin.*/PermitRootLogin\ yes/ /etc/ssh/sshd_config && \
    sed -i s/#.*StrictHostKeyChecking.*/StrictHostKeyChecking\ no/ /etc/ssh/ssh_config  && \
    ssh-keygen -A && \
    ssh-keygen -t rsa -f /root/.ssh/id_rsa -q -N "" && \
    cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys && \
    cd /usr/share && \
    wget http://ftp.twaren.net/Unix/Web/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar zxvf hadoop-${HADOOP_VERSION}.tar.gz  && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

COPY docker-app/hdfs-assets/*.xml ${HADOOP_HOME}/etc/hadoop/
COPY docker-app/hdfs-assets/entrypoint.sh /

RUN sed -i "s%<HADOOP_HOME>%${HADOOP_HOME}%" ${HADOOP_HOME}/etc/hadoop/*.xml && \
    chmod +x /entrypoint.sh && \
    ${HADOOP_HOME}/bin/hadoop namenode -format

ENTRYPOINT ["/entrypoint.sh"]
