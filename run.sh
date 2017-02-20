#!/usr/bin/env bash

cid_file="mist.cid"

if [ "$1" == "init" ]; then
    export SPARK_VERSION=2.1.0
    docker create --name mist-${SPARK_VERSION} -v /usr/share/mist hydrosphere/mist:tests-${SPARK_VERSION}
    docker run --name mosquitto-${SPARK_VERSION} -d ansi/mosquitto
    docker run --name hdfs-${SPARK_VERSION} --volumes-from mist-${SPARK_VERSION} -d hydrosphere/hdfs start
    echo "Image is created. Mqtt and HDFS are ready to go."
    exit 0
fi

if [ "$1" == "run" ]; then
    cid=$(docker run -d -v /var/run/docker.sock:/var/run/docker.sock -p 1234:1234 --link mosquitto-2.1.0:mosquitto --link hdfs-2.1.0:hdfs -v /c/Users/Bulat/Documents/Projects/Provectus/data:/data -v /c/Users/Bulat/Documents/Projects/Provectus/models:/models -v /c/Users/Bulat/Documents/Projects/Provectus/mist:/usr/share/mist hydrosphere/mist:tests-2.1.0 mist --config configs/docker.conf)
    echo "$cid" > ${cid_file}
    echo "$cid"
    exit 0
fi

if [ "$1" == "build" ]; then
    sbt -DsparkVersion=2.1.0 assembly
    exit 0
fi

if [ "$1" == "logs" ]; then
    if [ -f "${cid_file}" ]; then
        cid=$(cat ${cid_file})
        docker logs ${cid}
        exit 0
    else
        echo "Mist is not running in docker..."
        exit 1
    fi
fi

if [ "$1" == "kill" ]; then
    if [ -f "${cid_file}" ]; then
        cid=$(cat ${cid_file})
        docker rm -f ${cid}
        rm ${cid_file}
        exit 0
    else
        echo "Mist is not running in docker..."
        exit 1
    fi
fi
