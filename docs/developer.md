### About build

Mist is currently support Spark versions from 1.5.2 to latest.
Spark version inside  build conrolled via sbt settings, that has default value `1.5.2` or can be setted by arg `-DsparkVersion=${VERSION}`.
Example `sbt -DsparkVersion=2.1.0 mist/compile`

###### Run mist in dev mode

`sbt mist/mistRun`

##### Run all tests

`sbt testAll`

##### Run it tests

`sbt mist/it:test`
Note: Inegrations tests require installed docker

#### Docker tags

Becase we support many spark versions docker image tags has format "${MIST-VERSION}-${SPARK-VERSION}".

##### Build and run docker image

Note: Inside docker mist spawn worker by cloning self contaner and running it via docker-daemon
For that purpose `docker.sock` should be mounted to container.
```
sbt -DsparkVersion=2.1.0 mist/docker // will create hydrosphere/mist:0.12.0-2.1.0
docker run -p 2003:2003 -v /var/run/docker.sock:/var/run/docker.sock hydrosphere/mist:0.12.0-2.1.0 mist
```
