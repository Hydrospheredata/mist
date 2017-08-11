### About build

Mist supports Spark versions from 1.5.2 to latest.
Spark version inside build is set in sbt settings, that has default value `1.5.2` or can be set by arg `-DsparkVersion=${VERSION}`.
Example `sbt -DsparkVersion=2.1.0 mist/compile`

###### Run mist in dev mode

`sbt mist/mistRun`

Run with another ui directory (for ui development):
`sbt 'mist/mistRun --ui-dir ${path to dist in mist-ui}'`

##### Run all tests

`sbt testAll`

##### Run it tests

`sbt mist/it:test`
Note: Integrations tests require installed docker

#### Docker tags

Since we support multiple spark versions docker image tags has format "${MIST-VERSION}-${SPARK-VERSION}".

##### Build and run docker image

Note: Inside docker mist spawn worker by cloning self container and running it in docker-daemon
For that purpose `docker.sock` should be mounted to container.
```sh
sbt -DsparkVersion=2.1.0 mist/docker # will create hydrosphere/mist:0.13.0-2.1.0
docker run -p 2004:2004 -v /var/run/docker.sock:/var/run/docker.sock hydrosphere/mist:0.13.0-2.1.0 mist
```
