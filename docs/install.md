## Install Hydrosphere Mist 

You can install Mist by default tarball package or run it via docker.
All of distributions contains default configuration and our examples for a quick start.
Docker image also contains Apache Spark binaries.


Releases:

- tar <http://repo.hydrosphere.io/static/> 
- docker <https://hub.docker.com/r/hydrosphere/mist/>


We prebuild distributives for `1.5.2`, `1.6.2`, `2.0.2`, `2.1.0` of Spark.
Version of distributive is combination of version of mist and version of Spark.


As example latest Mist releases for Spark `1.6.2` version is:

- docker image `hydrosphere/mist:0.12.0-1.6.2`
- tar <http://repo.hydrosphere.io/static/mist-0.12.0-1.6.2.tar.gz>

### Install locally

- Download [Spark](https://spark.apache.org/docs/2.1.1/)
- Download Mist and run

```sh
wget http://repo.hydrosphere.io/static/mist-0.12.0-2.1.1.tar.gz
tar xvfz mist-0.12.1-2.1.1.tar.gz
cd mist-0.12.1-2.1.1

SPARK_HOME=${path to spark distributive} bin/mist-master start --debug true
```
- Check how it works on <http://localhost:2004/ui>


### From docker image

It is recommended to mount `./config` `./jobs` volumes to Mist docker container in advance.
So, we'll be able to customise configs and deploy new jobs as we go along. 

For spark 2.1.1:
```sh
mkdir jobs
mkdir my_config
curl -o ./my_config/docker.conf https://raw.githubusercontent.com/Hydrospheredata/mist/master/configs/default.conf 
curl -o ./my_config/router.conf https://raw.githubusercontent.com/Hydrospheredata/mist/master/configs/router-examples-spark2.conf

docker run \
   -p 2004:2004 \
   -v /var/run/docker.sock:/var/run/docker.sock \
   -v $PWD/my_config:/my_config \
   -v $PWD/jobs:/jobs \
   hydrosphere/mist:0.12.2-2.1.1 mist --config /my_config/docker.conf --router-config /my_config/router.conf
```

For spark 1.6.2

```sh
mkdir jobs
mkdir my_config
curl -o ./my_config/docker.conf https://raw.githubusercontent.com/Hydrospheredata/mist/master/configs/default.conf 
curl -o ./my_config/router.conf https://raw.githubusercontent.com/Hydrospheredata/mist/master/configs/router-examples-spark2.conf

docker run \
   -p 2004:2004 \
   -v /var/run/docker.sock:/var/run/docker.sock \
   -v $PWD/my_config:/my_config \
   -v $PWD/jobs:/jobs \
   hydrosphere/mist:0.12.2-1.6.2 mist --config /my_config/docker.conf --router-config /my_config/router.conf
```


![Hydrosphere Mist UI](http://dv9c7babquml0.cloudfront.net/docs-images/hydrisphere-mist-ui.png)



### Check how it works

Mist has built-in UI where you could check running workers and jobs as well as execute/debug API routes right from the web browser.
By default it's available by `/ui` path.
Link on it for local installation <http://localhost:2004/ui>.
Also Mist contains prebuilded examples from: 
- [spark1](https://github.com/Hydrospheredata/mist/tree/master/examples-spark1/src/main/scala)
- [spark2](https://github.com/Hydrospheredata/mist/tree/master/examples-spark2/src/main/scala)
- [python](https://github.com/Hydrospheredata/mist/tree/master/examples-python)
You can run that examples from ui or via any another interface that supported by Mist.
For example http call for [SimpleContext](https://github.com/Hydrospheredata/mist/blob/master/examples-spark1/src/main/scala/SimpleContext.scala)
from examples looks like that:
```sh
curl --header "Content-Type: application/json" \
     -X POST "http://localhost:2004/v2/api/endpoints/simple-context?force=true"
     -d '{"numbers": [1, 2, 3]}'

```
NOTE: here we use `force=true` to obtain job result in same http req/resp pair, it can be useful for quick jobs, but you should not use that parameter for long-running jobs


### Connecting to your existing Apache Spark cluster
If you would like to install Hydrosphere Mist on top of existing Apache Spark installation,
you should edit a config and specifying an address of your existing Apache Spark master.

For local installation it's placed at `${MIST_HOME}/config/default.conf`.
For docker it's in `my_config/docker.conf`

```
  mist.context-defaults.spark-conf = {
    spark.master = "spark://IP:PORT"
  }
```

