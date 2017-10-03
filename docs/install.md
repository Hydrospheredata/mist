## Install Hydrosphere Mist 

You can install Mist from default tarball package or run it in docker.
All of distributions have default configuration and our examples for a quick start.
Docker image also has Apache Spark binaries for a quick start.


Releases:

- tar <http://repo.hydrosphere.io/hydrosphere/static/> 
- docker <https://hub.docker.com/r/hydrosphere/mist/>


We prebuilt Mist for `1.5.2`, `1.6.2`, `2.0.2`, `2.1.0` Spark versions.
Version of distributive is a combination of Mist and Spark versions.


For example latest Mist release for Spark `1.6.2` version is:

- docker image `hydrosphere/mist:0.13.0-1.6.2`
- tar <http://repo.hydrosphere.io/hydrosphere/static/mist-0.13.0-1.6.2.tar.gz>

### Install locally

- Download [Spark](https://spark.apache.org/docs/2.1.1/)
- Download Mist and run

```sh
wget http://repo.hydrosphere.io/hydrosphere/static/mist-0.13.0-2.1.1.tar.gz
tar xvfz mist-0.13.0-2.1.1.tar.gz
cd mist-0.13.0-2.1.1

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
   hydrosphere/mist:0.13.0-2.1.1 mist --config /my_config/docker.conf --router-config /my_config/router.conf
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
   hydrosphere/mist:0.13.0-1.6.2 mist --config /my_config/docker.conf --router-config /my_config/router.conf
```


![Hydrosphere Mist UI](http://dv9c7babquml0.cloudfront.net/docs-images/hydrisphere-mist-ui.png)



### Check how it works

Mist has built-in UI where you could check running workers and jobs as well as execute/debug API routes right from the web browser.
By default it's available by `/ui` path.
Link on it for local installation <http://localhost:2004/ui>.
Also Mist has prebuilt examples for: 
- [spark1](https://github.com/Hydrospheredata/mist/tree/master/examples/examples-spark1/src/main/scala)
- [spark2](https://github.com/Hydrospheredata/mist/tree/master/examples/examples-spark2/src/main/scala)
- [python](https://github.com/Hydrospheredata/mist/tree/master/examples/examples-python)
You can run these examples from web ui, REST HTTP or Messaging API.
For example http call for [SimpleContext](https://github.com/Hydrospheredata/mist/blob/master/examples-spark1/src/main/scala/SimpleContext.scala)
from examples looks like that:
```sh
curl --header "Content-Type: application/json"\
     -X POST "http://localhost:2004/v2/api/endpoints/simple-context/jobs?force=true"\
     -d '{"numbers": [1, 2, 3]}'

```
NOTE: here we use `force=true` to get job result in same http req/resp pair, it can be useful for quick jobs, but you should not use that parameter for long-running jobs


### Connecting to your existing Apache Spark cluster
If you would like to install Hydrosphere Mist on top of existing Apache Spark installation,
you should edit a config and specifying an address of your existing Apache Spark master.

For local installation it's placed at `${MIST_HOME}/config/default.conf`.
For docker it's in `my_config/docker.conf`

For standalone cluster your config should looks like that:
```
  mist.context-defaults.spark-conf = {
    spark.master = "spark://IP:PORT"
  }
```
If you want use your Yarn or Mesos cluster, there is not something special configuration from Mist side excluding `spark-master` conf.
Please, follow to offical guides([Yarn](https://spark.apache.org/docs/latest/running-on-yarn.html), [Mesos](https://spark.apache.org/docs/latest/running-on-mesos.html))
Mist uses `spark-submit` under the hood, if you need to provide environment variables for it, just set them up before starting Mist

### Next
- [Mistify your Spark job](/docs/mist-job.md)
- [Run your Mist Job](/docs/run-job.md)
