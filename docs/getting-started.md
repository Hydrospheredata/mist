# Mist Getting Started

## Installing Hydrosphere Mist 

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

Default:
```sh
docker run -p 2004:2004 \
   -v /var/run/docker.sock:/var/run/docker.sock \
   hydrosphere/mist:0.12.1-2.1.1 mist
```

It is recommended to mount `./config` `./jobs` volumes to Mist docker container in advance.
So, we'll be able to customise configs and deploy new jobs as we go along. 

```sh
mkdir configs
curl -o ./configs/default.conf https://raw.githubusercontent.com/Hydrospheredata/mist/master/configs/docker.conf
mkdir jobs

docker run \
   -p 2004:2004 \
   -v /var/run/docker.sock:/var/run/docker.sock \
   -v $PWD/configs:/usr/share/mist/configs \
   -v $PWD/jobs:/jobs \
   hydrosphere/mist:0.12.1-2.1.1 mist

docker run -p 2004:2004 -v /var/run/docker.sock:/var/run/docker.sock -v $PWD/configs:/usr/share/mist/configs -v $PWD/jobs:/jobs hydrosphere/mist:0.12.1-2.1.1 mist
```

Go to Mist UI at `http://localhost:2003/ui`

![Hydrosphere Mist UI](http://dv9c7babquml0.cloudfront.net/docs-images/hydrisphere-mist-ui.png)

You could check running workers and jobs as well as execute/debug API routes right from the web browser.

### (Optional) Connecting to your existing Apache Spark cluster
If you would like to install Hydrosphere Mist on top of existing Apache Spark installation, please follow high level scheme and detailed steps outlined below. 

![Mist Spark Master config](http://dv9c7babquml0.cloudfront.net/docs-images/mist-spark-master.png)

#### (1/3) Creating or copying Mist config file and saving it in `./configs` directory  

```
mkdir configs
curl -o ./configs/mist.conf https://raw.githubusercontent.com/Hydrospheredata/mist/master/configs/docker.conf
```

#### (2/3) Editing a config and specifying an address of your existing Apache Spark master

```
mist.context-defaults.spark-conf = {
  spark.master = "spark://IP:PORT"
}
```

#### (3/3) Running Mist docker container with mounted config file

```
docker run -p 2003:2003 --name mist -v /var/run/docker.sock:/var/run/docker.sock -v $PWD/configs:/usr/share/mist/configs -v $PWD/jobs:/jobs -d hydrosphere/mist:master-2.1.0 mist
```

## What's next

Learn from use cases and tutorials here:
- [Use Cases & Tutorials](/docs/use-cases/README.md)
    - [Enterprise Analytics Applications](/docs/use-cases/enterprise-analytics.md)
    - [Reactive Applications](/docs/use-cases/reactive.md)
    - [Realtime Machine Learning Applications](/docs/use-cases/ml-realtime.md)
