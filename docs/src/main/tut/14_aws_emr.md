---
layout: docs
title: "AWS EMR"
permalink: aws-emr.html
position: 14
---

## AWS EMR Integration

*Note: this feature is available in a preview version.
API might be slightly changed in the future after including it into the release version.*

One of the core features of Mist that it provides a way to abstract from the direct job submission using `spark-submit`
and manages spark-drivers under the hood. In other words, it lazily starts workers when
a context receives a request to run a function on it.
So the goal of this feature to extend this lazy behavior of contexts to start clusters lazily too.

### Install

It's required to do configure Mist and setup AWS environment(roles, securiry groups).
This step maybe be skipped by using our [Cloudformation template](https://s3.eu-central-1.amazonaws.com/hydrosphere-cloudformation/mist-template.json)

<a href="https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=mist&templateURL=https://s3.eu-central-1.amazonaws.com/hydrosphere-cloudformation/mist-template.json">
  <img src="/mist-docs/img/cloudformation-launch-stack.png"/>
</a>

To setup template parameters you need to create AWS [Access Key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys).
Also, there are `MistLogin` and `MistPassword` parameters to setup basic authorization for accessing HTTP API.
After stack was successfully launched you can found public dns name of `mist-master` instance in the `Outputs` tab.
Note - it takes about 5 minutes to prepare the intance and launch Mist. After that, you can open it's UI and check it.

### Example

Examples could be found in hello_mist repository [here](https://github.com/Hydrospheredata/hello_mist/tree/master/scala).
If you skipped [Quick start](/mist-docs/quick_start.html) page, please check it first to become familiar with mists conpects and `mist-cli` tool.

Build:
```sh
# install mist-cli
pip install mist-cli
// or
easy_install mist-cli

# clone examples
git clone https://github.com/Hydrospheredata/hello_mist.git

cd scala
sbt package
```

There are two files: `conf/10_emr_ctx.conf` and `conf/11_emr_autoscale_ctx.conf`.
You need to select one of them to try and explicitly enable the the in `conf/20func.conf`.

Because exposing Mist API to the outside world without authorization isn't a good idea, our default template
installs `nginx` and setups basic autorization.
To use `mist-cli` you need to provide these credentials in the `--host` paremeter: `$login:$password@public-dns` and use `80` port.

Apply configuration:
```sh

mist-cli --host $MistLogin:$MistPassword@$public-dns-name --port 80 apply -f conf
```
