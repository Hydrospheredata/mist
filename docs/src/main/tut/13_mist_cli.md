---
layout: docs
title: "Mist-Cli"
permalink: mist-cli.html
position: 13
---

### Mist cli

[Github](https://github.com/Hydrospheredata/mist-cli)


Mist-cli provides an command line interface to mist-server to manage contexts/functions.
Actually under the hood it uses [http api](/mist_docs/http_api.html).

Instanciation of a new endpoint on mist requires following steps:
- upload an articat with function
- create context for it's invocation
- create function

The main goal of `mist-cli` is to reduce these step into one command and provide a some
configuration management way where you can easily reproduce settings on a mist on demand.

There are 3 types of configs:
- Artifact
- Context
- Function

See [examples](https://github.com/Hydrospheredata/mist-cli/tree/master/example)
