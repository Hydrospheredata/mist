---
layout: docs
title: "For developers"
permalink: dev.html
position: 12
---
## Useful sbt commands

### Run mist

```sh
# run mist with default spark version (2.0.0)
sbt mistRun
# specify spark version
sbt -DsparkVersion=2.2.0 mistRun
# specify ui-dist directory (for ui testing purposes)
sbt 'mistRun --ui-dir $path_to_ui_dist'
```

### Build mist

Tar:
```sh
# build tarball
sbt packageTar
# specify certain spark version
sbt -DsparkVersion=2.2.0 packageTar
```

Docker:
```sh
# build tarball
sbt docker
# specify certain spark version
sbt -DsparkVersion=2.2.0 docker
```

