function MistRequestStub() {
  this.routers = function(callback) {
    var data = {
      "streaming-1": {
        "className": "SimpleSparkStreamingLong$",
        "namespace": "streaming1",
        "path": "./examples/target/scala-2.11/mist_examples_2.11-0.10.0.jar"
      },
      "streaming-2": {
        "className": "SimpleSparkStreamingLong$",
        "namespace": "streaming2",
        "path": "./examples/target/scala-2.11/mist_examples_2.11-0.10.0.jar"
      },
      "streaming-3": {
        "className": "SimpleSparkStreamingLong$",
        "namespace": "streaming3",
        "path": "./examples/target/scala-2.11/mist_examples_2.11-0.10.0.jar"
      },
      "streaming-4": {
        "className": "SimpleSparkStreamingLong$",
        "namespace": "streaming4",
        "path": "./examples/target/scala-2.11/mist_examples_2.11-0.10.0.jar"
      },
      "streaming-5": {
        "className": "SimpleSparkStreamingLong$",
        "namespace": "streaming5",
        "path": "./examples/target/scala-2.11/mist_examples_2.11-0.10.0.jar"
      },
      "streaming-6": {
        "className": "SimpleSparkStreamingLong$",
        "namespace": "streaming6",
        "path": "./examples/target/scala-2.11/mist_examples_2.11-0.10.0.jar"
      },
      "streaming-7": {
        "className": "SimpleSparkStreamingLong$",
        "namespace": "streaming7",
        "path": "./examples/target/scala-2.11/mist_examples_2.11-0.10.0.jar"
      }
    };
    __success(data, callback);
  };

  this.runRouter = function(name, mode, params, callback) {
    var data = {
      "success": true,
      "payload": {
        "result": "Infinity Job Started"
      },
      "errors": [],
      "request": {
        "path": "./examples/target/scala-2.11/mist_examples_2.11-0.10.0.jar",
        "className": "SimpleSparkStreamingLong$",
        "namespace": "streaming2",
        "parameters": {
          "path": "/home/lblokhin/lymph/mist/examples/target/scala-2.11/mist_examples_2.11-0.10.0.jar",
          "externalId": "job3",
          "className": "SimpleSparkStreamingLong$",
          "parameters": {"numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0 ]},
          "namespace": "streaming3"
        },
        "router": "streaming-2"
      }
    };
    __success(data, callback);
  };

  this.jobs = function(callback) {
    var data = [
      {
      "uid": "83c77efc-d19e-4aec-9ef2-57313a08be4b",
      "time": "2016-12-30T09:57:22.099+04:00",
      "namespace": "streaming3",
      "router": "streaming-3"
    },
    {
      "uid": "af716bbe-1017-4a48-a59a-696b12164751",
      "time": "2016-12-30T09:57:27.394+04:00",
      "namespace": "streaming2",
      "router": "streaming-2"
    }
    ];
    __success(data, callback);
  };

  this.killJob = function(uid, callback) {
    var data = ["Job  83c77efc-d19e-4aec-9ef2-57313a08be4b is scheduled for shutdown. It may take a while."];
    __success(data, callback);
  };

  this.workers = function(callback) {
    var data = [
      {
      "namespace": "streaming3",
      "address": "akka.tcp://mist@127.0.0.1:44161"
    },
    {
      "namespace": "streaming2",
      "address": "akka.tcp://mist@127.0.0.1:38365"
    }
    ];
    __success(data, callback);
  };

  this.killWorker = function(namespase, callback) {
    var data = {"result":"Worker streaming3 is scheduled for shutdown."};
    __success(data, callback);
  };

  this.killWorkers = function(callback) {
    var data = {"result":"All Context is scheduled for shutdown."};
    __success(data, callback);
  };

  function __success(data, callback) {
    callback(data);
  }

  function __error(data) {
    // TODO
    WebMist.hideLoader();
    WebMist.showNotice(data);
  }
};
module.exports = new MistRequestStub;
