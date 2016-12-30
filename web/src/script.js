var Mustache = require('mustache');

window.Mist = {
  //{
  //  "streaming-1": {
  //    "className": "SimpleSparkStreamingLong$",
  //    "namespace": "streaming1",
  //    "path": "./examples/target/scala-2.11/mist_examples_2.11-0.0.2.jar"
  //  },
  //  "streaming-2": {
  //    "className": "SimpleSparkStreamingLong$",
  //    "namespace": "streaming2",
  //    "path": "./examples/target/scala-2.11/mist_examples_2.11-0.0.2.jar"
  //  },
  //  "streaming-3": {
  //    "className": "SimpleSparkStreamingLong$",
  //    "namespace": "streaming3",
  //    "path": "./examples/target/scala-2.11/mist_examples_2.11-0.0.2.jar"
  //  }
  //}
  routers: function(callback) {
    this.__request("GET", location.origin + "/internal/routers", {}, callback);
  },

  //{
  //  "success": true,
  //  "payload": {
  //    "result": "Infinity Job Started"
  //  },
  //  "errors": [],
  //  "request": {
  //    "path": "./examples/target/scala-2.11/mist_examples_2.11-0.0.2.jar",
  //    "className": "SimpleSparkStreamingLong$",
  //    "namespace": "streaming2",
  //    "parameters": {
  //      "path": "/home/lblokhin/lymph/mist/examples/target/scala-2.11/mist_examples_2.11-0.0.2.jar",
  //      "externalId": "job3",
  //      "className": "SimpleSparkStreamingLong$",
  //      "parameters": {"digits": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0 ]},
  //      "namespace": "streaming3"
  //    },
  //    "router": "streaming-2"
  //  }
  //}
  runRouter: function(name, params, callback) {
    this.__request("POST", location.origin + "/api/" + name, params, callback);
  },

  //[
  //  {
  //    "uid": "83c77efc-d19e-4aec-9ef2-57313a08be4b",
  //    "time": "2016-12-30T09:57:22.099+04:00",
  //    "namespace": "streaming3",
  //    "router": "streaming-3"
  //  },
  //  {
  //    "uid": "af716bbe-1017-4a48-a59a-696b12164751",
  //    "time": "2016-12-30T09:57:27.394+04:00",
  //    "namespace": "streaming2",
  //    "router": "streaming-2"
  //  }
  //]
  jobs: function(callback) {
    this.__request("GET", location.origin + "/internal/jobs", {}, callback);
  },

  //["Job  83c77efc-d19e-4aec-9ef2-57313a08be4b is scheduled for shutdown. It may take a while."]
  killJob: function(uid, callback) {
    this.__request("DELETE", location.origin + "/internal/jobs/" + uid, {}, callback);
  },

  //[
  //  {
  //    "namespace": "streaming3",
  //    "address": "akka.tcp://mist@127.0.0.1:44161"
  //  },
  //  {
  //    "namespace": "streaming2",
  //    "address": "akka.tcp://mist@127.0.0.1:38365"
  //  }
  //]
  workers: function(callback) {
    this.__request("GET", location.origin + "/internal/workers", {}, callback);
  },

  //{"result":"Worker streaming3 is scheduled for shutdown."}
  killWorker: function(namespase, callback) {
    this.__request("DELETE", location.origin + "/internal/workers/" + namespase, {}, callback);
  },

  //{"result":"All Context is scheduled for shutdown."}
  killWorkers: function(callback) {
    this.__request("DELETE", location.origin + "/internal/workers", {}, callback);
  },

  __request: function(verb, path, body, callback) {
    var xhr = new XMLHttpRequest();

    xhr.open(verb, path, true);
    xhr.setRequestHeader('Content-Type', 'application/json');

    xhr.onreadystatechange = function() {
      if (xhr.readyState != 4) return;
      if (xhr.status == 200) {
        var response = JSON.parse(xhr.responseText);
        return callback(response);
      } else {
        // TODO
        return alert(xhr.responseText);
      }
    };

    xhr.send(body);
  }
};

window.WebMist = {
  init: function() {
    this.initEvents();
    this.loadRouters();
  },

  initEvents: function() {
    document.getElementById('menu_routers').onclick = this.loadRouters.bind(this);
    document.getElementById('menu_workers').onclick = this.loadWorkers.bind(this);
    document.getElementById('menu_jobs').onclick = this.loadJobs.bind(this);
    document.getElementById('menu_settings').onclick = this.loadSettings.bind(this);

    document.addEventListener("click", this.__processEvent.bind(this));
  },

  // ROUTERS
  loadRouters: function() {
    this.__title("Routers");
    this.showLoader();
    Mist.routers(function(data) {
      this.hideLoader();
      var template = document.getElementById('routers').innerHTML;
      this.render(template, {"routers": Object.keys(data), callback: "runRouter"});
    }.bind(this));
  },

  runRouter: function(uid) {
    this.showLoader();
    var params = document.getElementById("config-" + uid).value;
    Mist.runRouter(uid, params, function(res) {
      this.hideLoader();
      this.showNotice(res.payload.result);
    }.bind(this));
  },

  // JOBS
  loadJobs: function() {
    this.__title("Jobs");
    this.showLoader();
    Mist.jobs(function(data) {
      this.hideLoader();
      var template = document.getElementById('jobs').innerHTML;
      this.render(template, {"jobs": data, callback: "killJob"});
    }.bind(this));
  },

  killJob: function(uid) {
    this.showLoader();
    Mist.killJob(uid, function(res) {
      var container = document.getElementById("row-" + uid);
      container.remove();
      this.hideLoader();
      this.showNotice(res[0]);
    }.bind(this));
  },

  // WORKERS
  loadWorkers: function(e) {
    this.__title("Workers");
    this.showLoader();
    Mist.workers(function(data) {
      this.hideLoader();
      var template = document.getElementById('jobs').innerHTML;
      this.render(template, {"jobs": data, "uid": function() {return this.namespace}, callback: "killWorker" });
    }.bind(this));
  },

  killWorker: function(uid) {
    this.showLoader();
    Mist.killWorker(uid, function(res) {
      var container = document.getElementById("row-" + uid);
      container.remove();
      this.hideLoader();
      this.showNotice(res.result);
    }.bind(this));
  },

  // SETTINGS
  loadSettings: function() {
    this.__title("Settings");
    this.__render("");
  },

  // HELPERS
  showNotice: function (text) {
    // TODO
    //console.log(text);
  },

  showLoader: function() {
    var loader = document.getElementById('loader');
    loader.classList.remove('hide');
  },

  hideLoader: function() {
    var loader = document.getElementById('loader');
    loader.classList.add('hide');
  },

  render: function(template, data) {
    Mustache.parse(template);
    this.__render(Mustache.render(template, data));
  },

  __processEvent: function(e) {
    var target = e.target;
    if (target.tagName === "BUTTON") {
      e.stopPropagation();
      e.preventDefault();
      uid = target.getAttribute('data-uid');
      callback = target.getAttribute('data-callback');
      this[callback](uid);
    }
  },

  __title: function(text) {
    document.getElementById('title').innerHTML = text;
  },

  __render: function(content) {
    document.getElementById('content').innerHTML = content;
  }
};

WebMist.init();
