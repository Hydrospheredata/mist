function MistRequest() {
  this.__apiRoot = "../"
  this.routers = function(callback) {
    __request("GET", "/internal/routers", {}, callback);
  };

  this.runRouter = function(name, mode, params, callback) {
    __request("POST", "/api/" + name + mode, params || "{}", callback);
  };

  this.jobs = function(callback) {
    __request("GET", "/internal/jobs", {}, callback);
  };

  this.killJob = function(id, namespace, callback) {
    __request("DELETE", "/internal/jobs/" + namespace + "/"+ id, {}, callback);
  };

  this.workers = function(callback) {
    __request("GET", "/internal/workers", {}, callback);
  };

  this.killWorker = function(namespase, callback) {
    __request("DELETE", "/internal/workers/" + namespase, {}, callback);
  };

  this.killWorkers = function(callback) {
    __request("DELETE", "/internal/workers", {}, callback);
  };

  function __request(verb, path, body, callback) {
    var xhr = new XMLHttpRequest();

    var apiPath = this.__apiRoot + path
    xhr.open(verb, apiPath, true);
    xhr.setRequestHeader('Content-Type', 'application/json');

    xhr.onreadystatechange = function() {
      if (xhr.readyState != 4) return;
      if (xhr.status == 200) {
        if (xhr.responseText === "") {
            return callback();
        } else {
            var response = JSON.parse(xhr.responseText);
            return callback(response);
        }
      } else {
        // TODO
        WebMist.hideLoader();
        return WebMist.showNotice(xhr.responseText);
      }
    };

    xhr.send(body);
  }
};
module.exports = new MistRequest;
