function MistRequest() {
  this.routers = function(callback) {
    __request("GET", location.origin + "/internal/routers", {}, callback);
  };

  this.runRouter = function(name, mode, params, callback) {
    __request("POST", location.origin + "/api/" + name + mode, params || "{}", callback);
  };

  this.jobs = function(callback) {
    __request("GET", location.origin + "/internal/jobs", {}, callback);
  };

  this.killJob = function(id, namespace, callback) {
    __request("DELETE", location.origin + "/internal/jobs/" + namespace + "/"+ id, {}, callback);
  };

  this.workers = function(callback) {
    __request("GET", location.origin + "/internal/workers", {}, callback);
  };

  this.killWorker = function(namespase, callback) {
    __request("DELETE", location.origin + "/internal/workers/" + namespase, {}, callback);
  };

  this.killWorkers = function(callback) {
    __request("DELETE", location.origin + "/internal/workers", {}, callback);
  };

  function __request(verb, path, body, callback) {
    var xhr = new XMLHttpRequest();

    xhr.open(verb, path, true);
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
