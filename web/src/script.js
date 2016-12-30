var Mustache = require('mustache');

window.Mist = {
  init: function() {
    this.initEvents();
    this.loadRouters();
  },

  initEvents: function() {
    document.getElementById('menu_routers').onclick = this.loadRouters.bind(this);
    document.getElementById('menu_workers').onclick = this.loadWorkers.bind(this);
    document.getElementById('menu_jobs').onclick = this.loadJobs.bind(this);
    document.getElementById('menu_settings').onclick = this.loadSettings.bind(this);
  },

  loadRouters: function() {
    this.__title("Routers");
    var template = document.getElementById('routers').innerHTML;
    var data = {routers: [{name: "router-111"}, {name: "router-222"}]};
    this.render(template, data);
  },

  loadWorkers: function(e) {
    this.__title("Workers");
    var _this = this;
    this.__request("POST", "internal", {cmd: "list workers"}, function(data) {
      var template = document.getElementById('jobs').innerHTML;
      _this.render(template, data);
    });
  },

  killWorker: function(e) {
    var id = e.currentTarget.elements[0].value;
    document.getElementById(id).remove();
  },

  loadJobs: function() {
    this.__title("Jobs");
    var _this = this;
    this.__request("POST", "internal", {cmd: "list jobs"}, function(data) {
      var template = document.getElementById('jobs').innerHTML;
      _this.render(template, data);
    });
  },

  loadSettings: function() {
    this.__title("Settings");
    this.__render("");
  },

  showLoader: function() {
    var loader = document.getElementById('loader');
    loader.classList.remove('hide');
  },

  hideLoader: function() {
    var loader = document.getElementById('loader');
    loader.classList.add('hide');
  },

  onError: function(text) {
    //!!!!! TODO
    console.log(text);
  },

  render: function(template, data) {
    Mustache.parse(template);
    this.__render(Mustache.render(template, data));
  },

  __request: function(verb, path, body, callback) {
    var _this = this;
    var xhr = new XMLHttpRequest();
    var json = JSON.stringify(body);
    this.showLoader();

    xhr.open(verb, path, true);
    xhr.setRequestHeader('Content-Type', 'application/json');

    xhr.onreadystatechange = function() {
      if (xhr.readyState != 4) return;
      _this.hideLoader();
      if (xhr.status != 200) {
        return _this.onError(xhr.responseText);
      } else {
        var response = JSON.parse(xhr.responseText);
        return callback(response);
      }
    };

    xhr.send(json);
  },

  __title: function(text) {
    document.getElementById('title').innerHTML = text;
  },

  __render: function(content) {
    document.getElementById('content').innerHTML = content;
  }
};

Mist.init();
