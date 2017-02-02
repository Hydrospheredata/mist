var CodeMirror = require("codemirror");
require("codemirror/addon/edit/matchbrackets");
require("codemirror/addon/edit/closebrackets");
require("codemirror/addon/display/placeholder");
require("codemirror/mode/javascript/javascript");

var Mustache = require('mustache');
var Mist = require('./request');
//var Mist = require('./request_stub');
var MistStorage = require('./storage');
var MistPopup = require('./popup');

window.WebMist = {
  init: function() {
    this.routersStorage = new MistStorage("routers");
    this.popup = new MistPopup("dialog");

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
    var routersStorage = this.routersStorage;
    this.__title("Routers");
    this.showLoader();
    var self = this;
    Mist.routers(function(data) {
      routersStorage.reset(data);
      this.hideLoader();
      var template = document.getElementById('routers').innerHTML;
      self.routerInfo = Object.keys(data).map(function (m) {
        return Object.assign({name: m}, data[m])
      });
      var renderParams = {
        routers: self.routerInfo,
        routerParams: function() {return routersStorage.get(this)},
        runCallback: "runRoute",
        trainCallback: "trainRoute",
        serveCallback: "serveRoute"
      };
      this.render(template, renderParams);
    }.bind(this));
  },

  runRoute: function(uid) {
    var mlSwitch = document.querySelector("[for=switch-" + uid + "]");
    if (mlSwitch === null) {
        this.__runJob(uid, "");
    } else {
        var checked = mlSwitch.className.match("is-checked") !== null;
        if (checked) {
            this.__runJob(uid, "?serve");
        } else {
            this.__runJob(uid, "?train")
        }
    }
  },

  __runJob: function(uid, mode) {
    this.showLoader();
    var params = window.editors["job-" + uid].getValue();
    this.routersStorage.set(uid, params);
    Mist.runRouter(uid, mode, params, function(res) {
      this.hideLoader();
      try {
        this.showPopup(JSON.stringify(res, null, "\t"));
      } catch (err) {}
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
    var notice = document.getElementById("notice");
    notice.innerHTML = text;
    notice.classList.add('elementToFadeInAndOut');
    notice.classList.remove('hide');
    setTimeout(function() {
      notice.classList.add('hide');
      notice.classList.remove('elementToFadeInAndOut');
      notice.innerHTML = "";
    }, 4000);
  },

  showPopup: function(text) {
    this.popup.show(text);
  },

  closePopup: function() {
    this.popup.close();
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
    this.__render(Mustache.render(template, data), data);
  },
  
  switchMLAction: function (route) {
    this.__updateCodeMirror(route);
  },

  __processEvent: function(e) {
    var target = e.target;
    if (target.className.match("mist-action-button") !== null || target.className.match("action-item") !== null) {
//      e.stopPropagation();
//      e.preventDefault();
      uid = target.getAttribute('data-uid');
      callback = target.getAttribute('data-callback');
      this[callback](uid);
    }
  },

  __title: function(text) {
    document.getElementById('title').innerHTML = text;
  },
  
  __updateCodeMirror: function (route) {
        var settings = this.routerInfo.filter(function (m) { return m.name === route })[0];
        function make(t) { 
          if (t == "String") {
              return "string";
          } 
          if (t.startsWith("Map")) { 
              var newObj = {}; 
              var types = t.match(/^Map\[(.*?),(.*?)\]$/).slice(1); 
              newObj[make(types[0])] = make(types[1]); 
              return newObj; 
          } 
          if (t == "scala.Int") { 
              return Math.round(Math.random() * 10);
          } 
          if (t == "scala.Double") {
              return Math.random() * 10; 
          } 
          if (t.startsWith("scala.List")) { 
              var list = []; 
              var types = t.match(/^scala.List\[(.*)\]$/).slice(1); 
              list.push(make(types[0])); 
              return list; 
          } 
          if (t.startsWith("scala.Option")) { 
              var types = t.match(/^scala.Option\[(.*)\]$/).slice(1);
              return make(types[0]); 
          } 
        }
        var generatedObject = {};
        if (settings.hasOwnProperty("execute")) {
          for (var key in settings["execute"]) {
              var newObj = {};
              newObj[key] = make(settings["execute"][key]);
              Object.assign(generatedObject, newObj);
          }
          if (Object.keys(generatedObject).length === 0) {
            generatedObject = null;
            window.editors[el.id].setOption("placeholder", "No parameters are required");
            window.editors[el.id].setOption("readOnly", true);
          }
        } else if (settings.hasOwnProperty("train") || settings.hasOwnProperty("serve")) {
          var switchElement = document.querySelector("#switch-" + route);
          var checked = switchElement.checked;
          if (checked) {
            for (var key in settings["serve"]) {
              var newObj = {};
              newObj[key] = make(settings["serve"][key]);
              Object.assign(generatedObject, newObj);
            }
          } else {
            for (var key in settings["train"]) {
              var newObj = {};
              newObj[key] = make(settings["train"][key]);
              Object.assign(generatedObject, newObj);
            }        
          }
          if (Object.keys(generatedObject).length === 0) {
            generatedObject = null;
            window.editors["job-"+route].setOption("placeholder", "No parameters are required");
            window.editors["job-"+route].setOption("readOnly", true);
          } else {
            window.editors["job-"+route].setOption("placeholder", "Parameters...");
            window.editors["job-"+route].setOption("readOnly", false);
          }
        }
        if (generatedObject === null || Object.keys(generatedObject).length === 0) {
          generatedObject = "";
        } else {
          generatedObject = JSON.stringify(generatedObject, null, "\t");
        }
        window.editors["job-" + route].setValue(generatedObject);
  },
  
  

  __render: function(content, data) {
    document.getElementById('content').innerHTML = content;
    [].forEach.call(document.querySelectorAll(".updateme"), function (el) {
      componentHandler.upgradeElement(el);
    });
    window.editors = {};
    var self = this;
    [].forEach.call(document.querySelectorAll("textarea.mist-textfield"), function (el) {
      window.editors[el.id] = CodeMirror.fromTextArea(el, {
        placeholder: "Parameters...",
        matchBrackets: true,
        autoCloseBrackets: true,
        mode: "application/json",
        lineWrapping: true
      });
      var route = el.id.replace("job-", "");
      self.__updateCodeMirror(route)
    });
  }
};

window.addEventListener("load", function () {
  WebMist.init();
})
