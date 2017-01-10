var Mustache = require('mustache');
var Mist = require('./request');
//var Mist = require('./request_stub');
var MistStorage = require('./storage');

window.WebMist = {
  init: function() {
    this.routersStorage = new MistStorage("routers");

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
    Mist.routers(function(data) {
      routersStorage.reset(data);
      this.hideLoader();
      var template = document.getElementById('routers').innerHTML;
      var renderParams = {
        routers: Object.keys(data),
        routerParams: function() {return routersStorage.get(this)},
        runCallback: "runRoute",
        trainCallback: "trainRoute",
        serveCallback: "serveRoute"
      };
      this.render(template, renderParams);
    }.bind(this));
  },

  runRoute: function(uid) {
    this.__runJob(uid, "")
  },

  trainRoute: function(uid) {
    this.__runJob(uid, "?train")
  },

  serveRoute: function () {
    this.__runJob(uid, "?serve")
  },

  __runJob: function(uid, mode) {
    this.showLoader();
    var params = document.getElementById("config-" + uid).value;
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
    var content = document.getElementById("popup-content");
    var dialog = document.querySelector('dialog');
    var showDialogButton = document.querySelector('#show-dialog');
    content.innerHTML = text;

    if (! dialog.showModal) {
      dialogPolyfill.registerDialog(dialog);
    }
    dialog.showModal();

    dialog.querySelector('.close').addEventListener('click', this.closePopup);
  },

  closePopup: function() {
    document.getElementById("popup-content").innerHTML = "";
    document.querySelector('dialog').close();
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
    if (target.className.match("mist-action-button") !== null || target.className.match("action-item") !== null) {
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
    [].forEach.call(document.querySelectorAll(".updateme, .mist-textfield"), function (el) {
      componentHandler.upgradeElement(el);
    });
  }
};

window.addEventListener("load", function () {
  WebMist.init();
})
