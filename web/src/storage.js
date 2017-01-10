function MistStorage(prefix) {
  var self = this;
  this.prefix = prefix || 'default';

  this.set = function(key, data) {
    localStorage.setItem(storageKey(key), data);
  };

  this.get = function(key) {
    return localStorage.getItem(storageKey(key)) || "";
  };

  this.remove = function(key) {
    localStorage.removeItem(storageKey(key));
  };

  this.reset = function(obj) {
    var localStorageCopy = Object.assign({}, localStorage);
    var keys = Object.keys(obj);
    var sKey, data;

    localStorage.clear();
    keys.forEach(function(key) {
      sKey = storageKey(key);
      data = localStorageCopy[sKey];
      if (data) { localStorage.setItem(sKey, data) }
    });
  };

  function storageKey(name) {
    return self.prefix + "." + name;
  }
};
module.exports = MistStorage;
