function MistPopup(container) {
  var self = this;

  this.show = function(text) {
    this.content.innerHTML = text;
    this.popup.show();
  };

  this.close = function() {
    this.content.innerHTML = '';
    this.popup.close();
  };

  function init() {
    self.popup = document.querySelector(container);
    self.popup.querySelector('.close').addEventListener('click', self.close.bind(self));
    self.content = self.popup.querySelector('#popup-content');
  }

  init();
};
module.exports = MistPopup;
