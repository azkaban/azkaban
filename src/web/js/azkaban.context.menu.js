$.namespace('azkaban');

var flowExecuteDialogView;
azkaban.FlowExecuteDialogView= Backbone.View.extend({
  events : {
  },
  initialize : function(settings) {
  },
  render: function() {
  },
  showContextMenu: function(menuData) {
  	//$('#execute-flow-panel').show();
  	$(this.el).show();
  },
  hideContextMenu: function(menuData) {
  	//$('#execute-flow-panel').hide();
  	$(this.el).hide();
  }
});