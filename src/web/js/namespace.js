$.namespace = function() {
  var a=arguments, o=window, i, j, d;
  for (i=0; i<a.length; i=i+1) {
    d=(""+a[i]).split(".");
    //o=YAHOO;
    // YAHOO is implied, so it is ignored if it is included
    for (j=0; j<d.length; j=j+1) {
        o[d[j]]=o[d[j]] || {};
        o=o[d[j]];
    }
  }
  return o;
};