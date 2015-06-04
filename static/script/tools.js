function urljoin(url, params){
  param_string = url + "?";
  $.each(params, function(k, v){
    param_string += k + "=" + v + "&";
  });
  param_string = param_string.slice(0, param_string.length - 1)
  return param_string
}
