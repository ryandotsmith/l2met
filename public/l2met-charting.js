$(document).ready(function() {

  window.graphs = {};
  var graphElement = document.querySelector("#chart");
  var palette = new Rickshaw.Color.Palette({ scheme: 'spectrum2000' });

  function initData() {
    $.ajax({
      dataType: 'json',
      error: function(xhr, status, err) {console.log(err);},
      url: 'http://localhost:8000/metrics?name=l2met&from=2012-11-29&to=2012-11-31&limit=100',
    }).done(function(data) {
      if (data.length > 0) {
        for (var i = 0; i < data.length; i++) {
          // Pull the metric from the returned collection.
          var metric = data[i];
          // Create a new div to hold our chart.
          var elt = document.createElement('div');
          elt.id = "chart-" + metric["name"];
          elt.className = "chart-item";
          graphElement.appendChild(elt);
          // Constructthe Rickshaw objects.
          var graph = new Rickshaw.Graph({
            element: elt,
            renderer: 'line',
            series: new Rickshaw.Series.FixedDuration([metric], palette, {
              timeBase: metric['data'][0]['x'],
              timeInterval: 60 * 1000,
              maxDataPoints: 100
            })
          });
          // Draw the graph.
          graph.render();
          var hoverDetail = new Rickshaw.Graph.HoverDetail({
            graph: graph,
            xFormatter: function(x) {return null;}
          });
          //Store a reference to the graph in our global object.
          graphs[metric["name"]] = metric;
        }
      }
    });
  }
  initData();
});
