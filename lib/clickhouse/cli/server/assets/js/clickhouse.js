Clickhouse = (function() {
  var
    history = [],
    historyIndex = -1,
    editor,

  init = function(herstory) {
    editor = CodeMirror.fromTextArea($('#sql').get(0), {
      mode: 'text/x-mariadb',
      indentWithTabs: true,
      smartIndent: true,
      lineNumbers: true,
      matchBrackets : true,
      autofocus: true
    });

    editor.addKeyMap({
      'Alt-Up': prev,
      'Alt-Down': next,
      'Cmd-Enter': submit,
      'Cmd-R': submit
    });

    Clickhouse.history(herstory);

    $(document).on('submit', 'form', query);
    $(document).on('mousewheel', '#result_wrapper', function(event) {
      var wrapper = $('#result_wrapper');
      if (wrapper.scrollLeft() + event.originalEvent.deltaX < 0) {
        event.preventDefault();
        wrapper.scrollLeft(0);
        wrapper.scrollTop(wrapper.scrollTop() + (event.originalEvent.deltaY || 0));
      }
    });
  },

  loadHistory = function(delta) {
    var
      index = historyIndex + (delta || 0),
      sql = history.slice(index)[0];

    if (index >= -history.length && index < 0 && sql !== undefined) {
      historyIndex = index;
      editor.getDoc().setValue(sql);
    }
  },

  prev = function() {
    loadHistory(-1);
  },

  next = function() {
    loadHistory(1);
  },

  submit = function() {
    editor.save();
    $('form').submit();
  },

  runTimer = function() {
    var
      start = new Date().getTime(),
      stats = $('#stats').html('Running: <span>0.00</span>s'),
      timer = stats.find('span');

    return setInterval(function() {
      now = new Date().getTime();
      timer.html((now - start) / 1000);
    }, 60);
  },

  query = function(event) {
    event.preventDefault();
    var timer = runTimer();

    $.post('/', $(event.target).serialize(), function(json) {
      Clickhouse.history(json.history);

      $('#result').DataTable().destroy();
      $('#result').remove();

      $('form').after('<table id="result" class="stripe"></table>');
      $('#result').DataTable({
        paging: false,
        // searching: false,
        ordering: false,
        columns: $.map(json.names, function(title) {
          return {title: title};
        }),
        data: json.data
      });

      clearInterval(timer);
      $('#stats').html(json.stats);
    });
  };

  return {
    init: init,
    history: function(herstory) {
      if (herstory !== undefined) {
        history = herstory;
        historyIndex = -1;
        loadHistory();
      }
      return history;
    },
    version: '0.1.5'
  };
})();
