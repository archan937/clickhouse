Clickhouse = (function() {
  var
    connections = [],
    historyArray = [],
    historyIndex = -1,
    editor,

  init = function(herstory, conns) {
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

    history(herstory);
    urls(conns);

    $(document).on('submit', 'form', query);
    $(document).on('click', 'a.download', downloadAsTSV);
    $(document).on('mousewheel', '#result_wrapper', disableSwipeBack);
  },

  urls = function(conns) {
    if (conns !== undefined) {
      connections = conns;

      var
        urls = $('#urls').empty(),
        sql = historyArray.slice(historyIndex)[0] || '',
        querystring = '';

      if (sql.match(/^(SELECT|SHOW|DESCRIBE)/)) {
        querystring = '/?query=' + encodeURIComponent(sql.replace(';', ' FORMAT JSONCompact;'));
      }

      $(connections.sort()).each(function(index, connection) {
        if (index > 0) {
          urls.append(' - ');
        }
        urls.append('<a href="' + connection + querystring + '">' + connection + '</a>');
      });

      if (connections.length) {
        $('input,.download').removeAttr('disabled');
      } else {
        $('input,.download').attr('disabled', 'disabled');
        urls.append('<strong class="not_connected">Not connected</span>');
      }
    }

    return connections;
  },

  history = function(herstory) {
    if (herstory !== undefined) {
      historyArray = herstory;
      historyIndex = -1;
      load();
    }
    return historyArray;
  },

  load = function(delta) {
    var
      index = historyIndex + (delta || 0),
      sql = historyArray.slice(index)[0];

    if (index >= -historyArray.length && index < 0 && sql !== undefined) {
      historyIndex = index;
      editor.getDoc().setValue(sql);
    }
  },

  prev = function() {
    load(-1);
  },

  next = function() {
    load(1);
  },

  submit = function() {
    editor.save();
    if (connections.length && $('[name="sql"]').val().match(';')) {
      $('form').submit();
    }
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

  clearResult = function() {
    if ($('table#result').length) {
      $('#result').DataTable().destroy();
    }
    $('#result').remove();
  },

  query = function(event) {
    event.preventDefault();
    var timer = runTimer(), start = new Date().getTime();

    $.ajax({
      url: '/',
      method: 'POST',
      data: $(event.target).serialize(),
      success: function(json, status) {
        var time = (new Date().getTime() - start) / 1000;
        clearResult();

        if (json.data) {
          urls(json.urls);
          history(json.history);

          $('#stats').html(json.stats.replace('. Processed', '. Request time: ' + time + 's. Processed'));
          $('form').after('<table id="result" class="stripe"></table>');
          $('#result').DataTable({
            paging: false,
            ordering: false,
            columns: $.map(json.names, function(title) {
              return {title: title};
            }),
            data: json.data
          });
        } else {
          $('#stats').empty();
        }
      },
      error: function(response) {
        clearResult();
        $('#stats').empty();
        var error = response.responseText.replace('Got status 500 (expected 200): ', '');
        $('form').after('<span id="result">' + error + '</span>');
      },
      complete: function() {
        clearInterval(timer);
      }
    });
  },

  downloadAsTSV = function(event) {
    event.preventDefault();

    editor.save();
    var sql = $('[name="sql"]').val();

    if (connections.length && sql.match(';')) {
      sql = encodeURIComponent(sql.replace(';', ' FORMAT TabSeparatedWithNames;'));
      var url = $('#urls a:first').attr('href').replace(/query=.*/, 'query=' + sql);
      window.open(url);
    }
  },

  disableSwipeBack = function(event) {
    var wrapper = $('#result_wrapper');
    if (wrapper.scrollLeft() + event.originalEvent.deltaX < 0) {
      event.preventDefault();
      wrapper.scrollLeft(0);
      wrapper.scrollTop(wrapper.scrollTop() + (event.originalEvent.deltaY || 0));
    }
  };

  return {
    init: init,
    urls: urls,
    history: history,
    version: '0.1.5'
  };
})();
