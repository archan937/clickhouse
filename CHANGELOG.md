## Clickhouse CHANGELOG

### Version 0.1.4 (October 25, 2016)

* Ensured that Clickhouse does not modify the passed cluster config
* Introduced Clickhouse#connect which returns either a cluster or connection without memoizing it as Clickhouse#connection

### Version 0.1.3 (October 21, 2016)

* Only removing connections from cluster connection pool after a Clickhouse::ConnectionError

### Version 0.1.2 (October 20, 2016)

* Being able to specify a URL
* Raising a Clickhouse::ConnectionError when getting a Faraday::Error
* Made Clickhouse::Connection::Client#ping! public
* Supporting cluster connections using Pond (thanks @chanks)

### Version 0.1.1 (October 19, 2016)

* Using the JSONCompact format as query output which does not brake when having a JSON string within the data
* Ensuring that Clickhouse::Connection::Query#count return an integer
* Made Clickhouse::Connection::Query#to_select_query public
* Being able to pass strings as :where or :having option
* Being able to symbolize the row to hash parsing

### Version 0.1.0 (October 18, 2016)

* Initial release
