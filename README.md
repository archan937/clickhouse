# Clickhouse [![Build Status](https://travis-ci.org/archan937/clickhouse.svg?branch=master)](https://travis-ci.org/archan937/clickhouse) [![Code Climate](https://codeclimate.com/github/archan937/clickhouse/badges/gpa.svg)](https://codeclimate.com/github/archan937/clickhouse) [![Test Coverage](https://codeclimate.com/github/archan937/clickhouse/badges/coverage.svg)](https://codeclimate.com/github/archan937/clickhouse/coverage) [![Gem](https://img.shields.io/gem/v/clickhouse.svg)](https://rubygems.org/gems/clickhouse) [![Gem](https://img.shields.io/gem/dt/clickhouse.svg)](https://rubygems.org/gems/clickhouse)

A Ruby database driver for ClickHouse (also Clickhouse CLI and web GUI included).

## Introduction

[ClickHouse](https://clickhouse.yandex) is a high-performance column-oriented database management system developed by [Yandex](https://yandex.com/company) which operates Russia's most popular search engine.

> ClickHouse manages extremely large volumes of data in a stable and sustainable manner. It currently powers Yandex.Metrica, world’s second largest web analytics platform, with over 13 trillion database records and over 20 billion events a day, generating customized reports on-the-fly, directly from non-aggregated data. This system was successfully implemented at CERN’s LHCb experiment to store and process metadata on 10bn events with over 1000 attributes per event registered in 2011.

On June 15th 2016, [Yandex open-sourced their awesome project](https://news.ycombinator.com/item?id=11908254) giving the community a [powerful asset](https://clickhouse.yandex/benchmark.html) which can compete with the big players like [Google BigQuery](https://cloud.google.com/bigquery/) and [Amazon Redshift](http://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html) with an important advantage: the client can use ClickHouse in its infrastructure and does not have to pay for the cloud ([read more](https://translate.google.com/translate?sl=ru&tl=en&js=y&prev=_t&hl=en&ie=UTF-8&u=https://habrahabr.ru/company/yandex/blog/303282/)).

### Why use the HTTP interface and not the TCP interface?

Well, the developers of ClickHouse themselves [discourage](https://github.com/yandex/ClickHouse/issues/45#issuecomment-231194134) using the TCP interface.

> TCP transport is more specific, we don't want to expose details.
Despite we have full compatibility of protocol of different versions of client and server, we want to keep the ability to "break" it for very old clients. And that protocol is not too clean to make a specification.

### Why use the JSONCompact format and not the native format?

Despite of it being the most efficient format, using the native format is also [discouraged](https://clickhouse.yandex/reference_en.html#Native) by the ClickHouse developers.

> The most efficient format. Data is written and read by blocks in binary format. For each block, the number of rows, number of columns, column names and types, and parts of columns in this block are recorded one after another. In other words, this format is "columnar" - it doesn't convert columns to rows. This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.
>
> You can use this format to quickly generate dumps that can only be read by the ClickHouse DBMS. It doesn't make sense to work with this format yourself.

## Installation

Run the following command to install `Clickhouse`:

    $ gem install "clickhouse"

## Usage

### Quick start

Require the Clickhouse gem.

```ruby
require "clickhouse"
```

Setup the logging output.

```ruby
require "logger"
Clickhouse.logger = Logger.new(STDOUT)
```

Establish the connection with the ClickHouse server (using the default config).

```ruby
Clickhouse.establish_connection
=> true
```

List databases and tables.

```ruby
Clickhouse.connection.databases
I, [2016-10-17T22:54:26.587401 #81829]  INFO -- :
  SQL (64.0ms)  SHOW DATABASES;
=> ["default", "system"]

Clickhouse.connection.tables
I, [2016-10-17T22:54:51.454012 #81829]  INFO -- :
  SQL (61.7ms)  SHOW TABLES;
=> []
```

Create tables.

```ruby
Clickhouse.connection.create_table("events") do |t|
  t.fixed_string :id, 16
  t.uint16       :year
  t.date         :date
  t.date_time    :time
  t.string       :event
  t.uint32       :user_id
  t.float32      :revenue
  t.engine       "MergeTree(date, (year, date), 8192)"
end
=> true

Clickhouse.connection.query "DESCRIBE TABLE events" # or Clickhouse.connection.describe_table "events"
=> #<Clickhouse::Connection::Query::ResultSet:0x007fa9ac137010
 @names=["name", "type", "default_type", "default_expression"],
 @rows=
  [["id", "FixedString(16)", nil, nil],
   ["year", "UInt16", nil, nil],
   ["date", "Date", nil, nil],
   ["time", "DateTime", nil, nil],
   ["event", "String", nil, nil],
   ["user_id", "UInt32", nil, nil],
   ["revenue", "Float32", nil, nil]],
 @types=["String", "String", "String", "String"]>
```

Insert data.

```ruby
Clickhouse.connection.insert_rows(events, :names => %w(id year date time event user_id revenue)) do |rows|
  rows << [
    "d91d1c90",
    2016,
    "2016-10-17",
    "2016-10-17 23:14:28",
    "click",
    1982,
    0.18
  ]
  rows << [
    "d91d2294",
    2016,
    "2016-10-17",
    "2016-10-17 23:14:41",
    "click",
    1947,
    0.203
  ]
end
=> true
```

Query data.

```ruby
Clickhouse.connection.count :from => "events"
I, [2016-10-17T23:19:45.592602 #82196]  INFO -- :
  SQL (65.4ms)  SELECT COUNT(*)
FROM events;
=> 2

Clickhouse.connection.select_row :select => "COUNT(*), year, date, avg(revenue)", :from => "events", :group => "year, date"
I, [2016-10-17T23:22:47.340232 #82196]  INFO -- :
  SQL (67.7ms)  SELECT COUNT(*), year, date, avg(revenue)
FROM events
GROUP BY year, date;
=> [2, 2016, #<Date: 2016-10-17 ((2457679j,0s,0n),+0s,2299161j)>, 0.1915000081062317]
```

### Connecting to a cluster

To connect to a cluster you only need to specify the URLs of the cluster servers in `:urls` of the configuration and that is it! The API of using Clickhouse stays the same.

```ruby
Clickhouse.establish_connection urls: %w(http://192.168.99.100:32809 http://192.168.99.100:32812 http://192.168.99.100:32815)
=> true

Clickhouse.connection.tables
I, [2016-10-21T11:56:47.375772 #63374]  INFO -- :
  SQL (6.2ms)  SHOW TABLES;
=> ["events"]
```

In case of a connection dropping out, Clickhouse will retry the request with another connection. The failed connection will also be removed from the connection pool.

```ruby
Clickhouse.establish_connection urls: %w(http://192.168.99.100:32809 http://192.168.99.100:1 http://192.168.99.100:32815)
=> true

Clickhouse.connection.pond.available.collect(&:url)
=> ["http://192.168.99.100:1", "http://192.168.99.100:32815", "http://192.168.99.100:32809"]

Clickhouse.connection.tables
I, [2016-10-21T12:11:55.974573 #63527]  INFO -- :
  SQL (7.1ms)  SHOW TABLES;
=> ["events"]

Clickhouse.connection.pond.available.collect(&:url)
=> ["http://192.168.99.100:32809", "http://192.168.99.100:32815"]
```

If all the connections failed, it will just return `nil`.

### Check out the tests

To see what more the `Clickhouse` gem has to offer, please take a look at the unit tests ( [test/unit/connection/test_query.rb](https://github.com/archan937/clickhouse/blob/master/test/unit/connection/test_query.rb) for instance).

## Using the console

As you probably already noticed, the `Clickhouse` repo is provided with a `script/console` file which you can use for development / testing purposes. Please note that you need to have a ClickHouse server running.

### Running a ClickHouse server on your Mac or Windows computer

Despite that the ClickHouse build is not intended to work on Mac OS X or Windows (only x86_64 with SSE 4.2 is supported), you can still run a ClickHouse server instance on both the operating systems using the [ClickHouse Server Docker Image](https://hub.docker.com/r/yandex/clickhouse-server/) hosted on [https://hub.docker.com/](Docker Hub).

The installation process is just a matter of two simple steps:

* Download and install [Kitematic](https://kitematic.com/) (Docker Toolbox) on your computer
* Install the [clickhouse-server](https://hub.docker.com/r/yandex/clickhouse-server/) container using Kitematic

Et voilà! Your ClickHouse server instance is up and running locally. Please make sure to use the proper IP address and port to connect with. You can find it at the container details within Kitematic (it is the `Access URL` corresponded with the `8123/tcp Docker port`).

### Example

    $ script/console
    Loading Clickhouse development environment (0.1.1)
    [1] pry(main)> connect! host: "192.168.99.100", port: 32770
    => true
    [2] pry(main)> conn.databases
    I, [2016-10-19T20:54:53.081388 #29847]  INFO -- :
      SQL (3.1ms)  SHOW DATABASES;
    => ["default", "system"]
    [3] pry(main)>

## Testing

Run the following command for testing:

    $ rake

You can also run a single test file:

    $ ruby test/unit/connection/test_query.rb

## Contact me

For support, remarks and requests, please mail me at [pm_engel@icloud.com](mailto:pm_engel@icloud.com).

## License

Copyright (c) 2016 Paul Engel, released under the MIT license

http://github.com/archan937 – http://twitter.com/archan937 – pm_engel@icloud.com

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
