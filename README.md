# ClickHouse::Client

This Gem provides a simple way to query ClickHouse databases using the HTTP interface. 

## Example usage

```ruby
require 'logger'
require 'net/http'

ClickHouse::Client.configure do |config|
  # Register your database(s)
  config.register_database(:main,
                           database: 'default',
                           url: 'http://localhost:8123',
                           username: 'default',
                           password: 'clickhouse',
                           variables: { mutations_sync: 1 }
                          )

  config.logger = Logger.new(STDOUT)

  # Use any HTTP client to build the POST request, here we use Net::HTTP
  config.http_post_proc = ->(url, headers, body) do
    # Query placeholders go to the URI
    params = URI.encode_www_form(body.except("query"))

    uri = URI.parse("#{url}&#{params}")
    request = Net::HTTP::Post.new(uri)

    headers.each do |header, value|
      request[header] = value
    end

    request['Content-type'] = 'application/x-www-form-urlencoded'
    request.body = body["query"]

    response = Net::HTTP.start(uri.hostname, uri.port) do |http|
      http.request(request)
    end

    ClickHouse::Client::Response.new(response.body, response.code.to_i, response.each_header.to_h)
  end
end

# Run some statements
puts ClickHouse::Client.select('SELECT 1+1', :main)

query = ClickHouse::Client::Query.new(raw_query: 'SELECT {number1:Int64} + {number2:Int64}', placeholders: { number1: 11, number2: 4 })
puts ClickHouse::Client.select(query, :main)

puts ClickHouse::Client.execute('CREATE TABLE IF NOT EXISTS t1 (id Int64) ENGINE=MergeTree PRIMARY KEY id', :main)
puts ClickHouse::Client.execute('DROP TABLE IF EXISTS t1', :main)
```

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Gitlab::Danger project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://gitlab.com/gitlab-org/ruby/gems/clickhouse-client/-/blob/main/CODE_OF_CONDUCT.md?ref_type=heads).
