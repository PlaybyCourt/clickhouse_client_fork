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
    uri = URI.parse(url)

    unless body.is_a?(IO)
      # Append placeholders to URI's query
      uri.query = [uri.query, URI.encode_www_form(body.except("query"))].compact.join('&')
    end

    request = Net::HTTP::Post.new(uri)

    headers.each do |header, value|
      request[header] = value
    end

    request['Content-type'] = 'application/x-www-form-urlencoded'

    if body.is_a?(IO)
      request.body_stream = body
    else
      request.body = body['query']
    end

    response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https') do |http|
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

## ClickHouse::Client::QueryBuilder

The QueryBuilder provides an ActiveRecord-like interface for constructing ClickHouse queries programmatically. While similar to ActiveRecord's query interface, it has been tailored specifically for ClickHouse's SQL dialect and features. Pass an optional `database:` keyword when you need to query a ClickHouse schema different from the one configured on the current connection.

### Basic Usage

```ruby
# Initialize a query builder for a table
query = ClickHouse::Client::QueryBuilder.new('users')

# Build and execute queries
query.select(:id, :name).where(active: true).to_sql
# => "SELECT `users`.`id`, `users`.`name` FROM `users` WHERE `users`.`active` = 'true'"

# Query a different database explicitly
dict_query = ClickHouse::Client::QueryBuilder.new('dict_facilities', database: 'default')
dict_query.select(:facility_id).limit(5).to_sql
# => "SELECT `dict_facilities`.`facility_id` FROM `default`.`dict_facilities` `dict_facilities` LIMIT 5"
```

### WHERE Clause

The `where` method supports various types of conditions:

#### Simple Equality Conditions

```ruby
query.where(status: 'active').to_sql
# => "SELECT * FROM `users` WHERE `users`.`status` = 'active'"

# Multiple conditions (joined with AND)
query.where(status: 'active', role: 'admin').to_sql
# => "SELECT * FROM `users` WHERE `users`.`status` = 'active' AND `users`.`role` = 'admin'"
```

#### Array Conditions (IN clause)

```ruby
query.where(id: [1, 2, 3]).to_sql
# => "SELECT * FROM `users` WHERE `users`.`id` IN (1, 2, 3)"
```

#### Using Arel Nodes for Complex Conditions

```ruby
# Greater than
query.where(query.table[:age].gt(18)).to_sql
# => "SELECT * FROM `users` WHERE `users`.`age` > 18"

# Less than
query.where(query.table[:price].lt(100)).to_sql
# => "SELECT * FROM `users` WHERE `users`.`price` < 100"

# Between
query.where(query.table[:created_at].between(Date.yesterday..Date.today)).to_sql
# => "SELECT * FROM `users` WHERE `users`.`created_at` BETWEEN '2025-09-10' AND '2025-09-11'"

# Combining conditions with AND
condition = query.table[:age].gt(18).and(query.table[:status].eq('active'))
query.where(condition).to_sql
# => "SELECT * FROM `users` WHERE `users`.`age` > 18 AND `users`.`status` = 'active'"

# Combining conditions with OR
condition = query.table[:role].eq('admin').or(query.table[:role].eq('moderator'))
query.where(condition).to_sql
# => "SELECT * FROM `users` WHERE (`users`.`role` = 'admin' OR `users`.`role` = 'moderator')"

# List of supported node types in where clause
puts ClickHouse::Client::QueryBuilder::VALID_NODES
```

#### Pattern Matching with LIKE/ILIKE

```ruby
# Case-insensitive pattern matching (ILIKE - default)
query.where(query.table[:email].matches('%@example.com')).to_sql
# => "SELECT * FROM `users` WHERE `users`.`email` ILIKE '%@example.com'"

# Case-sensitive pattern matching (LIKE)
query.where(query.table[:name].matches('John%', nil, true)).to_sql
# => "SELECT * FROM `users` WHERE `users`.`name` LIKE 'John%'"

# Negative pattern matching (NOT ILIKE)
query.where(query.table[:email].does_not_match('%@spam.com')).to_sql
# => "SELECT * FROM `users` WHERE `users`.`email` NOT ILIKE '%@spam.com'"
```

#### Subqueries

```ruby
# Using a subquery in WHERE clause
subquery = ClickHouse::Client::QueryBuilder.new('orders')
  .select(:user_id)
  .where(status: 'completed')

query.where(id: subquery).to_sql
# => "SELECT * FROM `users` WHERE `users`.`id` IN (SELECT `orders`.`user_id` FROM `orders` WHERE `orders`.`status` = 'completed')"
```

### HAVING Clause

The `having` method works similarly to `where` but is used for filtering aggregated results:

```ruby
# Using COUNT(*) in HAVING clause
count_func = Arel::Nodes::NamedFunction.new('COUNT', [Arel.star])
query.group(:department).having(count_func.gt(10)).to_sql
# => "SELECT * FROM `users` GROUP BY `users`.`department` HAVING COUNT(*) > 10"

# Using other aggregation functions
sum_func = Arel::Nodes::NamedFunction.new('SUM', [query.table[:salary]])
query.group(:department).having(sum_func.gt(100000)).to_sql
# => "SELECT * FROM `users` GROUP BY `users`.`department` HAVING SUM(`users`.`salary`) > 100000"
```

#### Combining WHERE and HAVING

```ruby
query
  .where(active: true)
  .group(:department)
  .having(query.table[:avg_salary].gt(50000))
  .to_sql
# => "SELECT * FROM `users` WHERE `users`.`active` = 'true' GROUP BY `users`.`department` HAVING `users`.`avg_salary` > 50000"
```

### Working with JOINs

When using JOINs, you can apply conditions to joined tables: _(Supports only `INNER JOIN`)_

```ruby
# Join with conditions on joined table
query
  .joins('orders', { 'id' => 'user_id' })
  .where(orders: { status: 'pending' })
  .to_sql
# => "SELECT * FROM `users` INNER JOIN `orders` ON `users`.`id` = `orders`.`user_id` WHERE `orders`.`status` = 'pending'"

# HAVING clause with joined tables
query
  .joins('orders', { 'id' => 'user_id' })
  .group(:department)
  .having(orders: { total: [100, 200, 300] })
  .to_sql
# => "SELECT * FROM `users` INNER JOIN `orders` ON `users`.`id` = `orders`.`user_id` GROUP BY `users`.`department` HAVING `orders`.`total` IN (100, 200, 300)"
```

### Complete Example

Here's a comprehensive example combining multiple QueryBuilder features:

```ruby
# Find active users in specific departments who have completed orders
# Group by department and filter groups with more than 5 users

completed_orders = ClickHouse::Client::QueryBuilder.new('orders')
  .select(:user_id)
  .where(status: 'completed')
  .where(query.table[:created_at].gt(Date.today - 30))

count_func = Arel::Nodes::NamedFunction.new('COUNT', [Arel.star])

result = ClickHouse::Client::QueryBuilder.new('users')
  .select(:department, count_func.as('user_count'))
  .where(active: true)
  .where(department: ['Sales', 'Marketing', 'Engineering'])
  .where(id: completed_orders)
  .where(query.table[:email].matches('%@company.com'))
  .group(:department)
  .having(count_func.gt(5))
  .order(Arel.sql('user_count'), :desc)
  .limit(10)

puts result.to_sql
"SELECT `users`.`department`, COUNT(*) AS user_count FROM `users` WHERE `users`.`active` = 'true' 
AND `users`.`department` IN ('Sales', 'Marketing', 'Engineering') 
AND `users`.`id` IN (SELECT `orders`.`user_id` FROM `orders` WHERE `orders`.`status` = 'completed' 
AND `users`.`created_at` > '2025-08-12') 
AND `users`.`email` ILIKE '%@company.com' 
GROUP BY department HAVING COUNT(*) AS user_count > 5 
ORDER BY user_count DESC LIMIT 10"
```

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Gitlab::Danger project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://gitlab.com/gitlab-org/ruby/gems/clickhouse-client/-/blob/main/CODE_OF_CONDUCT.md?ref_type=heads).
