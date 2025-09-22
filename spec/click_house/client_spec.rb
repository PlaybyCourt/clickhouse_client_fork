# frozen_string_literal: true

require 'uri'
require 'tempfile'
require 'stringio'

RSpec.describe ClickHouse::Client do
  let(:database_config) do
    {
      database: 'test_db',
      url: 'http://localhost:3333',
      username: 'user',
      password: 'pass',
      variables: {
        join_use_nulls: 1
      }
    }
  end

  describe '#select' do
    # Assuming we have a DB table with the following schema
    #
    # CREATE TABLE issues (
    #   `id` UInt64,
    #   `title` String DEFAULT '',
    #   `description` Nullable(String),
    #   `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    #   `updated_at` DateTime64(6, 'UTC') DEFAULT now()
    # )
    # ENGINE = ReplacingMergeTree(updated_at)
    # ORDER BY (id)

    let(:query_result_fixture) { File.expand_path('../fixtures/query_result.json', __dir__) }

    let(:configuration) do
      ClickHouse::Client::Configuration.new.tap do |config|
        config.log_proc = lambda { |query|
          { query_string: query.to_sql }
        }
        config.register_database(:test_db, **database_config)
        config.http_post_proc = lambda { |_url, _headers, _query|
          body = File.read(query_result_fixture)
          ClickHouse::Client::Response.new(body, 200)
        }
      end
    end

    it 'parses the results and returns the data as array of hashes' do
      result = described_class.select('SELECT * FROM issues', :test_db, configuration)

      timestamp1 = ActiveSupport::TimeZone["UTC"].parse('2023-06-21 13:33:44')
      timestamp2 = ActiveSupport::TimeZone["UTC"].parse('2023-06-21 13:33:50')
      timestamp3 = ActiveSupport::TimeZone["UTC"].parse('2023-06-21 13:33:40')

      expect(result).to eq([
        {
          'id' => 2,
          'title' => 'Title 2',
          'description' => 'description',
          'created_at' => timestamp1,
          'updated_at' => timestamp1
        },
        {
          'id' => 3,
          'title' => 'Title 3',
          'description' => nil,
          'created_at' => timestamp2,
          'updated_at' => timestamp2
        },
        {
          'id' => 1,
          'title' => 'Title 1',
          'description' => 'description',
          'created_at' => timestamp3,
          'updated_at' => timestamp3
        }
      ])
    end

    context 'when the DB is not configured' do
      it 'raises error' do
        expect do
          described_class.select('SELECT * FROM issues', :different_db, configuration)
        end.to raise_error(ClickHouse::Client::ConfigurationError, /not configured/)
      end
    end

    context 'when error response is returned' do
      let(:configuration) do
        ClickHouse::Client::Configuration.new.tap do |config|
          config.register_database(:test_db, **database_config)
          config.http_post_proc = lambda { |_url, _headers, _query|
            ClickHouse::Client::Response.new('some error', 404)
          }
        end
      end

      it 'raises error' do
        expect do
          described_class.select('SELECT * FROM issues', :test_db, configuration)
        end.to raise_error(ClickHouse::Client::DatabaseError, 'some error')
      end
    end

    describe 'params transformation' do
      let(:query_object) do
        ClickHouse::Client::Query.new(raw_query: 'SELECT * FROM issues',
          placeholders: { id: 1, title: %w[foo bar], description: 'baz' })
      end

      let(:expected_params) do
        {
          'param_id' => 1,
          'param_title' => "['foo','bar']",
          "param_description" => "baz",
          'query' => 'SELECT * FROM issues'
        }
      end

      it 'transforms query params to request params' do
        db_url_params = {
          database: :test_db,
          enable_http_compression: 1
        }.merge(database_config[:variables])

        expect(configuration.http_post_proc).to receive(:call).with(
          "#{database_config[:url]}?#{db_url_params.to_query}",
          kind_of(Hash),
          expected_params).and_call_original

        described_class.select(
          query_object,
          :test_db,
          configuration)
      end
    end

    describe 'default logging' do
      let(:fake_logger) { instance_double(Logger, info: 'logged!') }
      let(:query_string) { 'SELECT * FROM issues' }

      before do
        configuration.logger = fake_logger
      end

      shared_examples 'proper logging' do
        it 'calls the custom logger and log_proc' do
          expect(fake_logger).to receive(:info).at_least(:once).with({ query_string: })

          described_class.select(query_object, :test_db, configuration)
        end
      end

      context 'when query is a string' do
        let(:query_object) { query_string }

        it_behaves_like 'proper logging'
      end

      context 'when query is a Query object' do
        let(:query_object) { ClickHouse::Client::Query.new(raw_query: query_string) }

        it_behaves_like 'proper logging'
      end
    end
  end

  describe '#insert_csv' do
    let(:actual) { Struct.new(:url, :headers, :query).new }
    let(:query_string) { 'INSERT INTO events (id) FORMAT CSV' }

    let(:configuration) do
      ClickHouse::Client::Configuration.new.tap do |config|
        config.log_proc = lambda { |query|
          { query_string: query.to_sql }
        }
        config.register_database(:test_db, **database_config)
        config.http_post_proc = lambda { |url, headers, query|
          actual.url = url
          actual.headers = headers
          actual.query = query
          ClickHouse::Client::Response.new({}, 200)
        }
      end
    end

    let(:gzip_content) do
      ActiveSupport::Gzip.compress(<<~CSV)
        id
        10
        20
      CSV
    end

    subject(:insert_csv) { described_class.insert_csv(query_string, io, :test_db, configuration) }

    shared_examples 'CSV insert' do
      it 'inserts a CSV' do
        expect(insert_csv).to be(true)

        expect(actual.url).to include(URI.encode_uri_component(query_string))
        expect(actual.headers).to include(
          'Transfer-Encoding' => 'chunked',
          'Content-Encoding' => 'gzip'
        )
      end
    end

    context 'with CSV file' do
      let(:io) { Tempfile.create('events.csv.gz') }

      before do
        File.binwrite(io.path, gzip_content)
      end

      after do
        FileUtils.rm_f(io.path)
      end

      it_behaves_like 'CSV insert'
    end

    context 'with CSV StringIO' do
      let(:io) { StringIO.new(gzip_content) }

      it_behaves_like 'CSV insert'
    end
  end
end
