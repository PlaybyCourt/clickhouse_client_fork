# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ClickHouse::Client::QueryBuilder do
  let(:table_name) { :test_table }
  let(:builder) { described_class.new(table_name) }

  shared_examples "generates correct sql on multiple calls to `to_sql`" do |method_name, argument1, argument2|
    it 'returns the same SQL when called multiple times on the same builder' do
      query_builder = builder.public_send(method_name, argument1)
      first_sql = query_builder.to_sql
      second_sql = query_builder.to_sql

      expect(first_sql).to eq(second_sql)
    end

    it 'returns different SQL when called multiple times on different builders' do
      query_builder = builder.public_send(method_name, argument1)
      query_builder_2 = query_builder.public_send(method_name, argument2)

      first_sql = query_builder.to_sql
      second_sql = query_builder_2.to_sql

      expect(first_sql).not_to eq(second_sql)
    end
  end

  describe "#initialize" do
    it 'initializes with correct table' do
      expect(builder.table.name).to eq(table_name.to_s)
    end
  end

  describe '#where' do
    context 'with simple conditions' do
      it 'builds correct where query' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` = 'value1'
          AND `test_table`.`column2` = 'value2'
        SQL

        sql = builder.where(column1: 'value1', column2: 'value2').to_sql

        expect(sql).to eq(expected_sql)
      end
    end

    context 'with array conditions' do
      it 'builds correct where query' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` IN (1, 2, 3)
        SQL

        sql = builder.where(column1: [1, 2, 3]).to_sql

        expect(sql).to eq(expected_sql)
      end
    end

    it_behaves_like "generates correct sql on multiple calls to `to_sql`", :where, { column1: 'value1' },
      { column2: 'value2' }

    context 'with supported arel nodes' do
      it 'builds a query using the In node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` IN ('value1', 'value2')
        SQL

        sql = builder.where(builder.table[:column1].in(%w[value1 value2])).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the Equality node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` = 'value1'
        SQL

        sql = builder.where(builder.table[:column1].eq('value1')).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the LessThan node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` < 5
        SQL

        sql = builder.where(builder.table[:column1].lt(5)).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the LessThanOrEqual node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` <= 5
        SQL

        sql = builder.where(builder.table[:column1].lteq(5)).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the GreaterThan node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` > 5
        SQL

        sql = builder.where(builder.table[:column1].gt(5)).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the GreaterThanOrEqual node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` >= 5
        SQL

        sql = builder.where(builder.table[:column1].gteq(5)).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the NamedFunction node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE fn(`test_table`.`column1`) > 5
        SQL

        sql = builder.where(Arel::Nodes::NamedFunction.new('fn', [builder.table[:column1]]).gt(5)).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the NotIn node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` NOT IN ('value1', 'value2')
        SQL

        sql = builder.where(builder.table[:column1].not_in(%w[value1 value2])).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the NotEqual node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` != 'value1'
        SQL

        sql = builder.where(builder.table[:column1].not_eq('value1')).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the Between node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` BETWEEN 1 AND 10
        SQL

        sql = builder.where(builder.table[:column1].between(1..10)).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the And node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`column1` = 'value1' AND `test_table`.`column2` = 'value2'
        SQL

        condition = builder.table[:column1].eq('value1').and(builder.table[:column2].eq('value2'))
        sql = builder.where(condition).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the Or node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE (`test_table`.`column1` = 'value1' OR `test_table`.`column2` = 'value2')
        SQL

        condition = builder.table[:column1].eq('value1').or(builder.table[:column2].eq('value2'))
        sql = builder.where(condition).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'builds a query using the Grouping node' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          WHERE `test_table`.`status` = 'active' AND (`test_table`.`type` = 'premium' OR `test_table`.`type` = 'vip')
        SQL

        grouped_condition = Arel::Nodes::Grouping.new(
          builder.table[:type].eq('premium').or(builder.table[:type].eq('vip'))
        )
        condition = builder.table[:status].eq('active').and(grouped_condition)
        sql = builder.where(condition).to_sql

        expect(sql).to eq(expected_sql)
      end
    end

    context 'with unsupported arel nodes' do
      it 'raises an error for the unsupported node' do
        expect do
          builder.where(builder.table[:column1].matches_regexp('pattern')).to_sql
        end.to raise_error(ArgumentError, /Unsupported Arel node type for QueryBuilder:/)
      end
    end

    context 'with nested hash constraints for joined tables' do
      let(:users_table) { Arel::Table.new('users') }

      it 'adds conditions for a joined table using nested hash syntax' do
        result = builder
                   .joins('users', { 'user_id' => 'id' })
                   .where(users: { active: true })

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
        expect(result.to_sql).to include("WHERE `users`.`active` = 'true'")
      end

      it 'adds conditions with array values for a joined table' do
        result = builder
                   .joins('users', { 'user_id' => 'id' })
                   .where(users: { id: [1, 2, 3] })

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
        expect(result.to_sql).to include('WHERE `users`.`id` IN (1, 2, 3)')
      end

      it 'adds multiple conditions for a joined table' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`
              WHERE `users`.`active` = 'true' AND `users`.`role` = 'admin'
        SQL
        result = builder
                   .joins('users', { 'user_id' => 'id' })
                   .where(users: { active: true, role: 'admin' }).to_sql

        expect(result).to eq(expected_sql)
      end

      it 'combines conditions on main table and joined table' do
        result = builder
                   .joins('users', { 'user_id' => 'id' })
                   .where(
                     column1: 'abcd',
                     users: { active: true }
                   )

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
        expect(result.to_sql).to include('WHERE `test_table`.`column1` = \'abcd\'')
        expect(result.to_sql).to include("AND `users`.`active` = 'true'")
      end

      it 'handles conditions for multiple joined tables' do
        result = builder
                   .joins('users', { 'user_id' => 'id' })
                   .joins('projects', { 'project_id' => 'id' })
                   .where(
                     users: { active: true },
                     projects: { status: 'active' }
                   )

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
        expect(result.to_sql).to include('INNER JOIN `projects` ON `test_table`.`project_id` = `projects`.`id`')
        expect(result.to_sql).to include("WHERE `users`.`active` = 'true'")
        expect(result.to_sql).to include('AND `projects`.`status` = \'active\'')
      end

      it 'handles chained where calls with nested hashes' do
        result = builder
                   .joins('users', { 'user_id' => 'id' })
                   .where(users: { active: true })
                   .where(users: { role: 'admin' })

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
        expect(result.to_sql).to include("WHERE `users`.`active` = 'true'")
        expect(result.to_sql).to include('AND `users`.`role` = \'admin\'')
      end

      it 'handles mixed constraint types in chained calls' do
        result = builder
                   .joins('users', { 'user_id' => 'id' })
                   .where(users: { active: true })
                   .where(builder.table[:created_at].gt(Date.today - 7.days))

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
        expect(result.to_sql).to include("WHERE `users`.`active` = 'true'")
        expect(result.to_sql).to include('AND `test_table`.`created_at` > ')
      end
    end
  end

  describe '#select' do
    it 'builds correct select query with single field' do
      expected_sql = <<~SQL.squish.chomp
        SELECT `test_table`.`column1` FROM `test_table`
      SQL

      sql = builder.select(:column1).to_sql

      expect(sql).to eq(expected_sql)
    end

    it 'builds correct select query with multiple fields' do
      expected_sql = <<~SQL.squish.chomp
        SELECT `test_table`.`column1`, `test_table`.`column2` FROM `test_table`
      SQL

      sql = builder.select(:column1, :column2).to_sql

      expect(sql).to eq(expected_sql)
    end

    it 'adds new fields on multiple calls without duplicating' do
      expected_sql = <<~SQL.squish.chomp
        SELECT `test_table`.`column1`, `test_table`.`column2` FROM `test_table`
      SQL

      sql = builder.select(:column1).select(:column2).select(:column1).to_sql

      expect(sql).to eq(expected_sql)
    end

    context 'with Arel expressions' do
      it 'handles Arel::Nodes::SqlLiteral' do
        literal = Arel.sql('COUNT(*) as count')
        expected_sql = <<~SQL.squish.chomp
          SELECT COUNT(*) as count FROM `test_table`
        SQL

        sql = builder.select(literal).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'handles Arel::Nodes::As (aliased columns)' do
        aliased = builder.table[:column1].as('alias1')
        expected_sql = <<~SQL.squish.chomp
          SELECT `test_table`.`column1` AS alias1 FROM `test_table`
        SQL

        sql = builder.select(aliased).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'mixes regular fields with Arel expressions' do
        literal = Arel.sql('NOW() as current_time')
        expected_sql = <<~SQL.squish.chomp
          SELECT `test_table`.`column1`, NOW() as current_time, `test_table`.`column2` FROM `test_table`
        SQL

        sql = builder.select(:column1, literal, :column2).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'preserves Arel expressions on multiple select calls' do
        literal = Arel.sql('COUNT(*) as count')
        aliased = builder.table[:column1].as('alias1')

        expected_sql = <<~SQL.squish.chomp
          SELECT COUNT(*) as count, `test_table`.`column1` AS alias1, `test_table`.`column2` FROM `test_table`
        SQL

        sql = builder.select(literal).select(aliased).select(:column2).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'deduplicates identical Arel expressions' do
        literal = Arel.sql('DISTINCT column1')
        expected_sql = <<~SQL.squish.chomp
          SELECT DISTINCT column1 FROM `test_table`
        SQL

        sql = builder.select(literal).select(literal).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'handles Arel math operations' do
        math_expr = (builder.table[:column1] + builder.table[:column2]).as('sum')
        expected_sql = <<~SQL.squish.chomp
          SELECT (`test_table`.`column1` + `test_table`.`column2`) AS sum FROM `test_table`
        SQL

        sql = builder.select(math_expr).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'handles Arel functions' do
        func = Arel::Nodes::NamedFunction.new('COALESCE', [builder.table[:column1], 0])
        expected_sql = <<~SQL.squish.chomp
          SELECT COALESCE(`test_table`.`column1`, 0) FROM `test_table`
        SQL

        sql = builder.select(func).to_sql

        expect(sql).to eq(expected_sql)
      end
    end

    context 'with edge cases' do
      it 'handles string fields alongside Arel expressions' do
        literal = Arel.sql('COUNT(*)')
        expected_sql = <<~SQL.squish.chomp
          SELECT `test_table`.`column1`, COUNT(*) FROM `test_table`
        SQL

        sql = builder.select('column1', literal).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'preserves all projections when mixing field types across multiple calls' do
        literal = Arel.sql('MAX(column1) as max_val')
        aliased = builder.table[:column2].as('col2')

        sql = builder
                .select(:column1)
                .select(literal)
                .select(aliased)
                .select(:column3)
                .to_sql

        expect(sql).to include('`test_table`.`column1`')
        expect(sql).to include('MAX(column1) as max_val')
        expect(sql).to include('`test_table`.`column2` AS col2')
        expect(sql).to include('`test_table`.`column3`')
      end
    end

    it_behaves_like "generates correct sql on multiple calls to `to_sql`", :select, :column1, :column2
  end

  describe '#order' do
    it 'builds correct order query with direction :desc' do
      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
        SELECT * FROM `test_table`
        ORDER BY `test_table`.`column1` DESC
      SQL

      sql = builder.order(:column1, :desc).to_sql

      expect(sql).to eq(expected_sql)
    end

    it 'builds correct order query with default direction asc' do
      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
        SELECT * FROM `test_table`
        ORDER BY `test_table`.`column1` ASC
      SQL

      sql = builder.order(:column1).to_sql

      expect(sql).to eq(expected_sql)
    end

    it 'appends orderings on multiple calls' do
      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
        SELECT * FROM `test_table`
        ORDER BY `test_table`.`column1` DESC,
        `test_table`.`column2` ASC
      SQL

      sql = builder.order(:column1, :desc).order(:column2, :asc).to_sql

      expect(sql).to eq(expected_sql)
    end

    it 'appends orderings for the same column when ordered multiple times' do
      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
        SELECT * FROM `test_table`
        ORDER BY `test_table`.`column1` DESC,
        `test_table`.`column1` ASC
      SQL

      sql = builder.order(:column1, :desc).order(:column1, :asc).to_sql

      expect(sql).to eq(expected_sql)
    end

    it 'raises error for invalid direction' do
      expect do
        builder.order(:column1, :invalid)
      end.to raise_error(ArgumentError, "Invalid order direction 'invalid'. Must be :asc or :desc")
    end

    context 'with Arel nodes' do
      it 'supports Arel::SqlLiteral' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          ORDER BY RAND() ASC
        SQL

        sql = builder.order(Arel.sql('RAND()')).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'supports Arel::SqlLiteral with direction' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          ORDER BY LOWER(name) DESC
        SQL

        sql = builder.order(Arel.sql('LOWER(name)'), :desc).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'supports Arel::Nodes::NamedFunction' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          ORDER BY COALESCE(`test_table`.`priority`, 0) ASC
        SQL

        function_node = Arel::Nodes::NamedFunction.new(
          'COALESCE',
          [builder.table[:priority], 0]
        )
        sql = builder.order(function_node).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'supports Arel::Attribute directly' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          ORDER BY `test_table`.`column1` DESC
        SQL

        attribute = builder.table[:column1]
        sql = builder.order(attribute, :desc).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'supports regular fields with Arel nodes' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          ORDER BY `test_table`.`column1` ASC,
          RAND() DESC,
          `test_table`.`column2` ASC
        SQL

        sql = builder
                .order(:column1)
                .order(Arel.sql('RAND()'), :desc)
                .order(:column2)
                .to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'supports complex Arel expressions like case' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          ORDER BY CASE WHEN `test_table`.`status` = 'active' THEN 1 ELSE 2 END ASC
        SQL

        case_node = Arel::Nodes::Case.new
                                     .when(builder.table[:status].eq('active')).then(1)
                                     .else(2)

        sql = builder.order(case_node).to_sql

        expect(sql).to eq(expected_sql)
      end

      it 'supports ClickHouse-specific functions with Arel::SqlLiteral' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          ORDER BY sipHash64(`test_table`.`id`) ASC
        SQL

        sql = builder.order(Arel.sql('sipHash64(`test_table`.`id`)')).to_sql

        expect(sql).to eq(expected_sql)
      end
    end

    it_behaves_like "generates correct sql on multiple calls to `to_sql`", :order, :column1, :column2
  end

  describe '#limit' do
    it 'builds correct limit query' do
      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
        SELECT * FROM `test_table`
        LIMIT 10
      SQL

      sql = builder.limit(10).to_sql

      expect(sql).to eq(expected_sql)
    end

    it 'overrides previous limit value when called multiple times' do
      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
        SELECT * FROM `test_table`
        LIMIT 20
      SQL

      sql = builder.limit(10).limit(20).to_sql

      expect(sql).to eq(expected_sql)
    end
  end

  describe '#offset' do
    it 'builds correct offset query' do
      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
        SELECT * FROM `test_table`
        OFFSET 5
      SQL

      sql = builder.offset(5).to_sql

      expect(sql).to eq(expected_sql)
    end

    it 'overrides previous offset value when called multiple times' do
      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
        SELECT * FROM `test_table`
        OFFSET 10
      SQL

      sql = builder.offset(5).offset(10).to_sql

      expect(sql).to eq(expected_sql)
    end
  end

  describe '#from' do
    it 'builds a subquery into FROM correctly' do
      inner = builder.select(:id).where(id: 1).limit(5)
      outer = builder.select(:id).from(inner, 'foo').to_sql

      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
      SELECT `test_table`.`id` FROM
      (SELECT `test_table`.`id` FROM `test_table` WHERE `test_table`.`id` = 1 LIMIT 5) foo
      SQL

      expect(outer).to eq(expected_sql)
    end

    it 'builds a subquery from raw SQL snippet' do
      inner = Arel.sql("(SELECT 1)")
      outer = builder.from(inner, 'foo').to_sql

      expect(outer).to eq('SELECT * FROM (SELECT 1) `foo`')
    end

    it 'raises error when non-arel compliant object is passes' do
      expect { builder.from('(SELECT 1)', 'foo').to_sql }.to raise_error(Arel::Visitors::UnsupportedVisitError)
    end

    it 'does not mutate the existing builder object' do
      query = builder.where(foo: 1)

      inner = Arel.sql("(SELECT 1)")
      query.from(inner, 'foo').to_sql

      expect(query.to_sql).to eq('SELECT * FROM `test_table` WHERE `test_table`.`foo` = 1')
    end
  end

  describe '#joins' do
    let(:users_table) { Arel::Table.new('users') }

    it 'adds a simple join without conditions' do
      result = builder.joins('users')

      expect(result.to_sql).to include('INNER JOIN `users`')
      expect(result).not_to eq(builder) # Should return a new instance
    end

    context 'with hash constraints' do
      it 'adds a join with a single equality condition' do
        result = builder.joins('users', { 'user_id' => 'id' })

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
      end

      it 'adds a join with multiple equality conditions' do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT * FROM `test_table`
          INNER JOIN `users` ON
          `test_table`.`user_id` = `users`.`id`
              AND
          `test_table`.`tenant_id` = `users`.`tenant_id`
        SQL

        result = builder.joins('users', { 'user_id' => 'id', 'tenant_id' => 'tenant_id' }).to_sql

        expect(result).to eq(expected_sql)
      end

      it 'handles Arel attributes in hash constraints' do
        result = builder.joins('users', { builder.table[:user_id] => users_table[:id] })

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
      end
    end

    context 'with proc constraints' do
      it 'adds a join with a proc-based condition' do
        result = builder.joins('users', ->(test_table, users) { test_table[:user_id].eq(users[:id]) })

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
      end

      it 'handles complex conditions in proc' do
        result = builder.joins('users', lambda { |test_table, users|
          test_table[:user_id].eq(users[:id]).and(users[:active].eq(true))
        })

        expect(result.to_sql).to include('INNER JOIN `users` ON')
        expect(result.to_sql).to include('`test_table`.`user_id` = `users`.`id`')
        expect(result.to_sql).to include("`users`.`active` = 'true'")
      end
    end

    context 'with Arel node constraints' do
      it 'adds a join with an Arel node condition' do
        condition = builder.table[:user_id].eq(users_table[:id])
        result = builder.joins('users', condition)

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
      end
    end

    context 'with Arel table as table_name' do
      it 'uses the provided Arel table' do
        result = builder.joins(users_table, { 'user_id' => 'id' })

        expect(result.to_sql).to include('INNER JOIN `users` ON `test_table`.`user_id` = `users`.`id`')
      end
    end

    context 'with multiple joins' do
      it 'supports chaining multiple joins' do
        result = builder
                   .joins('users', { 'user_id' => 'id' })
                   .joins('projects', { 'project_id' => 'id' })

        expect(result.to_sql).to include('INNER JOIN `users` ON')
        expect(result.to_sql).to include('INNER JOIN `projects` ON')
      end
    end
  end

  describe '#group' do
    it 'builds correct group query' do
      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
        SELECT * FROM `test_table`
        GROUP BY column1
      SQL

      sql = builder.group(:column1).to_sql

      expect(sql).to eq(expected_sql)
    end

    it 'chains multiple groups when called multiple times' do
      expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
        SELECT * FROM `test_table`
        GROUP BY column1, column2
      SQL

      sql = builder.group(:column1).group(:column2).to_sql

      expect(sql).to eq(expected_sql)
    end
  end

  describe 'method chaining', :freeze_time do
    it 'builds correct SQL query when methods are chained' do
      Time.use_zone('UTC') do
        expected_sql = <<~SQL.squish.lines(chomp: true).join(' ')
          SELECT `test_table`.`column1`, `test_table`.`column2`
          FROM `test_table`
          WHERE `test_table`.`column1` = 'value1'
          AND `test_table`.`column2` = 'value2'
          AND `test_table`.`created_at` <= '#{Time.zone.today}'
          ORDER BY `test_table`.`column1` DESC
          LIMIT 10
          OFFSET 5
        SQL

        sql = builder
          .select(:column1, :column2)
          .where(column1: 'value1', column2: 'value2')
          .where(builder.table[:created_at].lteq(Time.zone.today))
          .order(:column1, 'desc')
          .limit(10)
          .offset(5)
          .to_sql

        expect(sql).to eq(expected_sql)
      end
    end
  end

  context 'when combining with a raw query' do
    it 'correctly generates the SQL query' do
      raw_query = 'SELECT * FROM isues WHERE title = {title:String} AND id IN ({query:Subquery})'
      placeholders = {
        title: "'test'",
        query: builder.select(:id).where(column1: 'value1', column2: 'value2')
      }

      query = ClickHouse::Client::Query.new(raw_query:, placeholders:)
      expected_sql = "SELECT * FROM isues WHERE title = {title:String} AND id IN (SELECT `test_table`.`id` " \
        "FROM `test_table` WHERE `test_table`.`column1` = 'value1' AND " \
        "`test_table`.`column2` = 'value2')"

      expect(query.to_sql).to eq(expected_sql)
    end
  end

  describe '#to_redacted_sql' do
    it 'calls Redactor correctly' do
      expect(ClickHouse::Client::Redactor).to receive(:redact).with(builder,
        an_instance_of(ClickHouse::Client::BindIndexManager))

      builder.to_redacted_sql
    end

    context 'when combining with a raw query' do
      it 'correctly generates the SQL query' do
        raw_query = 'SELECT * FROM isues WHERE title = {title:String} AND id IN ({query:Subquery})'
        placeholders = {
          title: "'test'",
          query: builder.select(:id).where(column1: 'value1', column2: 'value2')
        }

        query = ClickHouse::Client::Query.new(raw_query:, placeholders:)
        expected_sql = "SELECT * FROM isues WHERE title = {title:String} AND id IN (SELECT `test_table`.`id` " \
          "FROM `test_table` WHERE `test_table`.`column1` = 'value1' AND " \
          "`test_table`.`column2` = 'value2')"

        expect(query.to_sql).to eq(expected_sql)

        expected_redacted_sql = "SELECT * FROM isues WHERE title = $1 AND id IN (SELECT `test_table`.`id` " \
          "FROM `test_table` WHERE `test_table`.`column1` = $2 AND " \
          "`test_table`.`column2` = $3)"

        expect(query.to_redacted_sql).to eq(expected_redacted_sql)
      end
    end
  end
end
