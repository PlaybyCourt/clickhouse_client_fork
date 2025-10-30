# frozen_string_literal: true

require 'active_record'

module ClickHouse
  module Client
    class QueryBuilder < QueryLike
      attr_reader :table, :database
      attr_accessor :manager

      VALID_NODES = [
        Arel::Nodes::In,
        Arel::Nodes::Equality,
        Arel::Nodes::LessThan,
        Arel::Nodes::LessThanOrEqual,
        Arel::Nodes::GreaterThan,
        Arel::Nodes::GreaterThanOrEqual,
        Arel::Nodes::NamedFunction,
        Arel::Nodes::NotIn,
        Arel::Nodes::NotEqual,
        Arel::Nodes::Between,
        Arel::Nodes::And,
        Arel::Nodes::Or,
        Arel::Nodes::Grouping,
        Arel::Nodes::Matches,
        Arel::Nodes::DoesNotMatch,
        Arel::Nodes::Division,
        Arel::Nodes::Multiplication,
        Arel::Nodes::As
      ].freeze
      AREL_ENGINE = ClickHouse::Client::ArelEngine.new

      def initialize(table_name, database: nil)
        @database = database&.to_s
        @table = Arel::Table.new(table_name.to_s)

        from_source = build_from_source(@table)
        @manager = Arel::SelectManager.new(Arel::Table.engine).from(from_source).project(Arel.star)
      end

      def initialize_copy(other)
        super

        @manager = other.manager.clone
      end

      # The `where` method currently only supports IN and equal to queries along
      # with above listed VALID_NODES.
      # For example, using a range (start_date..end_date) will result in incorrect SQL.
      # If you need to query a range, use greater than and less than constraints with Arel.
      #
      # Correct usage:
      #   query.where(query.table[:created_at].lteq(Date.today)).to_sql
      #   "SELECT * FROM \"table\" WHERE \"table\".\"created_at\" <= '2023-08-01'"
      #
      # This also supports array constraints which will result in an IN query.
      #   query.where(entity_id: [1,2,3]).to_sql
      #   "SELECT * FROM \"table\" WHERE \"table\".\"entity_id\" IN (1, 2, 3)"
      #
      # Range support and more `Arel::Nodes` could be considered for future iterations.
      # @return [ClickHouse::QueryBuilder] New instance of query builder.
      def where(constraints)
        validate_constraint_type!(constraints)

        clone.tap do |new_instance|
          apply_constraints(new_instance, constraints, :where)
        end
      end

      # The `having` method applies constraints to the HAVING clause, similar to how
      # `where` applies constraints to the WHERE clause. It supports the same constraint types.
      # Correct usage:
      #   query.group(:name).having(count: 5).to_sql
      #   "SELECT * FROM \"table\" GROUP BY \"table\".\"name\" HAVING \"table\".\"count\" = 5"
      #
      #   query.group(:name).having(query.table[:count].gt(10)).to_sql
      #   "SELECT * FROM \"table\" GROUP BY \"table\".\"name\" HAVING \"table\".\"count\" > 10"
      #
      # @return [ClickHouse::QueryBuilder] New instance of query builder.
      def having(constraints)
        validate_constraint_type!(constraints)

        clone.tap do |new_instance|
          apply_constraints(new_instance, constraints, :having)
        end
      end

      def select(*fields)
        clone.tap do |new_instance|
          existing_fields = new_instance.manager.projections.filter_map do |projection|
            if projection.respond_to?(:to_s) && projection.to_s == '*'
              nil
            elsif projection.is_a?(Arel::Attributes::Attribute)
              projection.name.to_s
            elsif projection.is_a?(Arel::Expressions)
              projection
            end
          end

          new_projections = (existing_fields + fields).map do |field|
            if field.is_a?(Symbol)
              field.to_s
            else
              field
            end
          end

          new_instance.manager.projections = new_projections.uniq.map do |field|
            if field.is_a?(Arel::Expressions)
              field
            else
              new_instance.table[field.to_s]
            end
          end
        end
      end

      def order(field, direction = :asc)
        validate_order_direction!(direction)

        clone.tap do |new_instance|
          order_node = case field
                       when Arel::Nodes::SqlLiteral, Arel::Nodes::Node, Arel::Attribute
                         field
                       else
                         new_instance.table[field]
                       end

          new_order = direction.to_s.casecmp('desc').zero? ? order_node.desc : order_node.asc
          new_instance.manager.order(new_order)
        end
      end

      def group(*columns)
        clone.tap do |new_instance|
          new_instance.manager.group(*columns)
        end
      end

      def limit(count)
        manager.take(count)
        self
      end

      def offset(count)
        manager.skip(count)
        self
      end

      def from(subquery, alias_name)
        clone.tap do |new_instance|
          if subquery.is_a?(self.class)
            new_instance.manager.from(subquery.to_arel.as(alias_name))
          else
            new_instance.manager.from(Arel::Nodes::TableAlias.new(subquery, alias_name))
          end
        end
      end

      def joins(table_name, constraint = nil)
        clone.tap do |new_instance|
          join_table = table_name.is_a?(Arel::Table) ? table_name : Arel::Table.new(table_name)

          join_condition = case constraint
                           when Hash
                             # Handle hash based constraints like { table1.id: table2.ref_id } or {id: :ref_id}
                             constraint_conditions = constraint.map do |left, right|
                               left_field = left.is_a?(Arel::Attributes::Attribute) ? left : new_instance.table[left]
                               right_field = right.is_a?(Arel::Attributes::Attribute) ? right : join_table[right]
                               left_field.eq(right_field)
                             end

                             constraint_conditions.reduce(&:and)
                           when Proc
                             constraint.call(new_instance.table, join_table)
                           when Arel::Nodes::Node
                             constraint
                           end

          if join_condition
            new_instance.manager.join(join_table).on(join_condition)
          else
            new_instance.manager.join(join_table)
          end
        end
      end

      # Aggregation helper methods

      # Creates an AVG aggregate function node
      # @param column [Symbol, String, Arel::Expressions] The column to average
      # @return [Arel::Nodes::NamedFunction] The AVG function node
      # @example Basic average
      #   query.select(query.avg(:duration)).to_sql
      #   # => "SELECT avg(`table`.`duration`) FROM `table`"
      # @example Average with alias
      #   query.select(query.avg(:price).as('average_price')).to_sql
      #   # => "SELECT avg(`table`.`price`) AS average_price FROM `table`"
      def avg(column)
        column_node = normalize_operand(column)
        Arel::Nodes::NamedFunction.new('avg', [column_node])
      end

      # Creates a quantile aggregate function node
      # @param level [Float] The quantile level (e.g., 0.5 for median)
      # @param column [Symbol, String, Arel::Expressions] The column to calculate quantile for
      # @return [Arel::Nodes::NamedFunction] The quantile function node
      # @example Calculate median (50th percentile)
      #   query.select(query.quantile(0.5, :response_time)).to_sql
      #   # => "SELECT quantile(0.5)(`table`.`response_time`) FROM `table`"
      # @example Calculate 95th percentile with alias
      #   query.select(query.quantile(0.95, :latency).as('p95')).to_sql
      #   # => "SELECT quantile(0.95)(`table`.`latency`) AS p95 FROM `table`"
      def quantile(level, column)
        column_node = normalize_operand(column)
        Arel::Nodes::NamedFunction.new("quantile(#{level})", [column_node])
      end

      # Creates a COUNT aggregate function node
      # @param column [Symbol, String, Arel::Expressions, nil] The column to count, or nil for COUNT(*)
      # @return [Arel::Nodes::NamedFunction] The COUNT function node
      # @example Count all rows
      #   query.select(query.count).to_sql
      #   # => "SELECT count() FROM `table`"
      # @example Count specific column
      #   query.select(query.count(:id)).to_sql
      #   # => "SELECT count(`table`.`id`) FROM `table`"
      def count(column = nil)
        if column.nil?
          Arel::Nodes::NamedFunction.new('count', [])
        else
          column_node = normalize_operand(column)
          Arel::Nodes::NamedFunction.new('count', [column_node])
        end
      end

      # Creates a countIf aggregate function node
      # @param condition [Arel::Nodes::Node] The condition to count
      # @return [Arel::Nodes::NamedFunction] The countIf function node
      # @raise [ArgumentError] if condition is not an Arel node
      # @example Count rows matching a condition
      #   query.select(query.count_if(query.table[:status].eq('active'))).to_sql
      #   # => "SELECT countIf(`table`.`status` = 'active') FROM `table`"
      def count_if(condition)
        raise ArgumentError, "countIf requires an Arel node as condition" unless condition.is_a?(Arel::Nodes::Node)

        Arel::Nodes::NamedFunction.new('countIf', [condition])
      end

      # Creates a division node with grouping
      # @param left [Arel::Expressions, Symbol, String, Numeric] The dividend
      # @param right [Arel::Expressions, Symbol, String, Numeric] The divisor
      # @return [Arel::Nodes::Grouping] The grouped division node for proper precedence
      # @example Simple division
      #   query.select(query.division(:completed, :total)).to_sql
      #   # => "SELECT (`table`.`completed` / `table`.`total`) FROM `table`"
      # @example Calculate percentage
      #   rate = query.division(:success_count, :total_count)
      #   query.select(query.multiply(rate, 100).as('success_rate')).to_sql
      #   # => "SELECT ((`table`.`success_count` / `table`.`total_count`) * 100) AS success_rate FROM `table`"
      def division(left, right)
        left_node = normalize_operand(left)
        right_node = normalize_operand(right)

        Arel::Nodes::Grouping.new(Arel::Nodes::Division.new(left_node, right_node))
      end

      # Creates a multiplication node with grouping
      # @param left [Arel::Expressions, Symbol, String, Numeric] The left operand
      # @param right [Arel::Expressions, Symbol, String, Numeric] The right operand
      # @return [Arel::Nodes::Grouping] The grouped multiplication node for proper precedence
      # @example Multiply columns
      #   query.select(query.multiply(:quantity, :unit_price)).to_sql
      #   # => "SELECT (`table`.`quantity` * `table`.`unit_price`) FROM `table`"
      # @example Convert to percentage
      #   query.select(query.multiply(:rate, 100).as('percentage')).to_sql
      #   # => "SELECT (`table`.`rate` * 100) AS percentage FROM `table`"
      def multiply(left, right)
        left_node = normalize_operand(left)
        right_node = normalize_operand(right)

        Arel::Nodes::Grouping.new(Arel::Nodes::Multiplication.new(left_node, right_node))
      end

      # Creates an equality node
      # @param left [Arel::Expressions, Symbol, String] The left side of the comparison
      # @param right [Arel::Expressions, Symbol, String, Numeric, Boolean] The right side of the comparison
      # @return [Arel::Nodes::Equality] The equality node
      # @example Use in WHERE clause
      #   query.where(query.equality(:status, 'active')).to_sql
      #   # => "SELECT * FROM `table` WHERE `table`.`status` = 'active'"
      # @example Use with countIf
      #   query.select(query.count_if(query.equality(:type, 'premium'))).to_sql
      #   # => "SELECT countIf(`table`.`type` = 'premium') FROM `table`"
      def equality(left, right)
        left_node = normalize_operand(left)
        right_node = normalize_operand(right)
        Arel::Nodes::Equality.new(left_node, right_node)
      end

      # Creates an alias for a node
      # @param node [Arel::Nodes::Node] The node to alias
      # @param alias_name [String, Symbol] The alias name
      # @return [Arel::Nodes::As] The aliased node
      # @raise [ArgumentError] if node is not an Arel Expression
      # @example Alias an aggregate function
      #   avg_node = query.avg(:price)
      #   query.select(query.as(avg_node, 'average_price')).to_sql
      #   # => "SELECT avg(`table`.`price`) AS average_price FROM `table`"
      def as(node, alias_name)
        raise ArgumentError, "as requires an Arel node" unless node.is_a?(Arel::Expressions)

        node.as(alias_name.to_s)
      end

      def to_sql
        visitor = ClickHouse::Client::ArelVisitor.new(AREL_ENGINE)
        visitor.accept(manager.ast, Arel::Collectors::SQLString.new).value
      end

      def to_redacted_sql(bind_index_manager = ClickHouse::Client::BindIndexManager.new)
        ClickHouse::Client::Redactor.redact(self, bind_index_manager)
      end

      def to_arel
        manager
      end

      private

      def normalize_operand(operand)
        case operand
        when Arel::Expressions
          operand
        when Symbol, String
          table[operand.to_s]
        else
          Arel::Nodes.build_quoted(operand)
        end
      end

      def validate_constraint_type!(constraint)
        return unless constraint.is_a?(Arel::Nodes::Node) && VALID_NODES.exclude?(constraint.class)

        raise ArgumentError, "Unsupported Arel node type for QueryBuilder: #{constraint.class.name}"
      end

      # Builds the FROM source node. When a database override is provided we
      # render a qualified identifier (`database`.`table`) while preserving the
      # original table alias so projections keep using the unqualified name.
      def build_from_source(table)
        return table unless database

        qualified = "#{quote_identifier(database)}.#{quote_identifier(table.name)}"
        Arel::Nodes::TableAlias.new(Arel.sql(qualified), table.name)
      end

      def quote_identifier(name)
        AREL_ENGINE.quote_table_name(name.to_s)
      end

      def apply_constraints(instance, constraints, clause_type)
        if constraints.is_a?(Arel::Nodes::Node)
          apply_constraint_node(instance, constraints, clause_type)
        else
          constraints.each do |key, value|
            if value.is_a?(Hash)
              # Handle nested hash for joined tables
              join_table = Arel::Table.new(key)
              value.each do |nested_key, nested_value|
                constraint = build_constraint(join_table, nested_key, nested_value)
                apply_constraint_node(instance, constraint, clause_type)
              end
            else
              constraint = build_constraint(instance.table, key, value)
              apply_constraint_node(instance, constraint, clause_type)
            end
          end
        end
      end

      def apply_constraint_node(instance, constraint, clause_type)
        case clause_type
        when :where
          instance.manager.where(constraint)
        when :having
          instance.manager.having(constraint)
        else
          raise ArgumentError, "Unsupported clause type: #{clause_type}"
        end
      end

      def build_constraint(table, key, value)
        if value.is_a?(Array)
          table[key].in(value)
        elsif value.is_a?(ClickHouse::Client::QueryBuilder)
          table[key].in(value.to_arel)
        else
          table[key].eq(value)
        end
      end

      def validate_order_direction!(direction)
        return if %w[asc desc].include?(direction.to_s.downcase)

        raise ArgumentError, "Invalid order direction '#{direction}'. Must be :asc or :desc"
      end
    end
  end
end
