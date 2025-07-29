# frozen_string_literal: true

require 'active_record'

module ClickHouse
  module Client
    class QueryBuilder < QueryLike
      attr_reader :table
      attr_accessor :conditions, :manager

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
        Arel::Nodes::Grouping
      ].freeze

      def initialize(table_name)
        @table = Arel::Table.new(table_name)
        @manager = Arel::SelectManager.new(Arel::Table.engine).from(@table).project(Arel.star)
        @conditions = []
      end

      # The `where` method currently only supports IN and equal to queries along
      # with above listed VALID_NODES.
      # For example, using a range (start_date..end_date) will result in incorrect SQL.
      # If you need to query a range, use greater than and less than conditions with Arel.
      #
      # Correct usage:
      #   query.where(query.table[:created_at].lteq(Date.today)).to_sql
      #   "SELECT * FROM \"table\" WHERE \"table\".\"created_at\" <= '2023-08-01'"
      #
      # This also supports array conditions which will result in an IN query.
      #   query.where(entity_id: [1,2,3]).to_sql
      #   "SELECT * FROM \"table\" WHERE \"table\".\"entity_id\" IN (1, 2, 3)"
      #
      # Range support and more `Arel::Nodes` could be considered for future iterations.
      # @return [ClickHouse::QueryBuilder] New instance of query builder.
      def where(conditions)
        validate_condition_type!(conditions)

        deep_clone.tap do |new_instance|
          if conditions.is_a?(Arel::Nodes::Node)
            new_instance.conditions << conditions
          else
            add_conditions_to(new_instance, conditions)
          end
        end
      end

      def select(*fields)
        deep_clone.tap do |new_instance|
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

        deep_clone.tap do |new_instance|
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
        deep_clone.tap do |new_instance|
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

      def to_sql
        apply_conditions!

        visitor = Arel::Visitors::ToSql.new(ClickHouse::Client::ArelEngine.new)
        visitor.accept(manager.ast, Arel::Collectors::SQLString.new).value
      end

      def to_redacted_sql(bind_index_manager = ClickHouse::Client::BindIndexManager.new)
        ClickHouse::Client::Redactor.redact(self, bind_index_manager)
      end

      private

      def validate_condition_type!(condition)
        return unless condition.is_a?(Arel::Nodes::Node) && VALID_NODES.exclude?(condition.class)

        raise ArgumentError, "Unsupported Arel node type for QueryBuilder: #{condition.class.name}"
      end

      def add_conditions_to(instance, conditions)
        conditions.each do |key, value|
          instance.conditions << if value.is_a?(Array)
                                   instance.table[key].in(value)
                                 else
                                   instance.table[key].eq(value)
                                 end
        end
      end

      def deep_clone
        self.class.new(table.name).tap do |new_instance|
          new_instance.manager = manager.clone
          new_instance.conditions = conditions.map(&:clone)
        end
      end

      def apply_conditions!
        manager.constraints.clear
        conditions.each { |condition| manager.where(condition) }
      end

      def validate_order_direction!(direction)
        return if %w[asc desc].include?(direction.to_s.downcase)

        raise ArgumentError, "Invalid order direction '#{direction}'. Must be :asc or :desc"
      end
    end
  end
end
