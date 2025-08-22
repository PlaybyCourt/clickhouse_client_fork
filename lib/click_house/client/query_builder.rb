# frozen_string_literal: true

require 'active_record'

module ClickHouse
  module Client
    class QueryBuilder < QueryLike
      attr_reader :table
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
        Arel::Nodes::Grouping
      ].freeze

      def initialize(table_name)
        @table = Arel::Table.new(table_name)
        @manager = Arel::SelectManager.new(Arel::Table.engine).from(@table).project(Arel.star)
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
          add_constraints_to(new_instance, constraints)
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

      def to_sql
        visitor = Arel::Visitors::ToSql.new(ClickHouse::Client::ArelEngine.new)
        visitor.accept(manager.ast, Arel::Collectors::SQLString.new).value
      end

      def to_redacted_sql(bind_index_manager = ClickHouse::Client::BindIndexManager.new)
        ClickHouse::Client::Redactor.redact(self, bind_index_manager)
      end

      def to_arel
        manager
      end

      private

      def validate_constraint_type!(constraint)
        return unless constraint.is_a?(Arel::Nodes::Node) && VALID_NODES.exclude?(constraint.class)

        raise ArgumentError, "Unsupported Arel node type for QueryBuilder: #{constraint.class.name}"
      end

      def add_constraints_to(instance, constraints)
        if constraints.is_a?(Arel::Nodes::Node)
          instance.manager.where(constraints)
        else
          constraints.each do |key, value|
            if value.is_a?(Hash)
              # Handle nested hash for joined tables
              join_table = Arel::Table.new(key)
              value.each do |nested_key, nested_value|
                build_arel_constraint(instance, join_table, nested_key, nested_value)
              end
            else
              build_arel_constraint(instance, instance.table, key, value)
            end
          end
        end
      end

      def build_arel_constraint(instance, table, key, value)
        constraint = if value.is_a?(Array)
                       table[key].in(value)
                     else
                       table[key].eq(value)
                     end

        instance.manager.where(constraint)
      end

      def validate_order_direction!(direction)
        return if %w[asc desc].include?(direction.to_s.downcase)

        raise ArgumentError, "Invalid order direction '#{direction}'. Must be :asc or :desc"
      end
    end
  end
end
