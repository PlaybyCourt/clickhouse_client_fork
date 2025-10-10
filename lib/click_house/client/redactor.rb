# frozen_string_literal: true

module ClickHouse
  module Client
    module Redactor
      class << self
        # Redacts the SQL query represented by the query builder.
        #
        # @param query_builder [::ClickHouse::Querybuilder] The query builder object to be redacted.
        # @return [String] The redacted SQL query as a string.
        # @raise [ArgumentError] when the condition in the query is of an unsupported type.
        #
        # Example:
        #   query_builder = ClickHouse::QueryBuilder.new('users').where(name: 'John Doe')
        #   redacted_query = ClickHouse::Redactor.redact(query_builder)
        #   # The redacted_query will contain the SQL query with values replaced by placeholders.
        #   output: "SELECT * FROM \"users\" WHERE \"users\".\"name\" = $1"
        def redact(query_builder, bind_manager = ClickHouse::Client::BindIndexManager.new)
          redacted_constraints = query_builder.manager.constraints.map do |constraint|
            redact_constraint(constraint, bind_manager)
          end

          cloned_query_builder = query_builder.clone

          cloned_query_builder.manager.constraints.clear
          redacted_constraints.each do |constraint|
            cloned_query_builder.manager.where(constraint)
          end

          cloned_query_builder.to_sql
        end

        private

        def redact_constraint(constraint, bind_manager)
          case constraint
          when Arel::Nodes::In
            if constraint.right.is_a? Arel::Nodes::SelectStatement
              constraint.left.in(redact_select_statement(constraint.right, bind_manager))
            else
              constraint.left.in(Array.new(constraint.right.size) { Arel.sql(bind_manager.next_bind_str) })
            end
          when Arel::Nodes::Equality
            constraint.left.eq(Arel.sql(bind_manager.next_bind_str))
          when Arel::Nodes::LessThan
            constraint.left.lt(Arel.sql(bind_manager.next_bind_str))
          when Arel::Nodes::LessThanOrEqual
            constraint.left.lteq(Arel.sql(bind_manager.next_bind_str))
          when Arel::Nodes::GreaterThan
            constraint.left.gt(Arel.sql(bind_manager.next_bind_str))
          when Arel::Nodes::GreaterThanOrEqual
            constraint.left.gteq(Arel.sql(bind_manager.next_bind_str))
          when Arel::Nodes::NamedFunction
            redact_named_function(constraint, bind_manager)
          when Arel::Nodes::Matches
            constraint.left.matches(Arel.sql(bind_manager.next_bind_str), constraint.escape, constraint.case_sensitive)
          else
            raise ArgumentError, "Unsupported Arel node type for Redactor: #{constraint.class}"
          end
        end

        def redact_named_function(constraint, bind_manager)
          redacted_constraint =
            Arel::Nodes::NamedFunction.new(constraint.name, constraint.expressions.dup)

          case redacted_constraint.name
          when 'startsWith'
            redacted_constraint.expressions[1] = Arel.sql(bind_manager.next_bind_str)
          else
            redacted_constraint.expressions = redacted_constraint.expressions.map do
              Arel.sql(bind_manager.next_bind_str)
            end
          end

          redacted_constraint
        end

        def redact_select_statement(select_statement, bind_manager)
          cloned_statement = select_statement.clone
          cloned_statement.cores.map! do |select_core|
            select_core.wheres = select_core.wheres.map do |where|
              redact_constraint(where, bind_manager)
            end

            select_core
          end

          cloned_statement
        end
      end
    end
  end
end
