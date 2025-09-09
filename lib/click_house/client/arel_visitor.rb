# frozen_string_literal: true

module ClickHouse
  module Client
    class ArelVisitor < Arel::Visitors::ToSql
      private

      # rubocop:disable Naming/MethodName -- parent method calls in this format
      def visit_Arel_Nodes_Matches(object, collector)
        op = object.case_sensitive ? " LIKE " : " ILIKE "
        collector = infix_value object, collector, op
        if object.escape
          collector << " ESCAPE "
          visit object.escape, collector
        else
          collector
        end
      end

      def visit_Arel_Nodes_DoesNotMatch(object, collector)
        op = object.case_sensitive ? " NOT LIKE " : " NOT ILIKE "
        collector = infix_value object, collector, op
        if object.escape
          collector << " ESCAPE "
          visit object.escape, collector
        else
          collector
        end
      end

      # rubocop:enable Naming/MethodName
    end
  end
end
