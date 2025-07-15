# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ClickHouse::Client::Quoting do
  describe '.quote' do
    context 'with a String' do
      it 'wraps the string in single quotes and escapes single quotes' do
        expect(described_class.quote('test')).to eq("'test'")
        expect(described_class.quote("test's")).to eq("'test''s'")
      end

      it 'escapes backslashes' do
        expect(described_class.quote('\\')).to eq("'\\\\'")
      end
    end

    context 'with nil' do
      it 'returns NULL' do
        expect(described_class.quote(nil)).to eq('NULL')
      end
    end

    context 'with symbol' do
      it 'wraps the string in quotes' do
        expect(described_class.quote(:foo)).to eq("'foo'")
      end
    end

    context 'with numeric' do
      it 'returns string' do
        expect(described_class.quote(1)).to eq('1')
        expect(described_class.quote(1.2)).to eq('1.2')
      end
    end

    context 'with date' do
      it 'returns string' do
        expect(described_class.quote(Date.new(2022, 4, 5))).to eq("'2022-04-05'")
      end
    end

    context 'with array' do
      it 'wraps the elements in square brackets and quote the elements' do
        expect(described_class.quote(['test'])).to eq("['test']")
        expect(described_class.quote(['test', nil, 1])).to eq("['test',NULL,1]")
      end
    end
  end
end
