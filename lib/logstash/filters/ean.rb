# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# This filter let's you create a ean based on various parts
# of the logstash event.
# This can be useful for deduplication of messages or simply to provide
# a custom unique identifier.
#
# This is VERY experimental and is largely a proof-of-concept
class LogStash::Filters::Ean < LogStash::Filters::Base

  # A list of keys to use in creating the string to ean
  # Keys will be sorted before building the string
  # keys and values will then be concatenated with pipe delimeters
  # and eanmed
  config :key, :validate => :string, :default => "message"


  public
  def register
    @to_ean = ""
  end

  public
  def filter(event)
    return unless filter?(event)

    @logger.debug("Running ean filter", :event => event)

    @logger.debug("Adding key to string", :current_key => @key)
    @to_ean << "#{event[@key]}"

    @logger.debug("Final string built", :to_ean => @to_ean)

    digested_string = eanize(@to_ean)

    @logger.debug("Digested string", :digested_string => digested_string)

    event[k] = digested_string
  end

  public
  def eanize(twelve)

    return "" unless twelve.length == 12 && twelve.match(/\d{11}/)

    arr = (0..11).to_a.collect do |i|
      if (i+1).even?
        twelve[i,1].to_i * 3
      else
        twelve[i,1].to_i
      end
    end

    sum = arr.inject { |sum, n| sum + n }

    remainder = sum % 10

    if remainder == 0
      check = 0
    else
      check = 10 - remainder
    end

    twelve + check.to_s

  end

end # class LogStash::Filters::Checksum
