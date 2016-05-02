# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "mixpanel_client"
require "date"
require "time"

class LogStash::Inputs::Mixpanel < LogStash::Inputs::Base
  config_name "mixpanel"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  #
  config :engage, validate: :boolean, required: false, default: false

  # The API key of the project
  config :api_key, validate: :string, required: true

  # The Secret of the project
  config :api_secret, validate: :string, required: true

  # from_date to_date
  config :from_date, validate: :string, required: false, default: (Date.today - 1).to_s
  config :to_date, validate: :string, required: false, default: (Date.today).to_s

  public
  def register
    @client = Mixpanel::Client.new(api_key: @api_key, api_secret: @api_secret)
  end

  # def register

  def run(queue)
    fetch(queue)
  end

  # def run

  private

  def fetch(queue)
    if (@engage)
      fetch_engage(queue)
    else
      fetch_export(queue)
    end
  end

  def fetch_export(queue)
    @client
        .request("export", from_date: @from_date, to_date: @to_date)
        .each do |raw_event|
      event = LogStash::Event.new raw_event
      event["@timestamp"] = LogStash::Timestamp.at(event["properties"]["time"])
      decorate(event)
      queue << event
    end
  end

  def fetch_engage(queue)
    this_page = fetch_engage_page(queue)

    while (this_page and this_page['results'].size > 0)
      this_page = fetch_engage_page(queue, this_page['page'].to_i + 1, this_page['session_id'])
    end
  end

  def fetch_engage_page(queue, page_nr = 0, session_id = nil)
    data = nil
    if (session_id)
      data = @client.request("engage", page: page_nr, session_id: session_id)
    else
      data = @client.request("engage", page: page_nr)
    end
    data['results'].each do |raw_event|
      event = LogStash::Event.new raw_event
      decorate(event)
      queue << event
    end
    data
  end

end # class LogStash::Inputs::Example
