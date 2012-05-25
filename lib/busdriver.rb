require "redis"
require "json"
require "securerandom"
require "press"

module Busdriver
  extend Press

  TIMEOUT  = 3

  def self.timeout=(timeout)
    @timeout = timeout
  end

  def self.timeout
    @timeout ||= ENV['BUSDRIVER_TIMEOUT'] || TIMEOUT
  end

  TIME_TO_LIVE = 30

  def self.time_to_live=(time_to_live)
    @time_to_live = time_to_live
  end

  def self.time_to_live
    @time_to_live ||= ENV['BUSDRIVER_TIME_TO_LIVE'] || TIME_TO_LIVE
  end

  TIME_TO_EXPIRE = 90

  def self.time_to_expire=(time_to_expire)
    @time_to_expire = time_to_expire
  end

  def self.time_to_expire
    @time_to_expire ||= ENV['BUSDRIVER_TIME_TO_EXPIRE'] || TIME_TO_EXPIRE
  end

  def self.urls=(urls)
    @urls = urls
  end

  def self.urls
    @urls ||= ENV['BUSDRIVER_URLS'].split(",")
  end

  def self.url=(url)
    @url = url
  end

  def self.url
    @url ||= urls[ENV['BUSDRIVER_ZONE'].ord % urls.size]
  end

  def self.connect(url)
    Redis.connect(url: url, timeout: timeout)
  end

  def self.conns
    @conns ||= urls.map { |url| connect(url) }
  end

  def self.conn
    @conn ||= connect(url)
  end

  def self.each(&blk)
    conns.shuffle.each do |conn|
      begin
        yield conn
      rescue => e
        pdfme __FILE__, __method__, e, host: conn.client.host
      end
    end
  end

  def self.header_format
    { message_id: SecureRandom.uuid, published_on: Time.now.to_i, ttl: time_to_live }
  end

  def self.publish(key, data)
    header = header_format
    payload_json = JSON.dump(header: header, payload: data)
    pdfm __FILE__, __method__, header, key: key
    each do |conn|
      conn.rpush(key, payload_json)
      conn.expire(key, time_to_expire) rescue nil
      pdfm __FILE__, __method__, at: "published", key: key
      break
    end
  end

  def self.subscribe(keys, &blk)
    while true
      begin
        key, payload_json = conn.blpop(*keys, 1)
        if payload_json
          payload = JSON.parse(payload_json)
          header, data = payload.values_at("header", "payload")
          published_on, ttl = header.values_at("published_on", "ttl")
          pdfm __FILE__, __method__, header, key: key
          if Time.now.to_i - published_on.to_i > ttl
            pdfm __FILE__, __method__, header, at: "timeout", key: key
          else
            begin
              pdfm __FILE__, __method__, header, at: "received", key: key
              yield key, data
              pdfm __FILE__, __method__, header, at: "processed", key: key
            rescue => e
              pdfme __FILE__, __method__, e
            end
          end
        end
      rescue => e
        pdfme __FILE__, __method__, e, host: conn.client.host
        raise e
      end
    end
  end

  def self.publishz(zone, key, data)
    header = header_format.merge(zone: zone)
    payload_json = JSON.dump(header: header, payload: data)
    pdfm __FILE__, __method__, header, key: key
    conns[zone.ord % urls.size].rpush(key, payload_json)
    conns[zone.ord % urls.size].expire(key, time_to_expire) rescue nil
    pdfm __FILE__, __method__, at: "published", key: key
  rescue => e
    pdfme __FILE__, __method__, e
  end

  def self.subscribez(keys, &blk)
    while true
      begin
        key, payload_json = conn.blpop(*keys, 1)
        if payload_json
          payload = JSON.parse(payload_json)
          header, data = payload.values_at("header", "payload")
          published_on, ttl, zone = header.values_at("published_on", "ttl", "zone")
          pdfm __FILE__, __method__, header, key: key
          if Time.now.to_i - published_on.to_i > ttl
            pdfm __FILE__, __method__, header, at: "timeout", key: key
          else
            begin
              pdfm __FILE__, __method__, header, at: "received", key: key
              yield zone, key, data
              pdfm __FILE__, __method__, header, at: "processed", key: key
            rescue => e
              pdfme __FILE__, __method__, e
            end
          end
        end
      rescue => e
        pdfme __FILE__, __method__, e, host: conn.client.host
        raise e
      end
    end
  end

  def self.counts(pattern)
    llen, llens = 0, Hash.new(0)
    each do |conn|
      conn.keys(pattern).each do |key|
        len = conn.llen(key)
        llen += len
        llens[key] += len
      end
    end
    pdfm __FILE__, __method__, llens, length: llen
  end

  def self.drain(pattern)
    each do |conn|
      conn.keys(pattern).each do |key|
        conn.del(key)
        pdfm __FILE__, __method__, key: key
      end
    end
  end
end
