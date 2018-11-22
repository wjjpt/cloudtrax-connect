#!/usr/bin/env ruby

require 'json'
require 'kafka'

$stdout.sync = true
@name = "cloudtrax2k"
@aphash = {}
@interval = ENV['TIME_INTERVAL'].nil? ? 60 : ENV['TIME_INTERVAL'].to_i
kafka_hash = {  :kafka_broker => ENV['KAFKA_BROKER'].nil? ? "127.0.0.1" : ENV['KAFKA_BROKER'],
                :kafka_port => ENV['KAFKA_PORT'].nil? ? "9092" : ENV['KAFKA_PORT'],
                :kafka_topic => ENV['KAFKA_TOPIC'].nil? ? "cloudtrax" : ENV['KAFKA_TOPIC'],
                :kafka_topic_output => ENV['KAFKA_TOPIC_OUTPUT'].nil? ? "cloudtrax_RTLS" : ENV['KAFKA_TOPIC_OUTPUT'],
                :kafka_client_id => @name }

def j2j(event)
    rssi = event["rssi"].to_i
    event
end

def k2c(khash)
    kclient = Kafka.new(seed_brokers: ["#{khash[:kafka_broker]}:#{khash[:kafka_port]}"], client_id: khash[:kafka_client_id])
    puts "Subscribing to kafka topic #{khash[:kafka_topic]}"
    kconsumer = kclient.consumer(group_id: khash[:kafka_client_id])
    begin
        kconsumer.subscribe(khash[:kafka_topic], start_from_beginning: false)
        
        kconsumer.each_message do |message|
            puts "Message: #{message.offset}, #{message.value}" unless ENV['DEBUG'].nil?
            m = JSON.parse(message.value)
            @aphash[m["node_mac"]] = m["probe_requests"]
        end

    rescue Exception => e
        puts "Exception: #{e.class}, message: #{e.message}"
        puts "Disconnecting from kafka server"
        kconsumer.stop
        puts "[#{@name}] Stopping cloudtrax thread"
    end 
end

def c2k(khash)
    sleep 1
    kclient = Kafka.new(seed_brokers: ["#{khash[:kafka_broker]}:#{khash[:kafka_port]}"], client_id: khash[:kafka_client_id])
    puts "Producing to kafka topic #{khash[:kafka_topic_output]}"
    while true
        begin
            next if @aphash.empty?
            clienthash = {}
            @aphash.each_key do |ap|
                @aphash[ap].each do |client|
                    if clienthash[client["mac"]].nil?
                        # first time
                        clienthash[client["mac"]] = [{ "ap" => ap,
                                                       "last_seen" => client["last_seen"],
                                                       "rssi" =>  client["last_seen_signal"] }]
                    else
                        clienthash[client["mac"]] << { "ap" => ap,
                                                       "last_seen" => client["last_seen"],
                                                       "rssi" =>  client["last_seen_signal"] }
                    end
                end
            end
            clienthash.each_key do |client|
                kclient.deliver_message("#{{ "mac" => client, "aplist" => clienthash[client] }.to_json}",topic: "#{khash[:kafka_topic_output]}")
            end
            sleep @interval
        rescue Exception => e
            puts "Exception: #{e.class}, message: #{e.message}"
        end
    end
end

Signal.trap('INT') { throw :sigint }

catch :sigint do
    # running forever
    while true
        puts "[#{@name}] Starting cloudtrax threads"
        t1 = Thread.new{k2c(kafka_hash)}
        t2 = Thread.new{c2k(kafka_hash)}
        t1.join
        t2.join
    end
end

puts "Exiting from cloudtrax"

## vim:ts=4:sw=4:expandtab:ai:nowrap:formatoptions=croqln:
