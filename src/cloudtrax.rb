#!/usr/bin/env ruby

require 'json'
require 'kafka'

$stdout.sync = true
@name = "cloudtrax2k"
@aphash = {}
@apconfig = {}
@interval = ENV['TIME_INTERVAL'].nil? ? 60 : ENV['TIME_INTERVAL'].to_i
kafka_hash = {  :kafka_broker => ENV['KAFKA_BROKER'].nil? ? "127.0.0.1" : ENV['KAFKA_BROKER'],
                :kafka_port => ENV['KAFKA_PORT'].nil? ? "9092" : ENV['KAFKA_PORT'],
                :kafka_topic => ENV['KAFKA_TOPIC'].nil? ? "cloudtrax" : ENV['KAFKA_TOPIC'],
                :kafka_topic_setup => ENV['KAFKA_TOPIC_SETUP'].nil? ? "cloudtrax_setup" : ENV['KAFKA_TOPIC_SETUP'],
                :kafka_topic_output => ENV['KAFKA_TOPIC_OUTPUT'].nil? ? "cloudtrax_RTLS" : ENV['KAFKA_TOPIC_OUTPUT'],
                :kafka_client_id => @name }

def j2j(event)
    rssi = event["rssi"].to_i
    event
end

def apconf(khash)
    kclient = Kafka.new(seed_brokers: ["#{khash[:kafka_broker]}:#{khash[:kafka_port]}"], client_id: khash[:kafka_client_id])
    kconsumer = kclient.consumer(group_id: khash[:kafka_client_id])
    begin
        kconsumer.subscribe(khash[:kafka_topic_setup], start_from_beginning: true)
        kconsumer.each_message do |message|
            puts "Message: #{message.offset}, #{message.value}" unless ENV['DEBUG'].nil?
            m = JSON.parse(message.value)
            @apconfig[m["mac"]] = m
        end
    rescue Exception => e
        puts "Exception: #{e.class}, message: #{e.message}"
        puts "Disconnecting from kafka server"
        kconsumer.stop
        puts "[#{@name}] Stopping cloudtrax_setup thread"
    end 
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
            @aphash[m["node_mac"]] = { "network_id" => m["network_id"],"probe_requests" => m["probe_requests"]}
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
            clientnetid = {}
            @aphash.each_key do |ap|
                @aphash[ap]["probe_requests"].each do |client|
                    if clientnetid[client["mac"]].nil?
                        # first time
                        clientnetid[client["mac"]] = { "network_id" => @aphash[ap]["network_id"], "rssi" => client["last_seen_signal"] }
                    else
                        # select greater rssi value (closest AP)
                        if clientnetid[client["mac"]]["rssi"] < client["last_seen_signal"]
                            clientnetid[client["mac"]] = { "network_id" => @aphash[ap]["network_id"], "rssi" => client["last_seen_signal"] }
                        end
                    end
                    if clienthash[client["mac"]].nil?
                        # first time
                        clienthash[client["mac"]] = [{ "ap" => ap,
                                                       "lat" => @apconfig[ap]["lat"],
                                                       "lon" => @apconfig[ap]["lon"],
                                                       "network_id" => @aphash[ap]["network_id"],
                                                       "last_seen" => client["last_seen"],
                                                       "rssi" =>  client["last_seen_signal"] }]
                    else
                        clienthash[client["mac"]] << { "ap" => ap,
                                                       "lat" => @apconfig[ap]["lat"],
                                                       "lon" => @apconfig[ap]["lon"],
                                                       "network_id" => @aphash[ap]["network_id"],
                                                       "last_seen" => client["last_seen"],
                                                       "rssi" =>  client["last_seen_signal"] }
                    end
                end
            end
            mytime = Time.now.to_i
            clienthash.each_key do |client|
                kclient.deliver_message("#{{ "timestamp" => mytime, "mac" => client, "network_id" => clientnetid[client]["network_id"], "maxrssi" => clientnetid[client]["rssi"], "aplist" => clienthash[client] }.to_json}",topic: "#{khash[:kafka_topic_output]}")
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
        t3 = Thread.new{apconf(kafka_hash)}
        t1.join
        t2.join
        t3.join
    end
end

puts "Exiting from cloudtrax"

## vim:ts=4:sw=4:expandtab:ai:nowrap:formatoptions=croqln:
