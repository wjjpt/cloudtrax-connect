#!/usr/bin/env ruby

require 'json'
require 'kafka'
require 'geoutm'

$stdout.sync = true
@name = "cloudtrax2k"
@aphash = {}
@apconfig = {}
@interval = ENV['TIME_INTERVAL'].nil? ? 60 : ENV['TIME_INTERVAL'].to_i
kafka_hash = {  :kafka_broker => ENV['KAFKA_BROKER'].nil? ? "127.0.0.1" : ENV['KAFKA_BROKER'],
                :kafka_port => ENV['KAFKA_PORT'].nil? ? "9092" : ENV['KAFKA_PORT'],
                :kafka_topic => ENV['KAFKA_TOPIC'].nil? ? "cloudtrax" : ENV['KAFKA_TOPIC'],
                :kafka_topic_enrich => ENV['KAFKA_TOPIC_ENRICH'].nil? ? "cloudtrax_enrich" : ENV['KAFKA_TOPIC_ENRICH'],
                :kafka_topic_setup => ENV['KAFKA_TOPIC_SETUP'].nil? ? "cloudtrax_setup" : ENV['KAFKA_TOPIC_SETUP'],
                :kafka_topic_output => ENV['KAFKA_TOPIC_OUTPUT'].nil? ? "cloudtrax_RTLS" : ENV['KAFKA_TOPIC_OUTPUT'],
                :kafka_client_id => @name }

def ieminmaxloc(m)
    # m is a hash with mac client and list of AP with Xi,Yi location points in Lat&Lon
    # We need to translate it into UTM coordinates and then we can apply Extended Min-Max Location algorithm

    maxxminusd = nil
    maxyminusd = nil
    minxplusd = nil
    minyplusd = nil
    zone = nil
    valid = false

    m["aplist"].each do |ap|
        if ap["lat"].nil? or ap["lon"].nil?
            valid = false
        else
            valid = true
            utm = GeoUtm::LatLon.new(ap["lat"].to_f, ap["lon"].to_f).to_utm
            zone = utm.zone
            x = utm.e
            y = utm.n
            d = 0.42093*((ap["rssi"].to_f/-30.0)**6.9476)+0.54992
            if maxxminusd.nil?
                maxxminusd = x-d
            else
                maxxminusd = x-d if maxxminusd < (x-d)
            end
            if maxyminusd.nil?
                maxyminusd = y-d
            else
                maxyminusd = y-d if maxyminusd < (y-d)
            end
            if minxplusd.nil?
                minxplusd = x+d
            else
                minxplusd = x+d if minxplusd > (x+d)
            end
            if minyplusd.nil?
                minyplusd = y+d
            else
                minyplusd = y+d if minyplusd > (y+d)
            end
        end
    end
    if valid
        p1 = [ maxxminusd , minyplusd ]
        p2 = [ minxplusd , minyplusd ]
        p3 = [ minxplusd , maxyminusd ]
        p4 = [ maxxminusd , maxyminusd ]
        # calculing only E-MinMax algorithm
        utmx = ((p1[0]+p2[0])/2.0)
        utmy = ((p1[1]+p4[1])/2.0)
        latlon = GeoUtm::UTM.new(zone,utmx,utmy,GeoUtm::Ellipsoid::WGS84).to_lat_lon
        [latlon.lat.to_s,latlon.lon.to_s]
    else
        [nil,nil]
    end
end

def apconf(khash)
    kclient = Kafka.new(seed_brokers: ["#{khash[:kafka_broker]}:#{khash[:kafka_port]}"], client_id: khash[:kafka_client_id])
    puts "Subscribing to kafka topic #{khash[:kafka_topic_setup]}"
    kconsumer = kclient.consumer(group_id: "#{Time.now.to_i}")
    begin
        kconsumer.subscribe(khash[:kafka_topic_setup], start_from_beginning: true)
        kconsumer.each_message do |message|
            puts "Message: #{message.offset}, #{message.value}" unless ENV['DEBUG'].nil?
            m = JSON.parse(message.value)
            @apconfig[m["mac"].upcase] = m
        end
    rescue Exception => e
        puts "Exception: #{e.class}, message: #{e.message}"
        puts "Disconnecting from kafka server"
        kconsumer.stop
        puts "[#{@name}] Stopping cloudtrax_setup thread"
    end 
end

def trilat(khash)
    sleep 4
    kclient = Kafka.new(seed_brokers: ["#{khash[:kafka_broker]}:#{khash[:kafka_port]}"], client_id: khash[:kafka_client_id])
    puts "Subscribing to kafka topic #{khash[:kafka_topic_enrich]} and producing to #{khash[:kafka_topic_output]}"
    kconsumer = kclient.consumer(group_id: "#{Time.now.to_i}")
    begin
        kconsumer.subscribe(khash[:kafka_topic_enrich], start_from_beginning: false)
        kconsumer.each_message do |message|
            puts "Message from #{khash[:kafka_topic_enrich]} number #{message.offset}" unless ENV['DEBUG'].nil?
            m = JSON.parse(message.value)
            lat,lon = ieminmaxloc(m)
            if lat.nil? or lon.nil?
                next
            else
                kclient.deliver_message("#{{ "timestamp" => m["timestamp"], "mac" => m["mac"], "network_id" => m["network_id"], "maxrssi" => m["maxrssi"], "lat" => lat, "lon" => lon}.to_json}",topic: "#{khash[:kafka_topic_output]}")
            end
        end
    rescue Exception => e
        puts "Exception: #{e.class}, message: #{e.message}"
        puts "Disconnecting from kafka server"
        kconsumer.stop
        puts "[#{@name}] Stopping cloudtrax thread"
    end
end

def k2c(khash)
    sleep 1
    kclient = Kafka.new(seed_brokers: ["#{khash[:kafka_broker]}:#{khash[:kafka_port]}"], client_id: khash[:kafka_client_id])
    puts "Subscribing to kafka topic #{khash[:kafka_topic]}"
    kconsumer = kclient.consumer(group_id: khash[:kafka_client_id])
    begin
        kconsumer.subscribe(khash[:kafka_topic], start_from_beginning: false)
        kconsumer.each_message do |message|
            puts "Message from #{khash[:kafka_topic]} number #{message.offset}" unless ENV['DEBUG'].nil?
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
    sleep 2
    kclient = Kafka.new(seed_brokers: ["#{khash[:kafka_broker]}:#{khash[:kafka_port]}"], client_id: khash[:kafka_client_id])
    puts "Producing to kafka topic #{khash[:kafka_topic_enrich]}"
    while true
        begin
            if @aphash.empty?
                sleep 1
                next
            end
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
                    unless @apconfig[ap.upcase].nil?
                        lat = @apconfig[ap.upcase]["lat"]
                        lon = @apconfig[ap.upcase]["lon"]
                    else
                        lat = nil
                        lon = nil
                    end
                    if clienthash[client["mac"]].nil?
                        # first time
                        clienthash[client["mac"]] = [{ "ap" => ap,
                                                       "lat" => lat,
                                                       "lon" => lon,
                                                       "network_id" => @aphash[ap]["network_id"],
                                                       "last_seen" => client["last_seen"],
                                                       "rssi" =>  client["last_seen_signal"] }]
                    else
                        clienthash[client["mac"]] << { "ap" => ap,
                                                       "lat" => lat,
                                                       "lon" => lon,
                                                       "network_id" => @aphash[ap]["network_id"],
                                                       "last_seen" => client["last_seen"],
                                                       "rssi" =>  client["last_seen_signal"] }
                    end
                end
            end
            mytime = Time.now.to_i
            clienthash.each_key do |client|
                #puts "#{{ "timestamp" => mytime, "mac" => client, "network_id" => clientnetid[client]["network_id"], "maxrssi" => clientnetid[client]["rssi"], "aplist" => clienthash[client] }.to_json}" unless ENV['DEBUG'].nil?
                kclient.deliver_message("#{{ "timestamp" => mytime, "mac" => client, "network_id" => clientnetid[client]["network_id"], "maxrssi" => clientnetid[client]["rssi"], "aplist" => clienthash[client] }.to_json}",topic: "#{khash[:kafka_topic_enrich]}")
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
        t4 = Thread.new{trilat(kafka_hash)}
        t1.join
        t2.join
        t3.join
        t4.join
    end
end

puts "Exiting from cloudtrax"

## vim:ts=4:sw=4:expandtab:ai:nowrap:formatoptions=croqln:
