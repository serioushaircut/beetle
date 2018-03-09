# attempts_with_dlx_and_per_message_ttl.rb
# ! check the examples/README.rdoc for information on starting your redis/rabbit !
#
# start it with `ruby attempts_with_dlx_and_per_message_ttl.rb`

require "rubygems"
require File.expand_path("../lib/beetle", File.dirname(__FILE__))

# set Beetle log level to info, less noisy than debug
Beetle.config.logger.level = Logger::INFO

# setup client with dead lettering enabled
config = Beetle::Configuration.new
config.dead_lettering_enabled = true
config.dead_lettering_msg_ttl = 8000 # millis - works as 'global/queue-bases' message-ttl

client = Beetle::Client.new(config)
client.register_queue(:test)
client.register_message(:test)
client.register_message(:test_dead_letter) # to allow for directly publishing to this queue (just proof of concept)

# purge the test queue
client.purge(:test)
# empty the dedup store
client.deduplication_store.flushdb

# setup our counter
$completed = 0
$expected = 2
# store the start time
$start_time = Time.now.to_f

# declare a handler class for message processing
class Handler < Beetle::Handler
  def process
    logger.info "Processed after #{Time.now - $start_time}s: #{message.data}"
    $completed += 1
  end
end
client.register_handler(:test, Handler)

# publish messages directly into the queue whose dlx setup points backwards to the target queue 'test'
client.publish(:test_dead_letter, 'early', expiration: 4000)  # processed after 4 seconds
client.publish(:test_dead_letter, 'later', expiration: 12000) # processed after 8 seconds (global/queue based expiration)
puts "published 2 test messages"

# start the listening loop
client.listen do
  # catch INT-signal and stop listening
  trap("INT") { client.stop_listening }
  # we're adding a periodic timer to check whether all 10 messages have been processed without exceptions
  timer = EM.add_periodic_timer(1) do
    if $completed == $expected
      timer.cancel
      client.stop_listening
    end
  end
end

puts "Handled #{$completed} messages"
if $completed != $expected
  raise "Did not handle the correct number of messages"
end
