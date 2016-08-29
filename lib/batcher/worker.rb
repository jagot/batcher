require 'bunny'

module Batcher
  def self.run_cmd(cmd, options)
    puts cmd % options
    sleep 0.2
  end

  def self.worker(options)
    conn = Bunny.new
    conn.start

    ch   = conn.create_channel
    q    = ch.queue("batcher_queue", :durable => true)

    ch.prefetch(1)
    puts " [*] Waiting for tasks. To exit press CTRL+C"

    begin
      q.subscribe(:manual_ack => true, :block => true) do |delivery_info, properties, body|
        data = Marshal.load(body)
        data.merge! options
        command = data[:command]
        if command.class == Array
          command.each do |cmd|
            run_cmd cmd, data
          end
        else
          run_cmd command, data
        end
        puts
        ch.ack(delivery_info.delivery_tag)
      end
    rescue Interrupt => _
      conn.close
    end
  end
end
