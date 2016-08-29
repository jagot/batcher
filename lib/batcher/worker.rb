require 'bunny'
require 'pty'

module Batcher
  def self.run_cmd(cmd, options)
    command = cmd % options
    puts command
    master, slave = PTY.open
    read = IO.pipe[0]
    begin
      pid = spawn(command, :in=>read, :out=>slave,
                  :err=>[:child, :out])
      read.close     # we dont need the read
      slave.close    # or the slave
      begin
        while true
          print master.gets "\r"
        end
      rescue Errno::EIO
        status = PTY.check(pid)
        puts
        return status == 0
      end
    rescue PTY::ChildExited
      puts "The child process exited!"
    end
    false
  end

  def self.worker(options)
    conn = Bunny.new
    conn.start

    ch   = conn.create_channel
    q    = ch.queue("batcher_queue", :durable => true)
    qf   = ch.queue("batcher_finished", :durable => true)

    ch.prefetch(1)
    puts " [*] Waiting for tasks. To exit press CTRL+C"

    begin
      q.subscribe(:manual_ack => true, :block => true) do |delivery_info, properties, body|
        data = Marshal.load(body)
        data.merge! options
        command = data[:command]
        status = true
        if command.class == Array
          command.each do |cmd|
            status &= run_cmd cmd, data
            break unless status
          end
        else
          status = run_cmd command, data
        end
        puts
        ch.ack(delivery_info.delivery_tag)
        qf.publish(Marshal.dump({:id => data[:id], :success => status}), :persistent => true)
      end
    rescue Interrupt => _
      conn.close
    end
  end
end
