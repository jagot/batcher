require 'bunny'
require 'ruby-progressbar'

module Batcher
  def self.coord(n, lengths, variables)
    p = 1
    cum_lengths = lengths.reverse.map{ |x| p *= x }.reverse[1..-1]
    t = n
    c = lengths.each_with_index.map do |v,i|
      if i < cum_lengths.count
        cc = t/cum_lengths[i]
        t = t%cum_lengths[i]
        cc
      else
        t
      end
    end
    Hash[(0..c.count-1).map { |i| [variables[i], c[i]] }]
  end

  def self.tasker()
    app_config = YAML.load File.read './batcher_config.yml'
    sweep = app_config["sweep"]
    raise "Must specify one or more commands" unless sweep.has_key? "command"
    raise "Must specify directory pattern" unless sweep.has_key? "directory"
    raise "Must specify variables" unless sweep.has_key? "variables"
    command = sweep["command"]
    directory = sweep["directory"]
    variables = sweep["variables"].map do |v|
      v[0].to_sym
    end
    values = sweep["variables"].map do |v|
      if v[1].class == Hash
        v[1]["min"]..v[1]["max"]
      else
        v[1]
      end
    end
    lengths = values.map do |v|
      v.count
    end

    max_n = lengths.inject :*
    coords = (0..max_n-1).map do |n|
      coord n, lengths, variables
    end

    conn = Bunny.new
    conn.start

    ch   = conn.create_channel
    q    = ch.queue("batcher_queue", :durable => true)

    coords.each_with_index do |c,i|
      cdir = directory % c
      nc = c.merge({:directory => cdir, :command => command})

      msg  = nc.merge({:id => i})

      q.publish(Marshal.dump(msg), :persistent => true)
    end

    progressbar = ProgressBar.create(:total => max_n,
                                     :format => "[%c/%C] %e [%w]")

    status = q.status
    finished = 0
    while status[:message_count] > 0
      status = q.status
      f = max_n - status[:message_count]
      if f > finished
        progressbar.progress += f-finished
        finished = f
      end
      sleep 0.2
    end

    conn.close
  end
end
