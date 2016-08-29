require 'bunny'
require 'ruby-progressbar'

module Batcher
  def self.coord(n, lengths, variables, values)
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
    Hash[(0..c.count-1).map { |i| [variables[i], values[i][c[i]]] }]
  end

  def self.load_marked_runs()
    if File.exists? "runs.dat"
      Marshal.load File.read("runs.dat")
    else
      []
    end
  end

  def self.mark_run(id)
    runs = load_marked_runs
    runs << id
    File.open("runs.dat","w") { |file| file.write(Marshal.dump(runs)) }
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
        (v[1]["min"]..v[1]["max"]).to_a
      else
        v[1]
      end
    end
    lengths = values.map do |v|
      v.count
    end

    max_n = lengths.inject :*
    coords = (0..max_n-1).map do |n|
      coord n, lengths, variables, values
    end

    conn = Bunny.new
    conn.start

    ch   = conn.create_channel
    q    = ch.queue("batcher_queue", :durable => true)
    q.purge
    qf = ch.queue("batcher_finished", :durable => true)
    qf.purge

    ntasks = 0
    finished_runs = load_marked_runs
    coords.each_with_index do |c,i|
      next if finished_runs.include? i
      cdir = directory % c
      nc = c.merge({:directory => cdir, :command => command})

      msg  = nc.merge({:id => i})

      q.publish(Marshal.dump(msg), :persistent => true)
      ntasks += 1
    end

    progressbar = ProgressBar.create(:total => max_n,
                                     :starting_at => max_n - ntasks,
                                     :format => "[%c/%C] %e [%w%i]")

    begin
      qf.subscribe(:manual_ack => true, :block => true) do |delivery_info, properties, body|
        data = Marshal.load(body)
        mark_run data[:id] if data[:success]
        progressbar.increment
        ch.ack(delivery_info.delivery_tag)
      end
    rescue Interrupt => _
      conn.close
    end

    conn.close
  end
end
