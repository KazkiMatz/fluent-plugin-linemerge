# -*- coding: utf-8 -*-

module Fluent
  class LineMergeOutput < Output
    Fluent::Plugin.register_output('line_merge', self)

    def configure(conf)
      super

      @key = conf['key']
      @out_tag = conf['out_tag']
      @out_interval = conf['out_interval'].to_i

      @line_sets = {}
      @mutex = Mutex.new
    end

    def start
      super
      start_observer
    end

    def shutdown
      super
      if @observer
        @observer.terminate
        @observer.join
      end
    end

    def start_observer
      @observer = Thread.new(&method(:observe))
    end

    def observe
      loop {
        sleep @out_interval
        @mutex.synchronize {
          @line_sets.each {|tag, lines|
            next unless lines.length > 0

            new_tag = eval(@out_tag)
            merged_line = lines.join("\n")
            lines.clear
            Fluent::Engine.emit(new_tag, Fluent::Engine.now, {@key => merged_line})
          }
        }
      }
    end

    def emit(tag, es, chain)
      chain.next
      @mutex.synchronize {
        es.each {|time, record|
          @line_sets[tag] ||= []
          @line_sets[tag] << record[@key]
        }
      }
    end
  end
end
