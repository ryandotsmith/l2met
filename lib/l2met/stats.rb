module L2met
  module Stats
    extend self

    def aggregate(vals)
      {mean: mean(vals.map {|h| h["mean"]}),
        median: median(vals.map {|h| h["median"]}),
        min: min(vals.map {|h| h["min"]}),
        max: max(vals.map {|h| h["max"]}),
        perc95: perc95(vals.map {|h| h["perc95"]}),
        perc99: perc99(vals.map {|h| h["perc99"]}),
        last: last(vals.map {|h| h["last"]})}
    end

    def all(vals)
      {min: min(vals),
        max: max(vals),
        mean: mean(vals),
        median: median(vals),
        perc95: perc95(vals),
        perc99: perc99(vals),
        last: last(vals)}
    end

    def mean(vals)
      vals.reduce(:+) / vals.length.to_f
    end

    def median(vals)
      vals[vals.length/2]
    end

    def min(vals)
      vals.sort[0]
    end

    def max(vals)
      vals.sort[-1]
    end

    def perc95(vals)
      vals[(0.95 * vals.length).ceil - 1]
    end

    def perc99(vals)
      vals[(0.99 * vals.length).ceil - 1]
    end

    def last(vals)
      vals.last
    end

  end
end
