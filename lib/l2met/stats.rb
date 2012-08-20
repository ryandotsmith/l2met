module L2met
  module Stats
    extend self

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
  end
end
