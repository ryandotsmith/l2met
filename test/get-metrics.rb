require './helper'
require 'time'

class GetMetricsTest < L2metTest

=begin
  def test_l2met_retains_data
    num_vals = 50
    t = Time.now
    # Make sure we have enough time to work with the same bucket.
    t = Time.now until t.sec < 55
    bucket = (t.to_i * 60) / 60
    num_vals.times do
      out = `echo 'measure="l2met-simple-test"' | log-shuttle -batch-size=1`
    end
    # Wait for l2met to flush bucket.
    sleep(1)
    metrics = get('metrics?limit=1&resolution=1')
    assert_equal(1, metrics.length)
    metric = metrics.first
    assert_equal('l2met-simple-test', metric["name"])
    assert_equal(1, metric["mean"])
  end
=end

  def test_l2met_groups_data_with_resolution
    num_vals = 1
    stop = Time.now
    curr = stop - (60*60) # start at 1 hour ago
    while curr < stop
      num_vals.times do
        d = 'measure="l2met-simple-test"'
        msg = "<0>0 #{Time.at(curr).iso8601} 1234 token shuttle - - #{d}"
        msg = "#{msg.length} #{msg}"
        puts msg
        out = `echo '#{msg}' | log-shuttle -skip-headers=true -batch-size=1`
      end
      curr += 60
    end
    # Wait for l2met to flush bucket.
    sleep(1)
    metrics = get('metrics?limit=1&resolution=1')
    assert_equal(1, metrics.length)
    metric = metrics.first
    assert_equal('l2met-simple-test', metric["name"])
    assert_equal(1, metric["mean"])
  end

end
