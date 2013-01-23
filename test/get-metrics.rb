require './helper'

class GetMetricsTest < L2metTest

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

end
