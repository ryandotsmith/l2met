require 'l2met/register'
require 'l2met/utils'

class RegisterTest < MiniTest::Unit::TestCase
  include L2met

  def test_accept
    t = Time.now
    Register.start(0) and sleep(0.1)
    Register.accept('my-metric', 1,
      source: 'web.1',
      consumer: '123',
      time: t,
      type: 'counter')

    expected = {1154433106 => {:name=>"my-metric", :source=>"web.1", :consumer=>"123", :time=>Time.at(t), :type=>"counter", :value=>1}}
    m = Utils.trunc_time(t)
    assert_equal(expected, Register.snapshot!(m))
  end

  def test_accept_multiple_types
    t = Time.now
    Register.start(0) and sleep(0.1)
    Register.accept('my-metric', 1,
      source: 'web.1',
      consumer: '123',
      time: t,
      type: 'counter')
    Register.accept('my-metric2', 99,
      source: 'web.1',
      consumer: '123',
      time: t,
      type: 'last')

    expected = {}
    expected[1154433106] = {:name=>"my-metric", :source=>"web.1", :consumer=>"123", :time=> Time.at(t), :type=>"counter", :value=>1}
    expected[112753359] = {:name=>"my-metric2", :source=>"web.1", :consumer=>"123", :time=> Time.at(t), :type=>"last", :value=>99}
    m = Utils.trunc_time(t)
    assert_equal(expected, Register.snapshot!(m))
  end

end
