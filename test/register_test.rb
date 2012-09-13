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
    m = Utils.trunc_time(t)
    mkey = 1154433106
    expected = [{"counter"=>{mkey => {:source=>"web.1", :consumer=>"123", :time=>Time.at(t), :type=>"counter", :value=>1}}}]
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
    Register.accept('my-metric', 99,
      source: 'web.1',
      consumer: '123',
      time: t,
      type: 'last')
    m = Utils.trunc_time(t)
    mkey = 1154433106
    expected = []
    expected << {"counter"=>{mkey => {:source=>"web.1", :consumer=>"123", :time=>Time.at(t), :type=>"counter", :value=>1}}}
    expected <<  {"last"=>{mkey =>{:source=>"web.1", :consumer=>"123", :time=> Time.at(t), :type=>"last", :value=>99}}}
    assert_equal(expected, Register.snapshot!(m))
  end

end
