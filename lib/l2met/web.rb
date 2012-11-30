require 'sinatra/base'
require 'sinatra/google-auth'
require 'rack/handler/mongrel'
require 'rack/ssl-enforcer'
require 'scrolls'
require 'l2met/config'
require 'l2met/db'
require 'l2met/parser'
require 'l2met/utils'
require 'l2met/consumer'

module L2met
  class Web < Sinatra::Base
    use Rack::SslEnforcer
    register Sinatra::GoogleAuth
    set :public_folder, "./public"
    set :views, "./templates"

    def self.route(verb, action, *)
      condition {@instrument_action = action}
      super
    end

    before do
      content_type(:json)
      @start_request = Time.now
      @current_user = session[:user]
    end

    after do
      Heartbeat.pulse(@instrument_action)
    end

    error do
      e = env['sinatra.error']
      log({level: "error", exception: e.message}.merge(params))
      [500, Utils.enc_j(msg: "un-handled error")]
    end

    not_found do
      Utils.count(1, 'web.not-found')
      [404, Utils.enc_j(msg: "endpoint not found")]
    end

    head "/" do
      200
    end

    get "/heartbeat" do
      [200, Utils.enc_j(alive: Time.now)]
    end

    get "/" do
      authenticate
      content_type(:html)
      erb(:index)
    end

    put "/consumers" do
      authenticate
      [201, Utils.enc_j(Consumer.put(session[:user], params))]
    end

    get "/consumers" do
      authenticate
      [200, Utils.enc_j(Consumer.all(session[:user]))]
    end

    get "/consumers/:id" do
      authenticate
      [200, Utils.enc_j(Consumer.get(params[:id]))]
    end

    post "/consumers/:cid/logs" do
      Utils.count(1, 'web.post')
      Parser.unpack(params[:cid], request.env["rack.input"].read)
      [201, Utils.enc_j(msg: "OK")]
    end

    post "/beta/consumers/:cid/logs" do
      Utils.count(1, 'beta.post')
      Parser.unpack(params[:cid], request.env["rack.input"].read, true)
      [201, Utils.enc_j(msg: "OK")]
    end

    def self.start
      log(fn: "start", at: "build")
      @server = Mongrel::HttpServer.new("0.0.0.0", Config.port)
      @server.register("/", Rack::Handler::Mongrel.new(Web.new))
      log(fn: "start", at: "install_trap")
      ["TERM", "INT"].each do |s|
        Signal.trap(s) do
          log(fn: "trap", signal: s)
          @server.stop(true)
          log(fn: "trap", signal: s, at: "exit", status: 0)
          Kernel.exit!(0)
        end
      end
      log(fn: "start", at: "run", port: Config.port)
      @server.run.join
    end

    def self.log(data, &blk)
      Scrolls.log({ns: "web"}.merge(data), &blk)
    end

  end
end
