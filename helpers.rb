
require 'elasticsearch'
require 'json'
require 'socket'

Targets = [
  'http://speedtest.wdc01.softlayer.com/downloads/test10.zip',
  'http://speedtest.wdc01.softlayer.com/downloads/test500.zip'
]

ElasticsearchHost =
  'http://ceres-kibana01.global.preprod-ord.ohthree.com:9200'

ElasticsearchConfig = {
  :host => ElasticsearchHost
}

BandwidthMeterIndex = 'bandwidth-meter'
BandwidthMeterType = 'bandwidth-metrics'

def elasticsearch_client
  @elasticsearch_client ||= Elasticsearch::Client.new ElasticsearchConfig
end

def create_index(index=BandwidthMeterIndex)
  index = {
    :index => index,
    :body  => {
      :settings => {
        :index => {
          :number_of_shards   => 1,
          :number_of_replicas => 0
        }
      }
    }
  }
  result = elasticsearch_client.indices.create(arguments = index)
  puts "New elasticsearch index: #{JSON.pretty_generate result}"
  result
end

def index_exists?(_index=BandwidthMeterIndex)
  index = { :index => _index }
  elasticsearch_client.indices.exists?(arguments = index)
end

def ensure_index_created(_index=BandwidthMeterIndex)
  create_index(index = _index) unless index_exists?(index = _index)
end

def delete_index(_indices=BandwidthMeterIndex)
  index = { :index => _indices }
  result = elasticsearch_client.indices.delete(arguments = index)
  puts "Delete elasticsearch index: #{JSON.pretty_generate result}"
  result
end

def ensure_index_deleted(_index=BandwidthMeterIndex)
  delete_index(index = _index) if index_exists?(index = _index)
end

def build_doc(title, data,
              _index=BandwidthMeterIndex,
              _type=BandwidthMeterType)
  doc = {
    :index => _index,
    :type  => _type,
    :body  => {
      :title        => title,
      :published    => true,
      :published_at => Time.now.utc.iso8601
    }.update(data)
  }
end

def create_doc(title, data,
               _index=BandwidthMeterIndex)
  doc = build_doc title, data
  ensure_index_created(index = _index)
  result = elasticsearch_client.create(arguments = doc)
  puts "New elasticsearch document: #{JSON.pretty_generate result}"
  result
end

def bulk_create_docs(title, sample_data,
                     _index=BandwidthMeterIndex,
                     _type=BandwidthMeterType)
  body = Array.new
  for timestamp, data in sample_data
    body << {
      :index => {
        '_index' => _index, '_type' => _type
      }
    }
    body << data
  end
  bulk = {
    :body => body
  }
  ensure_index_created
  result = elasticsearch_client.bulk(arguments = bulk)
  puts "Bulk elasticsearch update: #{JSON.pretty_generate result}"
  result
end

def update_doc(document_id, data,
               _index=BandwidthMeterIndex,
               _type=BandwidthMeterType)
  ensure_index_created(index = _index)
  doc = {
    :index => _index,
    :type  => type,
    :id    => document_id,
    :body  => {
      :doc => {
        :data => data,
      }
    }
  }
  result = elasticsearch_client.update(arguments = doc)
  puts "Updated elasticsearch document: #{JSON.pretty_generate result}"
  result
end

def delete_all_docs(_index=BandwidthMeterIndex)
  ensure_index_created(index=_index)
  query = {
    :index => _index,
    :body  => {
      :query => {
        :term => {
          :local_origin_ip_address => '*'
        }
      }
    }
  }
  result = elasticsearch_client.delete_by_query(arguments = query)
  puts "Deleted elasticsearch documents: #{JSON.pretty_generate result}"
  result
end

def remote_ip_address
  @remote_internet_protocol_address ||=
    `curl -so - -O http://icanhazip.com/`.strip
end

def local_ip_address
  @local_internet_protocol_address ||=
    IPSocket.getaddress(Socket.gethostname)
end

def curl(target)
  tempfile = "/tmp/curl-#{Time.now.to_i}.log"
  File.delete tempfile if File.exist? tempfile
  tempfile = File.new(tempfile, 'w')
  curl_cmd = %Q{curl -o /dev/null #{target} 2> #{tempfile.path}}

  IO.popen(curl_cmd, 'r+') do |curl|
    tail(tempfile) do |line|
      if line.nil?
        false
      else
        result = yield line
        result or curl.pid.nil?
      end
    end
  end

  File.delete tempfile
end

def tail(file, debug=false)
  interrupted = false
  trap('INT') do
    interrupted = true
  end
  File.open(file) do |file|
    until interrupted
      line = file.gets
      puts line if debug
      result = yield line
      break if result
      if file.eof?
        sleep 0.1
        file.seek(file.tell)
      end
    end
  end
end

def take_sample
  yield Hash.new
end

# Returns kB/s
def normalize(s)
  s ||= '0'
  case s
  when /k$/i
    s.sub!(/k$/i, '')
    s = s.to_i * 1000
  when /m$/i
    s.sub!(/m$/i, '')
    s = s.to_i * 1000000
  when /g$/i
    s.sub!(/g$/i, '')
    s = s.to_i * 1000000000
  else
    s = s.to_i
  end
  (s / 1000).to_s
end
