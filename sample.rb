#! /usr/bin/env jruby

require 'helpers'

def process_curl_output(line, sample)
  return false if line.nil? or line.match(/% Total/) or line.match(/Dload/)

  timestamp = Time.now.utc.iso8601
  (percent_downloaded, total, percent_received, received, percent_transferred,
   transferred, average_download_speed, average_upload_speed, t1, t2, t3,
   download_speed) = line.gsub(/\s+/, ' ').split

  average_download_speed = normalize average_download_speed
  download_speed = normalize download_speed

  puts sample[timestamp] = {
    '@timestamp' => timestamp,
    'local_origin_ip_address' => local_ip_address,
    'remote_origin_ip_address' => remote_ip_address,
    'average_download_speed' => average_download_speed.to_i,
    'download_speed' => download_speed.to_i
  } if download_speed.to_i > 0

  percent_downloaded.to_i == 100
rescue Exception => ex
  puts "Error: #{ex.message} (#{ex.class})"
  puts ex.backtrace
  false
end

def main
  ensure_index_deleted # if starting fresh is important
  for target in Targets
    puts "Sampling #{target}"
    take_sample do |sample|
      curl(target) do |line| process_curl_output line, sample end
      bulk_create_docs target, sample
    end
  end
end

main if __FILE__ == $0
