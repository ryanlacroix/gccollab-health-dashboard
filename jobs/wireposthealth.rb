require 'json'

SCHEDULER.every '10m', :first_in => 0 do |job|
    file = File.read(Dir.pwd + '/db_data/ind_health.json')
    data_hash = JSON.parse(file)
    send_event('wirepost-health',   { value: data_hash['wireposts'] })
end