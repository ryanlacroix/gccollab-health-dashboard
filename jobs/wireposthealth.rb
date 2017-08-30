require 'json'

SCHEDULER.every '10m', :first_in => '3m' do |job|
    begin
        file = File.read(Dir.pwd + '/db_data/ind_health.json')
        data_hash = JSON.parse(file)
        send_event('wirepost-health',   { value: data_hash['wireposts'] })
    rescue
        puts("Reading stats failed")
    end
end