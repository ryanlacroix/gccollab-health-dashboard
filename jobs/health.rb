SCHEDULER.every '1h', :first_in => 0 do |job|
    health = File.open(Dir.pwd + "/db_data/health_stat.txt").first
    send_event('health',   { value: health })
end