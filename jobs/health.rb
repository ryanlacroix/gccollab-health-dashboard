SCHEDULER.every '24h', :first_in => 0 do |job|
    send_event('health',   { value: 50 })
end