SCHEDULER.every '10m', :first_in => '3m' do |job|
    begin
        health = File.open(Dir.pwd + "/db_data/health_stat.txt").first
        send_event('health',   { value: health })
    rescue
        puts("Reading stats failed")
    end
end