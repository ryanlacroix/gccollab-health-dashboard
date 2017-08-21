SCHEDULER.every '1d', :first_in => 0 do |job|
    puts "Refreshing network statistics.."
    a = `python3 #{Dir.pwd + '/db_data/'}NetworkHealthCollab.py`
    puts "done."
end