require 'rubygems'
require 'daru'

SCHEDULER.every '1h', :first_in => 0 do |job|
  # Run the python script, wait for finish and do necessary manipulations
  df = Daru::DataFrame.from_csv(Dir.pwd + '/db_data/daily_values.csv')
  labels = df.last(30)['time_created'].to_a

  date_list = []
  df.each_row do |vector|
    date_list.push(DateTime.parse(vector['time_created']))
  end

  df['time_obj'] = Daru::Vector.new date_list

  newest_date1 = df.last(30)['time_obj'].to_a[0]
  newest_date2 = df.last(30)['time_obj'].to_a[29]

  year_ago1 = Date.new(newest_date1.year-1, newest_date1.month, newest_date1.day)
  year_ago2 = Date.new(newest_date2.year-1, newest_date2.month, newest_date2.day)

  # Retrieve segment of the dataframe 1 year ago
  df_year_ago = df.where(df['time_obj'] > year_ago1)
  df_year_ago = df_year_ago.where(df_year_ago['time_obj'] < year_ago2)

  # Bring the dates forward one year to display on the same graph
  date_list = []
  df_year_ago.each_row do |vector|
    date_obj = DateTime.parse(vector['time_created'])
    date_obj = Date.new(date_obj.year + 1, date_obj.month, date_obj.day)
    date_obj = date_obj.strftime('%Y-%m-%d')
    date_list.push(date_obj)
  end
  df_year_ago['curr_date'] = Daru::Vector.new date_list

  data = [
    {
      label: 'Wireposts',
      data: df.last(30)['wireposts'].to_a,
      backgroundColor: [ 'rgba(255, 99, 132, 0.2)' ] * labels.length,
      borderColor: [ 'rgba(255, 99, 132, 1)' ] * labels.length,
      borderWidth: 1,
    }
  ]
  options = { }

  send_event('wireposts', { labels: labels, datasets: data, options: options })
end