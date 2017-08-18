require 'rubygems'
require 'daru'

SCHEDULER.every '1m', :first_in => 0 do |job|
  # Run the python script
  # `python asdfg`
  df = Daru::DataFrame.from_csv(Dir.pwd + '/db_data/Sample_Dashboard_Data.csv')
  # Note: change this to obtain your chart data from some external source
  labels = df.last(30)['time_created'].to_a

  # Build date objects from vector
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

  data = [
    {
      label: 'New discussions',
      #data: Array.new(labels.length) { rand(40..80) },
      data: df.last(30)['Discussion'].to_a,
      backgroundColor: [ 'rgba(255, 99, 132, 0.2)' ] * labels.length,
      borderColor: [ 'rgba(255, 99, 132, 1)' ] * labels.length,
      borderWidth: 1,
    }, {
      label: 'Discussion replies',
      data: df.last(30)['Discussion Reply'].to_a,
      backgroundColor: ['rgba(255, 206, 86, 0.2)'] * labels.length,
      borderColor: ['rgba(255, 206, 86, 1)'] * labels.length,
      borderWidth: 1,
    }, {
      label: 'New Discussions one year ago',
      data: df_year_ago['Discussion'].to_a,
      backgroundColor: [ 'rgba(114, 107, 213, 0.2)'] * df_year_ago.size,
      borderColor: [ 'rbga(25, 15, 167)'] * df_year_ago.size,
      borderWidth: 1,
    }
  ]
  options = { }

  send_event('discussions', { labels: labels, datasets: data, options: options })
end