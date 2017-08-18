require 'rubygems'
require 'daru'

SCHEDULER.every '1m', :first_in => 0 do |job|
    # Run the python script, wait for finish and do necessary manipulations
    # `python do_the_thing()`
    df = Daru::DataFrame.from_csv(Dir.pwd + '/db_data/Sample_Dashboard_Data.csv')

    # Retrieve values from a year ago
    date_list = []
    df.each_row do |vector|
    date_list.push(DateTime.parse(vector['time_created']))
    end
    df['time_obj'] = Daru::Vector.new date_list

    newest_date1 = df.last(30)['time_obj'].to_a[0]

    month_ago1 = Date.new(newest_date1.year, newest_date1.month - 2, newest_date1.day)
    month_ago2 = Date.new(newest_date1.year, newest_date1.month - 1, newest_date1.day)

    # Retrieve segment of the dataframe 1 year ago
    df_month_ago = df.where(df['time_obj'] > month_ago1)
    df_month_ago = df_month_ago.where(df_month_ago['time_obj'] < month_ago2)

    total_counts = Hash.new({ value: 0})

    # Get sums (last 30 days)
    files = df.last(30)['File'].sum
    discs = df.last(30)['Discussion'].sum
    replies = df.last(30)['Discussion Reply'].sum
    users = df.last(30)['user'].sum

    # Get sums (last year, 30 days)
    files_ly = df_month_ago['File'].sum
    discs_ly = df_month_ago['Discussion'].sum
    replies_ly= df_month_ago['Discussion Reply'].sum
    users_ly = df_month_ago['user'].sum

    def desc_diff(v1, v2)
        diff = (v1.to_i - v2.to_i)
        if (diff) > 0
            return " (⇧" + diff.to_s + ")"
        else
            return " (⇩" + diff.abs.to_s + ")"
        end
    end

    #puts (files.to_s + desc_diff(files, files_ly))

    total_counts['file'] = { label: 'Files uploaded', value: (files.to_s + desc_diff(files, files_ly)) }
    total_counts['disc'] = { label: 'Discussions started', value: (discs.to_s + desc_diff(discs, discs_ly)) }
    total_counts['repl'] = { label: 'Discussion replies', value: (replies.to_s + desc_diff(replies, replies_ly))}
    total_counts['user'] = { label: 'New users', value: (users.to_s + desc_diff(users, users_ly))}

    send_event('totals', { items: total_counts.values })
end