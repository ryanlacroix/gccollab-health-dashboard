require 'rubygems'
require 'daru'
require 'json'

SCHEDULER.every '10m', :first_in => 0 do |job|
    # Get stats file
    df = Daru::DataFrame.from_csv(Dir.pwd + '/db_data/daily_values.csv')

    # Individual health file
    file = File.read(Dir.pwd + '/db_data/ind_health.json')
    health_hash = JSON.parse(file)

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
    wireposts = df.last(30)['wireposts'].sum
    groups = df.last(30)['groupscreated'].sum
    groups_j = df.last(30)['groupsjoined'].sum
    messages = df.last(30)['messages'].sum
    blogposts = df.last(30)['blogposts'].sum
    likes = df.last(30)['likes'].sum
    comments = df.last(30)['comments'].sum
    
    # Get sums (last year, 30 days)
    wireposts_ly = df_month_ago['wireposts'].sum
    groups_ly = df_month_ago['groupscreated'].sum
    groups_j_ly = df_month_ago['groupsjoined'].sum
    messages_ly= df_month_ago['messages'].sum
    blogposts_ly = df_month_ago['blogposts'].sum
    likes_ly = df_month_ago['likes'].sum
    comments_ly = df_month_ago['comments'].sum

    def desc_diff(v1, v2)
        diff = (v1.to_i - v2.to_i)
        if (diff) > 0
            return " (⇧" + diff.to_s + ")"
        else
            return " (⇩" + diff.abs.to_s + ")"
        end
    end

    def healthify(h_value)
        return "Health: " + h_value.to_i.to_s + "%"
    end

    total_counts['wire'] = { label: 'Wire posts', value: (wireposts.to_s + desc_diff(wireposts, wireposts_ly)), healthvalue: healthify(health_hash['wireposts']) }
    total_counts['grpc'] = { label: 'New groups', value: (groups.to_s + desc_diff(groups, groups_ly)), healthvalue: healthify(health_hash['groupscreated']) }
    total_counts['grpj'] = { label: 'Groups joined',value: (groups_j.to_s + desc_diff(groups_j, groups_j_ly)), healthvalue: healthify(health_hash['groupsjoined']) }
    total_counts['repl'] = { label: 'Messages',   value: (messages.to_s + desc_diff(messages, messages_ly)), healthvalue: healthify(health_hash['messages'])}
    total_counts['user'] = { label: 'Blog posts', value: (blogposts.to_s + desc_diff(blogposts, blogposts_ly)), healthvalue: healthify(health_hash['blogposts'])}
    total_counts['like'] = { label: 'Likes',      value: (likes.to_s + desc_diff(likes, likes_ly)), healthvalue: healthify(health_hash['likes'])}
    total_counts['comm'] = { label: 'Comments',   value: (comments.to_s + desc_diff(comments, comments_ly)), healthvalue: healthify(health_hash['comments'])}

    send_event('totals', { items: total_counts.values })
end