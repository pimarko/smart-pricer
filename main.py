import pandas as pd
import sys

#constants
TOTAL_TICKETS_PER_WEEK_MOVIE_ROW = False
SEAT_LOAD_FACTOR_PER_WEEK_MOVIE_ROW = False
SEAT_LOAD_FACTOR_PER_WEEK_ROW = True


#definitions
def read_tickets_sold():
	tickets_sold = pd.read_csv('tickets_sold.csv')

	return tickets_sold

def read_row_capacity():
	row_capacity = pd.read_csv('row_capacity.csv')
	
	return row_capacity

#total_tickets_per_week_movie_row
if(TOTAL_TICKETS_PER_WEEK_MOVIE_ROW):
	tickets_sold = read_tickets_sold()
	tickets_sold_groupby = tickets_sold.groupby(['calendarweek', 'movie', 'auditorium_row'])['ticket_id'].count()

	print tickets_sold_groupby

#seat_load_factor_per_week_movie_row
elif(SEAT_LOAD_FACTOR_PER_WEEK_MOVIE_ROW):
	row_capacity = read_row_capacity()
	tickets_sold = read_tickets_sold()

 	#set as_index = False to get DataFrame object(otherwise Series Object)
	df_ticket_count = tickets_sold.groupby(['calendarweek', 'movie', 'auditorium_row'], as_index = False)['ticket_id'].count()
	df_ticket_count_max_seats_count = pd.merge(df_ticket_count,row_capacity,on = 'auditorium_row')
	df_final = df_ticket_count_max_seats_count['ticket_id'].div(df_ticket_count_max_seats_count['max_seats_per_row'])
	df_final.name = ''
	df_result = pd.concat([df_ticket_count_max_seats_count['calendarweek'],df_ticket_count_max_seats_count['movie'],df_ticket_count_max_seats_count['auditorium_row'],df_final], axis = 1)

	print df_result.to_string(index=False)



elif(SEAT_LOAD_FACTOR_PER_WEEK_ROW):
	row_capacity = read_row_capacity()
	tickets_sold = read_tickets_sold()

	df_ticket_count = tickets_sold.groupby(['calendarweek', 'show_id', 'auditorium_row'], as_index = False)['ticket_id'].count()
	df_ticket_count_max_seats_count = pd.merge(df_ticket_count,row_capacity,on = 'auditorium_row')
	df_load_factor = df_ticket_count_max_seats_count['ticket_id'].div(df_ticket_count_max_seats_count['max_seats_per_row'])
	df_load_factor.name = 'load_factor_per_week_show_id_row'
	df_row_load = pd.concat([df_ticket_count_max_seats_count['calendarweek'],df_ticket_count_max_seats_count['show_id'],df_ticket_count_max_seats_count['auditorium_row'],df_load_factor], axis = 1)
	df_count_same_row = df_row_load.groupby(['calendarweek','auditorium_row'],as_index = False)['show_id'].count()
	df_sum_load = df_row_load.groupby(['calendarweek','auditorium_row'],as_index = False)['load_factor_per_week_show_id_row'].sum()
	df_final = df_sum_load['load_factor_per_week_show_id_row'].div(df_count_same_row['show_id'])
	df_final.name = ''
	df_result = pd.concat([df_count_same_row['calendarweek'], df_count_same_row['auditorium_row'], df_final], axis = 1)

	print df_result.to_string(index=False)

else:
	print "Terminating..."