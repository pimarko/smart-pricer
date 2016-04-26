import pandas as pd
import numpy as np
from datetime import datetime
import csv
import sys
	
#-------------------TASK 1--------------------------------
#set flag TOTAL_TICKETS_PER_WEEK_MOVIE_ROW to generate the csv file containing the results of task 1
#csv file has 4 columns and the last one contains the result


#-------------------TASK 2--------------------------------
#set flag SEAT_LOAD_FACTOR_PER_WEEK_MOVIE_ROW to generate the csv file containing the results of task 2
#csv file has 4 columns where the last one contains the result


#-------------------TASK 3--------------------------------
#set flag SEAT_LOAD_FACTOR_PER_WEEK_ROW to generate the csv file containing the results of task 3
#csv file has 3 columns where the last one contains the result


#constants
TOTAL_TICKETS_PER_WEEK_MOVIE_ROW = False
SEAT_LOAD_FACTOR_PER_WEEK_MOVIE_ROW = False
SEAT_LOAD_FACTOR_PER_WEEK_ROW = True

now = datetime.now()

#definitions
def read_tickets_sold():
	tickets_sold = pd.read_csv('tickets_sold.csv')

	return tickets_sold

def read_row_capacity():
	row_capacity = pd.read_csv('row_capacity.csv')
	
	return dict(row_capacity.values)


#total_tickets_per_week_movie_row
if(TOTAL_TICKETS_PER_WEEK_MOVIE_ROW):
	tickets_sold = read_tickets_sold()
	tickets_sold_groupby = tickets_sold.groupby(['calendarweek', 'movie', 'auditorium_row'])['ticket_id'].groups

	tickets_num = []
	calendarweek = []
	movie = []
	row = []
	for key in tickets_sold_groupby:
		ticket_num = len(tickets_sold_groupby[key])
		calendarweek.append(key[0])
		movie.append(key[1])
		row.append(key[2])
		tickets_num.append(ticket_num)

	SUBMISSION_FILENAME = "Total_tickets_per_week_movie_row_" + str(now.day) +"_" + str(now.hour) + \
	"_" + str(now.minute) + "_" + str(now.second) + ".csv"
	
	predictions_file = open(SUBMISSION_FILENAME, "wb")
	open_file_object = csv.writer(predictions_file)
	open_file_object.writerow(["calendarweek","movie","auditorium_row","total_tickets_per_week_movie_row"])
	open_file_object.writerows(zip(calendarweek,movie,row,tickets_num))
	predictions_file.close()

	print "Prediction file written."

#seat_load_factor_per_week_movie_row
elif(SEAT_LOAD_FACTOR_PER_WEEK_MOVIE_ROW):
	row_capacity = read_row_capacity()
	tickets_sold = read_tickets_sold()
	tickets_sold_groupby = tickets_sold.groupby(['calendarweek', 'movie', 'auditorium_row'])['ticket_id'].groups


	load_factors = []
	calendarweek = []
	movie = []
	row = []
	for key in tickets_sold_groupby:
		ticket_num = float(len(tickets_sold_groupby[key]))
		row_capacity_current = float(row_capacity[key[2]])
		load_factor = ticket_num/row_capacity_current
		calendarweek.append(key[0])
		movie.append(key[1])
		row.append(key[2])
		load_factors.append(load_factor)


	SUBMISSION_FILENAME = "Seat_load_factor_per_week_movie_row_" + str(now.day) +"_" + str(now.hour) + \
	"_" + str(now.minute) + "_" + str(now.second) + ".csv"
	
	predictions_file = open(SUBMISSION_FILENAME, "wb")
	open_file_object = csv.writer(predictions_file)
	open_file_object.writerow(["calendarweek","movie","auditorium_row","seat_load_factor_per_week_movie_row"])
	open_file_object.writerows(zip(calendarweek,movie,row,load_factors))
	predictions_file.close()

	print "Prediction file written."

elif(SEAT_LOAD_FACTOR_PER_WEEK_ROW):
	row_capacity = read_row_capacity()
	tickets_sold = read_tickets_sold()

	tickets_sold_groupby = tickets_sold.groupby(['calendarweek', 'show_id','auditorium_row'])['ticket_id'].groups

	for key in tickets_sold_groupby:
		ticket_num = float(len(tickets_sold_groupby[key]))
		row_capacity_current = float(row_capacity[key[2]])
		load_factor = ticket_num/row_capacity_current
		tickets_sold_groupby[key] = load_factor
	
	new_dict = {}
	for i in range(len(tickets_sold_groupby)):
		item_i = tickets_sold_groupby.items()[i]
		key_i0 = item_i[0][0]
		key_i2 = item_i[0][2]
		value_i = item_i[1]
		keys_inserted = new_dict.keys()
		go_further = True
		for key_inserted in keys_inserted:
			if(key_inserted[0] == key_i0 and key_inserted[1] == key_i2):
				go_further = False
				break
		
		if(go_further):
			avg_load = []
			save_index = []
			hit = False
			for j in range(i+1,len(tickets_sold_groupby)):
				item_j = tickets_sold_groupby.items()[j]
				key_j0 = item_j[0][0]
				key_j2 = item_j[0][2]
				value_j = item_j[1]
				if(key_i0 == key_j0 and key_i2 == key_j2):
					if(hit == False):
						avg_load.append(value_i)
						avg_load.append(value_j)
						save_index.append(key_i0)
						save_index.append(key_i2)
						hit = True
					else:
						avg_load.append(value_j)

			if(len(avg_load) > 0):
				get_avg = sum(avg_load)/len(avg_load)
				new_dict[(save_index[0],save_index[1])] = get_avg

	load_factors = []
	calendarweek = []
	row = []
	for key in new_dict:
		load_factor = new_dict[key] 
		calendarweek.append(key[0])
		row.append(key[1])
		load_factors.append(load_factor)


	SUBMISSION_FILENAME = "Seat_load_factor_per_week_row_" + str(now.day) +"_" + str(now.hour) + \
	"_" + str(now.minute) + "_" + str(now.second) + ".csv"
	
	predictions_file = open(SUBMISSION_FILENAME, "wb")
	open_file_object = csv.writer(predictions_file)
	open_file_object.writerow(["calendarweek","auditorium_row","seat_load_factor_per_week_row"])
	open_file_object.writerows(zip(calendarweek,row,load_factors))
	predictions_file.close()

	print "Prediction file written."

else:
	print "Terminating..."