# -*- coding: utf-8 -*-
"""
Created on Mon Mar 28 22:39:41 2022

@author: samuel ubrezi
"""

import db_setup
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

database_name = 'weather.db'
db_connection = db_setup.connect_database(database_name)

sql_query = """
    select	round(t2.latitude, 1) as latitude_rounded,
    		round(t2.longitude,1) as longitude_rounded,
    		sum(t1.precipitation) as total_precipitation,
		    count(t1.event_id) as total_events
    from	event t1
    left join	station t2
    	on t1.station_id = t2.id 
    where t1.type_id = 4
    group by latitude_rounded, longitude_rounded
    order by latitude_rounded, longitude_rounded
"""

df = pd.read_sql(sql_query, db_connection)


# ax = df.plot.scatter(x = 'longitude_rounded',
#                     y = 'latitude_rounded',
#                     c = 'total_precipitation',
#                     # s = 'total_events',
#                     colormap = 'viridis_r')

ax2 = df.plot.hexbin(x = 'longitude_rounded',
                    y = 'latitude_rounded',
                    C = 'total_precipitation',
                    colormap = 'viridis_r',
                    reduce_C_function = np.max,
                    gridsize = 35)
