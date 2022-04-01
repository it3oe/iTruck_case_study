# -*- coding: utf-8 -*-
"""
Created on Mon Mar 28 22:39:41 2022

@author: samuel ubrezi
"""

import db_setup
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn import metrics

database_name = 'weather.db'
db_connection = db_setup.connect_database(database_name)

sql_query = """
    select	t1.event_id,
		t2.severity_type,
		t1.precipitation,
		cast((JulianDay(t1.end_time) - JulianDay(t1.start_time))* 24 * 60 as integer) as rain_duration
    from	event t1
    left join	weather_severity t2
    	on t1.severity_id = t2.id 
    where t1.type_id = 4
"""

df = pd.read_sql(sql_query, db_connection)
df_sample = df.sample(1_000)
plt1, ax1 = plt.subplots()
ax = sns.scatterplot(data = df_sample, 
                     x = 'precipitation', 
                     y = 'rain_duration', 
                     hue = 'severity_type', 
                     ax = ax1).set_title('Rain data visualisation')

df['target'] = df['severity_type'].map({'Light': 0, 'Moderate': 1, 'Heavy': 2})
x = preprocessing.normalize(df[['precipitation','rain_duration']].to_numpy(), axis = 0)
y = preprocessing.normalize(np.expand_dims(df['target'].to_numpy(), axis = 1)).flatten()

seed = 5
test_size = 0.3 # 70-30 ratio
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_size, random_state=seed)

NN = MLPClassifier(hidden_layer_sizes = (120,60),
                   activation = 'tanh',
                   max_iter = 100,
                   random_state = seed, 
                   verbose = True)
NN.fit(x_train,y_train)

y_pred = NN.predict(x_test)
met = metrics.confusion_matrix(y_test, y_pred, normalize = 'all')
plt2, ax2 = plt.subplots()
sns.heatmap(data = met*100, 
                   annot=True, fmt = '.2f', 
                   cmap = 'Blues', 
                   cbar = False,
                   ax = ax2).set_title('Confustion matrix')
for t in ax2.texts: t.set_text(t.get_text() + " %")

acc_train, acc_test = NN.score(x_train, y_train), NN.score(x_test, y_test)
print(f'Model accuracy is:\n\tTrain: {100*acc_train:.2f}%\n\tTest: {100*acc_test:.2f}%')