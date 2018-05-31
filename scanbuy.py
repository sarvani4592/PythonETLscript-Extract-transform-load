import csv
import MySQLdb
import pandas as pd

# Establish a connection to Mysql
conn = MySQLdb.connect(host='DESKTOP-LCUCHM7', user='sarvani', passwd='dollar2017SR@', db='SCANBUY_PYTHONETL')
#get the cursor which is used to traverse the database line by line
cursor = conn.cursor()
# create csv_data iterable to loop through each entry
csv_data = csv.reader(file('20170701_20170701165514569.csv'))
csv_data1 = csv.reader(file('20170701_20170702004210139.csv'))

# execute and insert the csv into the database.
for row in csv_data:
	cursor.execute('INSERT INTO SCANBUY(pid, ad_id, id_type,user_agent,ip,epoch_timestamp,lat,`long`,accuracy,opt_out,country,proc_date)''VALUES(%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)',row)

for row in csv_data1:
	cursor.execute('INSERT INTO SCANBUY(pid, ad_id, id_type,user_agent,ip,epoch_timestamp,lat,`long`,accuracy,opt_out,country,proc_date)''VALUES(%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)',row)

#fetching data from sql to pandas dataframe

df = pd.read_sql('SELECT ad_id, id_type, lat, `long`, epoch_timestamp from SCANBUY', con=conn)
#deleting unnecessary columns
'''del df['pid']
del df['user_agent']
del df['ip']
del df['accuracy']
del df['opt_out']
del df['country']
del df['proc_date']'''
#rounding to 3 decimal places
df['lat'] = pd.Series([round(val, 3) for val in df['lat']], index = df.index)
df['`long`'] = pd.Series([round(val,3)for val in df['`long`']],index =df.index)

#epoch to datetime conversion

df['epoch_timestamp'] = pd.to_datetime(df['epoch_timestamp'], unit='s')


#grouping by columns to delete duplicated values
df1=df.drop_duplicates(subset=['ad_id','id_type','lat','`long`'],keep='first')

#convert each row to json format
df1.to_json('sarvaniippagunta-scanbuy-<20180530>.json', orient='records', lines=True)

#Print the total row count and unique Ad_id at the end of the script.
print df1.count(axis=1)
print df1.ad_id.unique()

#closing connections
conn.commit()
cursor.close()
conn.close()
