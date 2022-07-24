#!/usr/bin/env python
# coding: utf-8

# In[1]:


#function con() to create the connection with the database
import psycopg2
from psycopg2 import Error
def con():


    try:
    # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                  password="bashar123",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="project2 new_version")

    # Create a cursor to perform database operations
      
        return connection
    except (Exception, psycopg2.Error) as error:
             print("Error while connecting to PostgreSQL", error)
             cursor = connection.cursor()
    


# In[2]:


#function to execute the qery and return the query result
def execute_query(string):
    connection = con()
    cursor = connection.cursor()
    cursor.execute(string)
    return cursor.fetchall()


# In[3]:


#creation of the list filtered_result representing the groups created through the SQL query
query="select distinct l1.uid,lp.uid from lines as l1, (select l2.uid,p.tempo,l2.id,p.geom from people2020 as p,lines as l2 where p.uid=l2.uid)as lp where l1.id!=lp.id and  lp.tempo between l1.start and l1.end and st_distance(l1.geom,lp.geom)<1 order by l1.uid"
result=execute_query(query);
list_result=list(result);
filtered_result=[];
for i in range(0,len(list_result),2):
    filtered_result.append(result[i]);
print(filtered_result)


# In[4]:


#creation of the empty table "groups"
connection = con()
cursor = connection.cursor()
query2="CREATE TABLE groups (id INT PRIMARY KEY, person1 VARCHAR(20),person2 VARCHAR(20));"
cursor.execute(query2)
connection.commit()


# In[5]:


#filling of the table groups with the elements in filtered_result
connection = con()
cursor = connection.cursor()
for i in range(0,len(filtered_result)):
    query2="INSERT INTO groups VALUES ("+str((i+1))+","+str(filtered_result[i][0])+","+str(filtered_result[i][1])+" )";
    cursor.execute(query2)
    connection.commit()


# In[6]:


connection = con()
cursor = connection.cursor()
for i in range(0,len(filtered_result)):
    query6="create table  group"+str(i+1)+" as (select lp.uid,lp.geom from lines as l1,     (select p.uid,p.tempo,p.geom from people2020 as p where p.uid in ( '"+str(filtered_result[i][0]) +"','"     +str(filtered_result[i][1])+"' ))as lp     where l1.uid in ('"+str(filtered_result[i][0])+"','"+str(filtered_result[i][1])+"' ) and     l1.uid!=lp.uid and  lp.tempo    between  l1.start and l1.end and st_distance(l1.geom,lp.geom)<1 )"
    cursor.execute(query6)
    connection.commit()


# In[7]:


counts=[]
for i in range(0,len(filtered_result)):
    query7="select count(distinct a.geom) as count1,count(distinct b.geom) as count2 from group"+str(i+1)+" as a,     group"+str(i+1)+ " as b where st_distance(a.geom,b.geom)<1 and a.uid='"+str(filtered_result[i][0])+"     ' and b.uid='"+str(filtered_result[i][1])+"'"
    counts.append(execute_query(query7))


# In[10]:


connection = con()
cursor = connection.cursor()
#update the table "groups" filling the column num_points_of_user1_proximity_to_user2 with the elements in A
for i in range(0,len(counts)):
    query4="update groups as g set num_points_of_user1_proximity_user2="+str(counts[i][0][0])+" ,     num_points_of_user2_proximity_user1="+str(counts[i][0][1])+"where g.id="+str(i+1)
    cursor.execute(query4)
    connection.commit()


# In[ ]:




