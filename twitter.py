'''
Created on Nov 27, 2013

@author: henok
'''
#!/usr/bin/env python
import sys
import os
import datetime
import time
from sets import Set

#Thrift client API for HBase is ued in this project 
from thrift import Thrift
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport.TTransport import TBufferedTransport
 
from hbase import Hbase
from hbase.ttypes import *
#connect to a database located in 10.0.2.15 host at port 9090.
#the database must be running and Thrift listening at port 9090 
transport = TSocket.TSocket('10.0.2.15',9090)
transport.open()
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)
print(client.getTableNames())

table_name = 'twitter'

def usage():
    print "Login: type  L"
    print "Register:  type R"
    print "Tweet: type T"
    print "Start Following others : Type F"
    chioce =raw_input("")
    if (chioce == 'R'):
      add_user()
    elif(chioce == 'F'):  
        username=raw_input("Please Enter your user name :")
        follow (username)
    elif (chioce=='L'):
          username=raw_input("Please Enter your user name :")
          log_in(username)
    elif(chioce == 'T'):    
          username=raw_input("Please Enter your user name :")
          my_tweets(username)
# when a user logs in he will be presented with all the tweets from the people he is following
def log_in(username):
    print "Hi ",username," Welcome! These are the tweet for you:"
    # get all entries of row key called username in the following column family
    scanid=client.scannerOpen(table_name, username, ['following'],{})
    rows = client.scannerGetList(scanid,10) # just get 10 of them
    if rows:
        for k in range(0, len(rows)):
          v= rows[k].columns.items()
          print v[0][0].split(':')[1],":", v[k][1].value # prints the user name and his tweets 
    else:
        usage()
#returns a set of all users in database. 
def hbase_users(username):
    users =[]
    scanid=client.scannerOpen(table_name, '', ['user_p'],{})
    rows = client.scannerGetList(scanid,10)
    if rows:
        for k in range (0,len(rows)):
            if (rows[k].row != username):
                users.append(rows[k].row)
    return set(users)            
#allows registers user to the system 
def add_user():
    colfamily1 = 'user_p'
    user_name =raw_input("Please Enter a user name: ")
    email =raw_input("Please Enter your email: ")   
    fullcol1 = ('%s:%s' % (colfamily1, 'email'))
    client.mutateRow(table_name, user_name, [Mutation(column=fullcol1, value=email)],{}) 
    usage()
#allows user to tweet. Every tweet of this user will put in for the followers of this user 
def my_tweets(username):
    colfamily1 = 'my_tweets'
    print "Hi ",username," Please Enter your Tweet : 140 characters "
    tweet=raw_input()
    fullcol1 = ('%s:%s' % (colfamily1,'tweet'))
    client.mutateRow(table_name, username,[Mutation(column=fullcol1, value=tweet)],{})
    fusers=follower_list(username)
    if (len(fusers)!=0):
            for f in range (0, len(fusers)):
                update_followers(username,fusers[f],tweet)
    
    usage()
def update_followers(username,fusername,tweet):
    fullcol1 = ('%s:%s:%s' % ('following', username,'tweet'))
    client.mutateRow(table_name, fusername,[Mutation(column=fullcol1, value=tweet)],{})
    
def following_list(username):
    fusers =[]
    scanid=client.scannerOpen(table_name, username, ['following'],{})
    rows = client.scannerGetList(scanid,10)
    print len(rows)
    print len(rows[0].columns)
    if (len(rows[0].columns)!=0):
        for k in range(0,len(rows)):
          print rows[k]
          v= rows[k].columns.items()
          st = v[0][0]
          fusers.append(v[0][0].split(':')[1])
   
    return set(fusers)    
#given a user name returns a list of tweets of this user name
def get_tweets(username):
    scanid=client.scannerOpen(table_name, username, ['my_tweets'],{})
    rows = client.scannerGetList(scanid,10)# just get 10 of the tweets of this user
    tweets =[]
    print len(rows)
    if rows:
      print rows[0]
      for k in range(0,len(rows[0].columns)):
         print len(rows[0].columns)
         print rows[0].columns['my_tweets:tweet'].value
         tweets.append(rows[0].columns['my_tweets:tweet'].value)
#         print rows[k].columns['my_twettes:tweet'].value
#         print rows[k].columns['my_twettes:time'].timestamp
#         print rows[k].columns['my_twettes:time'].value
    return tweets
# allows a user to start following other users. As soon as the user chose to follow 
# a specific user all the tweets of this user will be avaliable for the one who is following 
def follow (username):
    colfamily1 = 'following'
    print "Hi ",username, " Here are friends you can follow :"
    hbaseuses= hbase_users(username)
    listfolowwing = following_list(username)
    for i in range (0, len(list(hbaseuses - listfolowwing))):
        print list(hbaseuses - listfolowwing)[i]
    user=raw_input("Please Enter the user name :")
    user_tweets=get_tweets(user)
    if (len(user_tweets)!=0):# if there are already tweets from this user make them avaliable
      for i in range (0, len(user_tweets)):
         fullcol1 = ('%s:%s:%s' % (colfamily1, user,'tweet'+str(i)))
         client.mutateRow(table_name, username, [Mutation(column=fullcol1, value=user_tweets[i])],{})
    print "You are following ",user
    add_follwer(user,username)
    usage()
#adds a user, named username as a follower of another user 
def add_follwer(username,user):
    colfamily1 = 'follower'
    fullcol1 = ('%s:%s' % (colfamily1, user))
    client.mutateRow(table_name, username, [Mutation(column=fullcol1, value=user)],{})
# returns a list of followers of a given user name
def follower_list(username):
    fusers =[]
    scanid=client.scannerOpen(table_name, username, ['follower'],{})
    rows = client.scannerGetList(scanid,10)
    print len(rows)# if this user has followers
    if rows:
        for k in range(0, len(rows)):
           print rows[k]
           v= rows[k].columns.items()
           st = v[0][0]
           fusers.append(v[0][0].split(':')[1])
    return fusers
    
usage()    
  