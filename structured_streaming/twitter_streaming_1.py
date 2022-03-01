#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import sys, os

os.environ['SPARK_HOME'] = '/home/kanak/spark-2.4.7-bin-hadoop2.7'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python'

import socket
import requests
import requests_oauthlib
import json

ACCESS_TOKEN = '39024105-HsiNqecmQDXBdjYXXOZNzrd5t6NUdgkzuCSU1O0gm'
ACCESS_SECRET = 'IuLvAHNmsoagb0F2Qn24xs3erxQ2aubY11LUsUlAQJ9WY'
CONSUMER_KEY = 'fflOuU50FdAfiILk6eXkzDmS7'
CONSUMER_SECRET = '1Dfm78Kcc6h1b5peAJ8EzD7jwwoXuIQrxMoDDK06fINyxLIR9x'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=my_auth, stream=True)
	print(query_url, response)
	return response

def send_tweets_to_spark(http_resp, tcp_connection):
	for line in http_resp.iter_lines():
		try:
			full_tweet = json.loads(line)
			tweet_text = full_tweet['text']
			print("Tweet Text: " + tweet_text)
			print ("------------------------------------------")
			#tcp_connection.send(tweet_text + '\n')
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)
            
if __name__ == "__main__":            
          
    resp = get_tweets()
    
    TCP_IP = "localhost"
    TCP_PORT = 9009
    '''
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print("Waiting for TCP connection...")
    conn, addr = s.accept()
    print("Connected... Starting getting tweets.")
    
    send_tweets_to_spark(resp, conn)            
    '''


