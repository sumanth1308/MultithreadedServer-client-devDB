#!/usr/bin/python
import pickle
from socket import *

s= ""
rs = ""
l = []



def printTable(a):
        """Prints the result as a formatted table, accepts an input of a list of a list of dictionaries"""
        for key, value in a[0].iteritems():
                print  "%12s\t" %(key),
        print ''
        for i in range(len(a)):
                for key, value in a[i].iteritems():
                        print "%12s\t" %(value),
                print ''





sock = socket(AF_INET,SOCK_STREAM)
while True:
	sock.connect(("localhost",12345))
	query = raw_input("enter query:\n")
	sock.send(query)
	rs = sock.recv(10000)
	l = pickle.loads(rs)
	if type(l) == list:	
		printTable(l)	
	elif type(l) == str:
		print l	
	sock.close()
	sock = socket(AF_INET,SOCK_STREAM)
