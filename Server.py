#! /usr/bin/python
try:
 import threading
 import httpparser  #user defined httpparser 
 import sys
 import time
 import dev_db
 import pickle
 from socket import *
except ImportError as e:
        print 'ERROR IN IMPORTING PYTHON LIBRARIES: ',e




#IncompatibleFileType exception class
class IncompatibleFileTypeException(Exception):
        value = ''
        def __init__(self,value):
                self.value = value
        def __str__(self):
                return repr(self.value)


#class to implement client threads
class clientConnectionSocket(threading.Thread):
 def __init__(self,clientaddr):
 	threading.Thread.__init__(self)
 	self.clientaddr = clientaddr
 def run(self):
    try:
           clientSocket = connectionSocket
           message = clientSocket.recv(512)
           httpResponse = ''
           fileContents = ''
           #parse the http message      
           message = message.strip("\n")
           print "query = ",message
           rString = dev_db.evaluateQuery(message,self.clientaddr) #sumanth
           rString = pickle.dumps(rString)
           clientSocket.send(rString)


    except IOError as e:

           clientSocket.send('File not found to load a table from, server terminating'+time.ctime())

    except IncompatibleFileTypeException as e:
           clientSocket.send(e.value)
    except RuntimeError as e:
           clientSocket.send('ERROR 405 : METHOD NOT SUPPORTED ON THIS SERVER ')

    clientSocket.close()
    print 'TCP connection to:',clientaddr,'closed', time.ctime()

try:

 serverPort = 12345
 serverName = ''
 serverSocket = socket(AF_INET, SOCK_STREAM)
 serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
 serverSocket.bind((serverName, serverPort))
 #listen for incoming connections on the serverSocket
 serverSocket.listen(5)


 print "Server Process started", time.ctime()
 print 'SERVER READY TO RECIEVE DATA:\n-------------------------------------'
 print 'SERVER CONSOLE:\n-------------------------------------'
 while 1:
        #create client thread if there is an incoming client request
        connectionSocket, clientAddress = serverSocket.accept()
        print "TCP connection to:",clientAddress,"active", time.ctime()

        clientThread = clientConnectionSocket(clientAddress)
        clientThread.start()

        #NOTE:server should be terminated using a keyboard interrupt
except (error,timeout) as e:           #socket.error and socket.timeout
        print 'A SOCKET ERROR HAS OCCURED \n', e
except IOError as e:
        print 'INPUT/OUTPUT ERROR \n',e
except KeyboardInterrupt as e:
        print "Server process terminated ", time.ctime()


