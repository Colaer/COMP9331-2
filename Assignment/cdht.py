#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : cdht.py
# @Author: Jingjie Jin
# @ID    : Z5085901
# @Date  : 2018/5/3

import sys
import socket
import time
import select


# Initialization
def initialization():
    global peerId
    global successor1
    global successor2
    global udpSocket
    global tcpSocket

    # set peer with two seccessors
    peerId = sys.argv[1]
    successor1 = sys.argv[2]
    successor2 = sys.argv[3]

    # set UDP socket
    udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udpSocket.bind((host, basePort + int(peerId)))
    udpSocket.setblocking(0)

    # set TCP listener socket
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcpSocket.bind((host, basePort + int(peerId)))
    tcpSocket.listen(5)
    tcpSocket.setblocking(0)

# UDP ping handler
def udpHandler():
    global predecessor1
    global predecessor2
    global pingTracker1
    global pingTracker2

    data, address = udpSocket.recvfrom(1024)
    data = data.decode().strip()
    splitData = data.split(' ')
    if len(splitData) == 4: # 0=pingRequest 1=sequenceNumber 2=peerId 3=predecessorCounts
        if splitData[0] == 'pingRequest':
            print('A ping request message was received from Peer', splitData[2] + '.')
            pingResponse(splitData[2], splitData[1])
        if splitData[3] == '1':
            predecessor1 = splitData[2]
        elif splitData[3] == '2':
            predecessor2 = splitData[2]
    elif len(splitData) == 3: # 0=pingResponse 1=sequenceNumber 2=peerId
        if splitData[0] == 'pingResponse':
            print('A ping response message was received from Peer', splitData[2] + '.')
            if int(splitData[2]) == int(successor1):
                if int(splitData[1]) in pingTracker1:
                    pingTracker1.remove(int(splitData[1]))
                    trackFilter(1, int(splitData[1]))
            elif int(splitData[2]) == int(successor2):
                if int(splitData[1]) in pingTracker2:
                    pingTracker2.remove(int(splitData[1]))
                    trackFilter(2, int(splitData[1]))

# Ping request sender
def pingRequest(toPeer, counts):
    global seqNum1
    global seqNum2
    global pingTracker1
    global pingTracker2
    if toPeer == successor1:
        udpSocket.sendto(str('pingRequest ' + str(seqNum1) + ' ' + peerId + ' ' + str(counts)).encode(), (host, basePort + int(toPeer)))
        pingTracker1.append(seqNum1)
        seqNum1 += 1
    else:  # toPeer == successor2:
        udpSocket.sendto(str('pingRequest ' + str(seqNum2) + ' ' + peerId + ' ' + str(counts)).encode(), (host, basePort + int(toPeer)))
        pingTracker2.append(seqNum2)
        seqNum2 += 1

# Ping response sender
def pingResponse(toPeer, seqNum):
    udpSocket.sendto(str('pingResponse ' + str(seqNum) + ' ' + peerId).encode(), (host, basePort + int(toPeer)))

# Ping tracke filter
def trackFilter(trackNum, seqNum):
    global pingTracker1
    global pingTracker2
    if trackNum == 1:
        pingTracker1 = [i for i in pingTracker1 if (i >= seqNum)]
    else: # trackNum == 2
        pingTracker2 = [i for i in pingTracker2 if (i >= seqNum)]

# TCP handler
def tcpHandler():
    global successor1
    global successor2
    global predecessor1
    global predecessor2
    global quitFlag
    global pingTracker1
    global pingTracker2
    global seqNum1
    global seqNum2

    connection, address = tcpSocket.accept()
    data = connection.recv(1024).decode().strip()
    splitData = data.split(' ')

    # graceful quit confirm
    if data == 'gracefulQuit':
        if quitFlag == False:
            quitFlag = True
        else:
            sys.exit()

    # successor request
    elif data == 'successorRequest':
        connection.send(successor1.encode())

    # 0=request/response 1=filename 2=requester
    elif len(splitData) == 3:
        if splitData[0] == 'fileRequest':
            fileLocation(splitData[1], splitData[2])
        elif splitData[0] == 'fileResponse':
            print('Received a response message from Peer', splitData[2] + ', which has the file', splitData[1] + '.')

    # successors/predecessors update(after quit)
    elif len(splitData) == 4: # 0=successor/predecessor, 1=left peer, 2=new successor1/predecessor1, 3=new successor2/predecessor2
        if splitData[0] == 'predecessorUpdate':
            print('Peer', splitData[1], 'will depart from the network.')
            if int(splitData[1]) == int(successor1):
                pingTracker1 = []
                seqNum1 = 0
            elif int(splitData[1]) == int(successor2):
                pingTracker2 = []
                seqNum2 = 0
            successor1 = splitData[2]
            successor2 = splitData[3]
            print('My first successor is now peer', successor1 + '.')
            print('My second successor is now peer', successor2 + '.')
            gracefulQuit(splitData[1])
        elif splitData[0] == 'successorUpdate':
            predecessor1 = splitData[2]
            predecessor2 = splitData[3]
    connection.close()

# Send quit confirm
def gracefulQuit(leftPeer):
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpSocket.connect((host, basePort + int(leftPeer)))
    tcpSocket.send('gracefulQuit'.encode())
    tcpSocket.close()

# Hash function
def hashFile(fileName):
    return (int(fileName) % 256)

# File location
def fileLocation(fileName, requester):
    hash = hashFile(fileName)
    if int(hash) <= int(peerId):
        if int(peerId) > int(predecessor1) or int(peerId) < int(successor1):
            print('File', fileName, 'is here.')
            if requester != peerId:
                print('A response message, destined for peer', requester + ', has been sent.')
                fileResponse(fileName, requester)
    elif int(peerId) < int(predecessor1):
        print('File', fileName, 'is here.')
        if requester != peerId:
            print('A response message, destined for peer', requester + ', has been sent.')
            fileResponse(fileName, requester)
    else:
        print('File', fileName, 'is not stored here.')
        fileRequest(fileName, requester)

# File request sender
def fileRequest(fileName, requester):
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpSocket.connect((host, basePort + int(successor1)))
    tcpSocket.send(str('fileRequest ' + fileName + ' '+ requester).encode())
    print('File request message has been forwarded to my successor.')
    tcpSocket.close()

# File response sender
def fileResponse(fileName, requester):
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpSocket.connect((host, basePort + int(requester)))
    tcpSocket.send(str('fileResponse ' + fileName + ' '+ peerId).encode())
    tcpSocket.close()

# Quit handler
def quitHandler():
    # update first predecessor
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpSocket.connect((host, basePort + int(predecessor1)))
    tcpSocket.send(str('predecessorUpdate ' + peerId + ' ' + successor1 + ' '+ successor2).encode())
    tcpSocket.close()

    # update second predecessor
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpSocket.connect((host, basePort + int(predecessor2)))
    tcpSocket.send(str('predecessorUpdate ' + peerId + ' '+ predecessor1 + ' '+ successor1).encode())
    tcpSocket.close()

    # update first successor
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpSocket.connect((host, basePort + int(successor1)))
    tcpSocket.send(str('successorUpdate ' + peerId + ' '+ predecessor1 + ' '+ predecessor2).encode())
    tcpSocket.close()

    # update second successor
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpSocket.connect((host, basePort + int(successor2)))
    tcpSocket.send(str('successorUpdate ' + peerId + ' ' + successor1 + ' ' + predecessor1).encode())
    tcpSocket.close()

def ungracefulQuit(killedSuccessor):
    global successor1
    global successor2
    global pingTracker1
    global pingTracker2
    global seqNum1
    global seqNum2

    # first successor is killed ungracefully
    if killedSuccessor == 'first':
        print('Peer', successor1, 'is no longer alive.')
        successor1 = successor2
        print('My first successor is now peer', successor1 + '.')
        tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcpSocket.connect((host, basePort + int(successor1)))
        tcpSocket.send('successorRequest'.encode())
        successor2 = str(tcpSocket.recv(1024).decode())
        print('My second successor is now peer', successor2 + '.')
        tcpSocket.close()
        pingTracker1 = []
        pingTracker2 = []
        seqNum1 = 0
        seqNum2 = 0

    # second successor is killed ungracefully
    else: # killedSuccessor == 'second'
        print('Peer', successor2, 'is no longer alive.')
        print('My first successor is now peer', successor1 + '.')
        tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcpSocket.connect((host, basePort + int(successor1)))
        tcpSocket.send('successorRequest'.encode())
        successor2 = str(tcpSocket.recv(1024).decode())
        print('My second successor is now peer', successor2 + '.')
        tcpSocket.close()
        pingTracker2 = []
        seqNum2 = 0

# Set host and base port
host = '127.0.0.1'
basePort = 50000
peerId = ''
pingTime = 10
quitFlag = False
seqNum1 = 0
seqNum2 = 0
successor1 = ''
successor2 = ''
predecessor1 = ''
predecessor2 = ''
udpSocket = ''
tcpSocket = ''
pingTracker1 = []
pingTracker2 = []
timeout1 = ''
timeout2 = ''

# Main function
def mainFunction():
    global timeout1
    global timeout2

    initialization()
    while True:
        try:
            if select.select([sys.stdin, ], [], [], 1)[0]:
                input = sys.stdin.readline().strip()
                splitInput = input.split(' ')
                if len(splitInput) == 2:
                    if splitInput[0] == 'request':
                        fileLocation(splitInput[1], peerId)
                elif input == 'quit':
                    quitHandler()
                else:
                    print('[' + input + ']' + 'is invalid command.')

            sockets = [udpSocket, tcpSocket]
            inputSocket = select.select(sockets, [], [], 1)[0]
            for socket in inputSocket:
                if socket == udpSocket:
                    udpHandler()
                elif socket == tcpSocket:
                    tcpHandler()
            if int(time.time()) % pingTime == 0: # ping frequency
                pingRequest(successor1, 1)
                pingRequest(successor2, 2)
                time.sleep(1)
            if len(pingTracker1) > 2: # if 3 or more consecutive pings not responded
                if timeout1 == '':
                    timeout1 = float(time.time())
                if (float(time.time()) - timeout1) > 4: # if wait 4 seconds, there will be an ungraceful quit
                    ungracefulQuit('first')
            else:
                timeout1 = ''
            if len(pingTracker2) > 3:
                if timeout2 == '':
                    timeout2 = float(time.time())
                if (float(time.time()) - timeout2) > 5:
                    ungracefulQuit('second')
            else:
                timeout2 = ''
        except KeyboardInterrupt:
            print('Ctrl-C is pressed...')
            sys.exit('This peer is killed ungracefully.')

if __name__ == '__main__':
    mainFunction()

