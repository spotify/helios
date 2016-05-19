# -*- coding: utf-8 -*-
# Copyright (c) 2015 Spotify AB

import socket
import asyncore
import time
import sys
import os

PORT=33333
TIMEOUT=60
connected=False
closed=False

class Server(asyncore.dispatcher):
    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(('', port))
        self.listen(1)

    def handle_accept(self):
        # when we get a client connection start a dispatcher for that
        # client
        socket, address = self.accept()
        print 'Accepted connection from', address
        # Don't accept more incoming connections
        self.close()
        global connected
        connected=True
        Handler(socket)

class Handler(asyncore.dispatcher_with_send):
    def handle_close(self):
        print "Connection closed"
        global closed
        closed=True
        self.close()

    def handle_read(self):
        # Echo any data received
        self.out_buffer = self.recv(1024)
        if not self.out_buffer:
            self.close()

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

print "Listening for connection on port {}...".format(PORT)

s = Server('', PORT)

t0 = time.time()

while True:
    asyncore.loop(timeout=1, count=1)
    if closed:
        print "Exiting -- connection closed"
        break
    if connected == False and time.time() - t0 >= TIMEOUT:
        print "Exiting -- no connection established within timeout"
        break
