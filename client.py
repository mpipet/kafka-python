#!/usr/bin/env python
# -*- coding: utf-8 -*-

import struct
import zlib
import socket

def printHex(buff):
	print "".join("{:02x}".format(ord(char)) for char in buff)


# Client class prototype for
class Client:

	CLIENT_ID = 'KAFKA_PYTHON'
	API_VERSION = 0

	MAGIC_BYTE = 1

	PRODUCE_REQUEST = 0

	# RequestMessage => ApiKey ApiVersion CorrelationId ClientId	
	def header(self, apiKey, apiVersion, correlationId, clientId):
		buff = self._int16(apiKey) 
		buff += self._int16(apiVersion) 
		buff += self._int32(correlationId) 
		buff += self._string(clientId)

		return buff


	# MessageSet => [Offset MessageSize Message]
	# MessageSets are not preceded by an int32 like other array elements in the protocol
	def messageSet(self, messages):

		messageSet = ''
		for message in messages:
			messageBuff = self.message(message['timestamp'], message['key'], message['value'])

			messageSet += self._int64(message['offset'])
			messageSet += self._size(messageBuff)
			messageSet += messageBuff

		return messageSet


	# Message => Crc MagicByte Attributes Key Value
	def message(self, timestamp, key, value):

		message = self._int8(self.MAGIC_BYTE)
		#@TODO attributes: compression and timestamp type, all bits set to 0 for now
		message += self._int8(0)
		message += self._int64(timestamp)
		message += self._bytes(key)
		message += self._bytes(value)

		crc = zlib.crc32(message)

		message = self._int32(crc) + message		

		return message

	# ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
	# For now produce can be made only on one topic and one partition at a time 
	# @TODO to be refactored to fit protocol
	def produceRequest(self, requiredAcks, timeout, topicName, partition, messageSet):

		header = self.header(self.PRODUCE_REQUEST, self.API_VERSION, 0, self.CLIENT_ID)

		buff = self._int16(requiredAcks) 
		buff += self._int32(timeout) 

		# encode topic array
		buff += self._int32(1)
		buff += self._string(topicName)

		# encode partitions array
		buff += self._int32(1)
		buff += self._int32(partition)

		buff += self._size(messageSet)
		buff += messageSet


		produceRequest = self._size(header + buff)
		produceRequest +=  header + buff

		return produceRequest


	# bytes type as defined in kafka protocol
	# string size N as a signed int16 stored in big endian followed by N bytes   
	def _bytes(self, string):
		return struct.pack('>l', len(string)) + string 


	# string type as defined in kafka protocol
	# string size N as a signed int32 stored in big endian followed by N bytes 
	def _string(self, string):
		return struct.pack('>h', len(string)) + string 


	def _size(self, buff):
		return struct.pack('>l', len(buff))	


	# return int16 type as defined in kafka protocol
 	# int8 stored in big endian 
	def _int8(self, integer):
		return struct.pack('>b', integer)


	# return int16 type as defined in kafka protocol
	# int16 stored in big endian  
	def _int16(self, integer):
		return struct.pack('>h', integer)


	# return int32 type as defined in kafka protocol
	# int32 stored in big endian  
	def _int32(self, integer):
		return struct.pack('>l', integer)

	# return int64 type as defined in kafka protocol
	# int64 stored in big endian 
	def _int64(self, integer):
		return struct.pack('>q', integer)

	# return N number of structs in an array
	# N is a kafka protocol int32 
	def _array(self, array):
		return self._int32(len(array))


client = Client()

messages = [{
	'offset': 10000,
	'timestamp': 0,
	'key': 'erg',
	'value': 'loooolfzefzefzfzf'
}]

messageSet = client.messageSet(messages)
buff = client.produceRequest(1, 1000, 'live2', 1, messageSet)		
printHex(buff)


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('192.168.33.33', 9092))
s.send(buff)

response = s.recv(1024)
 
printHex(response)