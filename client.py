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

	def requestOrResponse(self, message):
		requestOrResponse = [
			self._size(message),
			message
		]

		return ''.join(requestOrResponse)

	# RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage	
	def requestMessage(self, apiKey, apiVersion, correlationId, clientId, requestMessage):
		requestMessage = [
			self._int16(apiKey), 
			self._int16(apiVersion), 
			self._int32(correlationId), 
			self._string(clientId),
			requestMessage
		]

		return ''.join(requestMessage)


	# MessageSet => [Offset MessageSize Message]
	# MessageSets are not preceded by an int32 like other array elements in the protocol
	def messageSet(self, messages):
		messageSet = []
		for message in messages:
			msgContent = self.message(message['timestamp'], message['key'], message['value'])

			message = [
				self._int64(message['offset']),
				self._size(msgContent),
				msgContent
			]

			messageSet.append(''.join(message))

		return ''.join(messageSet)


	# Message => Crc MagicByte Attributes Key Value
	def message(self, timestamp, key, value):
		message = [
			self._int8(self.MAGIC_BYTE),
			#@TODO attributes: compression and timestamp type, all bits set to 0 for now
			self._int8(0),
			self._int64(timestamp),
			self._bytes(key),
			self._bytes(value)
		]

		crc = zlib.crc32(''.join(message))
		return self._int32(crc) + ''.join(message)


	# ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
	# For now produce can be made only on one topic and one partition at a time 
	# @TODO to be refactored to fit protocol
	def produceRequest(self, requiredAcks, timeout, topicName, partition, messageSet):
		produceRequest = [
			self._int16(requiredAcks),
			self._int32(timeout),
			# encode topic array
			self._int32(1),
			self._string(topicName),
			# encode partitions array
			self._int32(1),
			self._int32(partition),

			self._size(messageSet),
			messageSet
		]

		return ''.join(produceRequest)

	# bytes type as defined in kafka protocol
	# string size N as a signed int16 stored in big endian followed by N bytes   
	def _bytes(self, string):
		length = len(string)
		return struct.pack('>l%dc' % (length), length, *string) 


	# string type as defined in kafka protocol
	# string size N as a signed int32 stored in big endian followed by N bytes 
	def _string(self, string):
		length = len(string)
		return struct.pack('>h%dc' % (length), length, *string)


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
produceRequest = client.produceRequest(1, 1000, 'live2', 1, messageSet)
requestMessage = client.requestMessage(client.PRODUCE_REQUEST, client.API_VERSION, 0, client.CLIENT_ID, produceRequest)
request = client.requestOrResponse(requestMessage)

printHex(request)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('192.168.33.33', 9092))
s.send(request)

response = s.recv(1024)
 
printHex(response)