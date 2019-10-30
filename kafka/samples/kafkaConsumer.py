# IMPORTING THE LIBRARIES REQUIRED FOR KAFKA PRODUCER CODE
import sys
import uuid
import subprocess
from kafka import KafkaConsumer

'''
PARAMETERS SETTING FOR KAFKA PRODUCER CODE

'''
# SETTING THE KAFKA SERVER NAME AND PORT
serverName = "bootstrap_servers:port"

# SETTING THE KAFKA TOPIC
kafkaTopic = 'input-topic'

# SETTING THE COUNTER TO COUNT THE NUMBER OF MESSAGES
counter = 0

# SETTING UP KAFKA CONSUMER WITH KAFKA TOPIC BOOTSTRAP SERVER PARAMETERS
consumer = KafkaConsumer(kafkaTopic , bootstrap_servers=serverName , auto_offset_reset='earliest')


# STARTING THE BLOCK TO READ MESSAGES FROM KAFKA AND COUNT
try:
    
    # STARTING THE LOOP TO PROCESS EACH MESSAGE AND COUNT THEM
    for message in consumer:
        counter = counter + 1
        print("Input Data: "+ str(message.value)) 
	data = message.value
		
	fileName = str(uuid.uuid4()) + ".txt"
	print("Storing into file")
	f= open(fileName,"w+")
	f.write(data)
	f.close()
	print("Storing into file done. File Name: "+fileName)

	print("Storing into HDFS")
	subprocess.call(["hdfs", "dfs", "-put", fileName, "hdfs-path"])
    # PRINTING THE NUMBER OF MESSAGES
    print ("Total no. of messages: "+ str(counter))

# STARTING THE EXCEPT BLOCK
except KeyboardInterrupt:
    sys.exit()

