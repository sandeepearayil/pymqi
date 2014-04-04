# See discussion and more examples at http://packages.python.org/pymqi/examples.html
# or in doc/sphinx/examples.rst in the source distribution.

# Original Package : ttp://packages.python.org/pymqi/examples.html
# Addition: dis_attributes.py
# Author : Sandeep Earayil

# The intention behind this module is to provide a REST API for extracting details out of an IBM websphere MQ Queue manager.
# A JSON feed is produced for each of the queries. For e.g. an query on the Queue attributes like display_queues produces a JSOON feed
# of name value pairs.

import logging

import pymqi
import CMQC, CMQCFC, CMQXC

logging.basicConfig(level=logging.INFO)



# Decorator for connection to the Queue Manager
def connect_qmgr(query_func):
    # Callable method for the __call__
    def query_qmgr():
        global qmgr, pcf
        queue_manager = "QM01"
        channel = "CHANNEL.1"
        host = "127.0.0.1"
        port = "1434"
        conn_info = "%s(%s)" % (host, port)
        qmgr = pymqi.connect(queue_manager, channel, conn_info)
        pcf = pymqi.PCFExecute(qmgr)
        query_func()
        qmgr.disconnect()
    return query_qmgr


# Displays all the queues and their attributes on the queue manager
# @connect_qmgr is the decorator for the connection to the queue manager.
# List comprehension is used to generate a dicionary of key value pairs of queue attributes. More attributes can be added with relevnt UNIX commands
@connect_qmgr
def display_queues():
    prefix = "*"
    try:
        args = {CMQC.MQCA_Q_NAME: prefix,
                CMQC.MQIA_Q_TYPE: CMQC.MQQT_ALL}
        response = pcf.MQCMD_INQUIRE_Q(args)
        # For Dumps print out the response
        #print(response)
    except pymqi.MQMIError, e:
        if e.comp == CMQC.MQCC_FAILED and e.reason == CMQC.MQRC_UNKNOWN_OBJECT_NAME:
            logging.info("No queues matched given arguments.")
            else:
                raise
                # Raise relevant error
    else:
        # List comprehension used to create a list of Queue Attributes. A dictionary of name value pairs similar to a JSON feed
        queue_att = [{"Queue Name":queue_info[CMQC.MQCA_Q_NAME],"Description":queue_info[CMQC.MQCA_Q_DESC],"Queue Type":queue_info[CMQC.MQIA_Q_TYPE]} for queue_info in response]
        print (queue_att)
        return (queue_att)

# Displays all the topics and their attributes on the queue manager
# @connect_qmgr is the decorator for the connection to the queue manager.
# List comprehension is used to generate a dicionary of key value pairs of topic attributes. More attributes can be added with relevnt UNIX commands

@connect_qmgr
def display_topics():
    prefix = "*"
    try:
        args = {CMQC.MQCA_TOPIC_NAME: prefix}
        t_response = pcf.MQCMD_INQUIRE_TOPIC(args)
        # For Dumps print out the response
        #print(t_response)
    except pymqi.MQMIError, e:
        if e.comp == CMQC.MQCC_FAILED and e.reason == CMQC.MQRC_UNKNOWN_OBJECT_NAME:
            logging.info("No topics matched given arguments.")
            else:
                raise
                # Raise relevant error
    else:
        # List Comprehension used to extract the topic and relevent attributes. More attributes can be extracted in the similar fashion.
        topic_att = [{"Topic Name":t_att[CMQC.MQCA_TOPIC_NAME],"Topic Type":str(t_att[CMQC.MQIA_TOPIC_TYPE]),"Description":t_att[CMQC.MQCA_TOPIC_DESC]} for t_att in t_response]
        print (topic_att)
        return (topic_att)
        
        
# @connect_qmgr is the decorator for the connection to the queue manager.
# List comprehension is used to generate a dicionary of key value pairs of subscriber attributes. More attributes can be added with relevnt UNIX commands

@connect_qmgr    
def display_subscribers():
    prefix = "*"
    try:
        args = {CMQC.MQCACF_SUB_NAME: prefix}
        s_response = pcf.MQCMD_INQUIRE_SUBSCRIPTION(args)
        # For Dumps print out the response
        #print(t_response)
    except pymqi.MQMIError, e:
        if e.comp == CMQC.MQCC_FAILED and e.reason == CMQC.MQRC_UNKNOWN_OBJECT_NAME:
            logging.info("No Subscriptions matched given arguments.")
            else:
                raise
                # Raise relevant error
    else:
        # List comprehension used
        sub_att =[{"Subscriber Name":s_att[CMQCFC.MQCACF_SUB_NAME],"Destiantion Queue":s_att[CMQCFC.MQCACF_DESTINATION],"Destination Queue Manager":s_att[CMQCFC.MQCACF_DESTINATION_Q_MGR],"Topic String":s_att[CMQC.MQCA_TOPIC_STRING]} for s_att in s_response]
        print (topic_att)
        return (sub_att)
    

# @connect_qmgr is the decorator for the connection to the queue manager.
# List comprehension is used to generate a dicionary of key value pairs of queue statistics. More attributes can be added with relevnt UNIX commands

@connect_qmgr    
def display_queuestats():
    prefix = "*"
    try:
        args = {CMQC.MQCA_Q_NAME: prefix,
                CMQC.MQIA_Q_TYPE: CMQC.MQQT_ALL}
        q_response = pcf.MQCMD_INQUIRE_Q_STATUS(args)
        # For Dumps print out the response
        #print(q_response)
    except pymqi.MQMIError, e:
        if e.comp == CMQC.MQCC_FAILED and e.reason == CMQC.MQRC_UNKNOWN_OBJECT_NAME:
            logging.info("No queues matched given arguments.")
            else:
                raise
                # Raise relevant error
    else:
        # List comprehension used to create a list of Queue Statistics. More stats can be added in the similar fashion.
        queue_stats = [{"Queue Name":q_stats[CMQC.MQCA_Q_NAME],"PUT":q_stats[CMQCFC.MQCACF_LAST_PUT_DATE],"GET":q_stats[CMQCFC.MQCACF_LAST_GET_DATE]} for q_stats in q_response]
        print (queue_stats)
        return (queue_stats)
    
    
def start_agent():
    display_queues()
    display_topics()
    display_subscribers()
    display_queuestats()
    


if __name__ == "__main__":start_agent()
