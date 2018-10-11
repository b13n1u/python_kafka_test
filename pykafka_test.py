#!/usr/bin/env python

import argparse
import time
import datetime
import sys

from pykafka import KafkaClient

def producer(topic, bootstrap, console=False, file=None, test=False):
    client = KafkaClient(hosts="%s" % bootstrap)
    topic = client.topics['%s' % topic]

    if console == True:
        with topic.get_sync_producer() as producer:
            while True:
                producer.produce('%s' % sys.stdin.readline())
    elif test == True:
        with topic.get_sync_producer() as producer:
            i = 0
            while True:
                i += 1
                msg = "%s %s %d" % (datetime.datetime.now().strftime('%d%m%Y_%H%M%S%f'), "TEST", i)
                print msg
                producer.produce('%s' % msg)
                time.sleep(1)

    elif file != None:
        with topic.get_sync_producer() as producer:
            with open("%s" % file) as f:
                while True:                                    #:D:):|:\:(:,(
                    line = f.readline()
                    producer.produce('%s' % line)
                    if not line: break

def consumer(topic, bootstrap, console=False, file=None):
    now = datetime.datetime.now().strftime('%d%m%Y_%H%M%S%f')
    client = KafkaClient(hosts="%s" % bootstrap)
    topic = client.topics['%s' % topic]
    consumer = topic.get_simple_consumer()
    if console == True:
        for message in consumer:
            if message is not None:
                print message.offset, message.value
    elif file !=None:
        with open('%s' % file, 'a',buffering=0) as f:
            for message in consumer:
                #print message.value
                f.write("%s: %s" % (message.offset, message.value))
                f.flush()
                if message.value == "..EOF..\n":
                    f.close()
                    print "Found eof. Exit."
                    sys.exit(0)

def main():
    now = datetime.datetime.now().strftime('%d%m%Y_%H%M%S')

    p = argparse.ArgumentParser(description='Simple producer consumer script.')

    sub = p.add_subparsers(help='commands', dest='mode')

    produce = sub.add_parser('produce', help='Producer settings')
    produce.add_argument('-c', '--console', help='Produce from console', action='store_true')
    produce.add_argument('-f', '--file', help='Produce from file')
    produce.add_argument('-t', '--topic', help='Topic to produce to')
    produce.add_argument('-g', '--generate', help='generate messsgaes', action='store_false')
    produce.add_argument('-b', '--bootstrap', help='List of kafka servers')

    consume = sub.add_parser('consume', help='Consumer settings')
    consume.add_argument('-c', '--console', help='Consume to console', action='store_true')
    consume.add_argument('-f', '--file', help='Consume to file', default=None)
    consume.add_argument('-t', '--topic', help='Topic to consume from')
    consume.add_argument('-b', '--bootstrap', help='List of kafka servers')


    args = p.parse_args()

   if args.mode == 'produce':
        producer(args.topic, args.bootstrap, args.console, args.file, args.generate)
    elif args.mode == 'consume':
        consumer(args.topic, args.bootstrap, args.console, args.file)

if __name__ == "__main__":
    main()
