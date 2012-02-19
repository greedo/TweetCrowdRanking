#!/usr/bin/python

# Joe Cabrera
# Multithreading Searcher and Indexer. 
# One Thread Indexes new documents in the background while thread in the foreground waits for new user queries
# This is a tweetIndexer that indexes streaming Tweet data

# import needed system files
#import sys, time, math, subprocess

# TweetStream - http://pypi.python.org/pypi/tweetstream
import tweetstream

import lucene
from lucene import SimpleFSDirectory, System, File, Document, Field, StandardAnalyzer, IndexWriter, Version, VERSION
from lucene import QueryParser, IndexSearcher
from lucene import IndexReader

import threading, signal, os

# Indexer thread
class Indexer(threading.Thread):

	# set some initial values for the class, the root directory to start indexing and pass in a writer instance
	def __init__(self, root, writer):
		threading.Thread.__init__(self)
		self.root = root
		self.writer = writer
		
	def run(self):
		env.attachCurrentThread()
		stream = tweetstream.SampleStream("username", "password")

		for tweet in stream:
			
                	try:
             									
				contents = unicode(tweet['text'])
				user_name = tweet['user']['screen_name']
				creation_date = str(tweet['created_at'])
				hashtags = tweet['entities']['hashtags']
				
				# One tweet can have multiple hashtags
				for hashtag in hashtags:
				
					# we only want to add documents that contain a hashtag
					if len(hashtag) > 0:
				
						print hashtag
					
						doc = Document()
						doc.add(Field("contents", contents, Field.Store.YES, Field.Index.NOT_ANALYZED))
						doc.add(Field("user_name", user_name, Field.Store.YES, Field.Index.NOT_ANALYZED))
						doc.add(Field("creation_date", creation_date, Field.Store.YES, Field.Index.NOT_ANALYZED))
						doc.add(Field("hashtag", hashtag, Field.Store.YES, Field.Index.ANALYZED))
					
						self.writer.addDocument(doc)
					
						# optimize for fast search and commit the changes
						# this is only really required if we have added a new document
						self.writer.optimize()
						self.writer.commit()
					else:
						pass
						
			except Exception as e: pass
		

# before we close we always want to close the writer to prevent corruptions to the index
def quit_gracefully(*args):
	#indexer.join()
	writer.close()
	
	print "Cleaning up and terminating"
	exit(0)

# main thread for the QueryParser
def run(writer, analyzer):
	while True:
		print 
		print "Hit enter with no input to quit."
		command = raw_input("Query:")
		if command == '':
			return

		print "Searching for:", command
		IndexReader = writer.getReader()
		searcher = IndexSearcher(IndexReader)
		query = QueryParser(Version.LUCENE_CURRENT, "hashtag", analyzer).parse(command)
		scoreDocs = searcher.search(query, 50).scoreDocs
		print "%s total matching documents." % len(scoreDocs)

		for scoreDoc in scoreDocs:
			doc = searcher.doc(scoreDoc.doc)
			print 'tweet:', doc.get("contents")
			print 'user_name:', doc.get("user_name")
			print 'when', doc.get("creation_date")

# main function for everything
if __name__ == '__main__':
	signal.signal(signal.SIGINT, quit_gracefully)
	STORE_DIR = "/var/www/index"
	env=lucene.initVM()
	print 'Using Directory: ', STORE_DIR
	
	notExist = 0
        
        # both the main program and the background indexer will share the same directory and analyzer
	if not os.path.exists(STORE_DIR):
		os.mkdir(STORE_DIR)
		notExist = 1
		
	directory = SimpleFSDirectory(File(STORE_DIR))
	analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
	
	# we will need a writer
	writer = IndexWriter(directory,analyzer,True,IndexWriter.MaxFieldLength.LIMITED)
	writer.setMaxFieldLength(1048576)
	
	if notExist == 1:
		writer.close()
	
	# and start the indexer
	# note the indexer thread is set to daemon causing it to terminate on a SIGINT
	indexer = Indexer(STORE_DIR,writer)
	indexer.setDaemon(True)
	indexer.start()
	print 'Starting Indexer in background...'
	
	run(writer, analyzer)
	quit_gracefully()
