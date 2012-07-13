#!/usr/bin/python

import sys
import json
import pyjaccson
#from Geometric_Computations import Geometric_Computations


c = pyjaccson.Connection('localhost:6060')

hyperbins_vert = c.getCollection('hyperbins_vert')
hyperintersections = c.getCollection('hyperintersections')


#hiconn = DBconnector.get_pa_conn('database-sects.cfg').patent_grants
#hyperintersections = hiconn.hyperintersections # will need to create this .cfg
#hitemp = hiconn.hitemp


def intersect_cluster_freqs(args):
	
	global hyperbins_vert, hyperintersections#, articles, CW, hyperintersections
	
	article_id, bins = args

	counts = {}
	#for b in range(10):
	#	docs = hyperbins_vert.find({"wheel_bin": {'$in': bins[len(bins)/10*b:len(bins)/10*(b+1)]}},
	#	{"article_id": True,"wheel_bin": {'$slice': [len(bins)/10*b,len(bins)/10*(b+1)]}})
	#	set_bins = set(bins[len(bins)/10*b:len(bins)/10*(b+1)])
	for doc in hyperbins_vert.find({'wheel_bin':{'$in': bins}})
		counts[doc['_id']] = counts.get(doc['_id'],0) + len(set_bins & set(doc['wheel_bin']))
	
	intersects = [i for i in counts.items() if i[1]>1 and i[0] != article_id]

	# reverse relationships
	
	for i in intersects:
		#sys.stderr.write(str({"_id": i[0]}) + ' ' + str({'$set': {"sects."+article_id: i[1]}}))
		#reliable_update_or_die(hyperintersections, {"_id": i[0]}, {'$set': {"sects."+article_id: i[1]}})
		#hitemp.insert({"doc_id":i[0], article_id: i[1]})
		hyperintersections.update({"_id": i[0]},{'$set': {"sects."+article_id: i[1]}})

	#hibw.add_and_report_or_die({"_id": article_id, "sects": dict(intersects)})
	hyperintersections.insert({"_id": article_id, "sects": dict(intersects)})


for doc in hyperbins_vert.find():
	
	intersect_cluster_freqs((doc['_id'], doc['wheel_bin']))
	
hyperintersections.flush()


