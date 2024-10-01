#!/usr/bin/python
import datetime
import json
import hashlib
import requests

# TODO: schema

class Review:
	# obj is a decoded json dict from the reviews array
	def __init__(self, obj):
		# TODO: in perl, an object is just syntactic sugar for a dict. Can
		# we get a dict from the actual name table of the object itself?
		self.obj = {
			'id'		: obj['recommendationid'],
			'author'	: hashlib.sha256(obj['author']['steamid'].encode('utf-8')).hexdigest(),
			# TODO: UTC? timestamp_updated or timestamp_created?
			'date'		: datetime.date.fromtimestamp(obj['timestamp_updated']).isoformat(),
			'hours'		: obj['author']['playtime_at_review'], # TODO: check presumption
			'content'	: obj['review'],
			'comments'	: obj['comment_count'],
			# TODO: source?
			'helpful'	: obj['votes_up'],
			'funny'		: obj['votes_funny'],
			'recommended' : obj['voted_up'] # apparently
			# TODO: franchise and gameName -- are they really to be stored
			# separately for each review?
		}

	def __str__(self):
		return json.dumps(self.obj)

def somethingelse(response_obj):
	return [Review(x).obj for x in response_obj['reviews']]

def steam_api_request(steamid):
	r = requests.get("https://store.steampowered.com/appreviews/{:d}".format(steamid), params={'json':1})
	r.raise_for_status()
	return json.dumps(somethingelse(r.json()))

# test (doesn't call steam_api_request because that would be rude; opens
# cached output instead)
print(json.dumps(somethingelse(json.load(open("1158310.json", "r"))), indent="\t"))
