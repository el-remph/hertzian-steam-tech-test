#!/usr/bin/python
import datetime
import json
import jsonschema
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

schema = {
	"type": "array",
	"items": {
		"type": "object",
		"properties": {
			"id":	{"type": "string"},	# should be an integer, but steam gives it as
										# a string so maybe they know something
			"author":	{"type": "string"},	# actually a hex string... should we store
											# as an integer?
			"date":	{"type": "string", "format": "date"},
			"hours":	{"type": "integer"},
			"content":	{"type": "string"},
			"comments":	{"type": "integer"},
			"helpful":	{"type": "integer"},
			"funny":	{"type": "integer"},
			"recommended":	{"type": "boolean"}
		}
	}
}

def steam_api_request(steamid):
	reviews = []
	cursor = '*'
	n_requested = 100 # max supported by steam as of 2024-10-02

	while True:
		r = requests.get("https://store.steampowered.com/appreviews/{:d}".format(steamid),
						params={'json':1, 'num_per_page':n_requested, 'cursor':cursor})
		r.raise_for_status()
		response_obj = r.json()
		if not response_obj['success']:
			raise Exception('bad response')

		reviews += [Review(x).obj for x in response_obj['reviews']]

		n_received = response_obj['query_summary']['num_reviews']
		assert n_received == len(response_obj['reviews'])
		if n_received != n_requested:
			break
		cursor = response_obj['cursor']

	jsonschema.validate(reviews, schema,
					format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER)
	return reviews

# test
print(json.dumps(steam_api_request(1158310), indent="\t"))
