import requests
import os
import json
import time
from datetime import datetime, timedelta

endpoint="https://api.twitter.com/2/tweets/search/all"

def auth():
	with open(".token", "r") as fh:
		return fh.read().strip()
	#return os.environ.get("BEARER_TOKEN")


def create_url():
	#max query is 1024 characters!
	#next_token:
	# Tweet fields are adjustable.
	# Options include:
	# attachments, author_id, context_annotations,
	# conversation_id, created_at, entities, geo, id,
	# in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
	# possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
	# source, text, and withheld
	url = "https://api.twitter.com/2/tweets/search/all"
	return url


def create_headers(bearer_token):
	headers = {"Authorization": "Bearer {}".format(bearer_token)}
	return headers

class BadStatusCode(Exception):
	def __init__(self,status_code):
		self.status_code=status_code
	def __str__(self):
		return f"BadStatusCode: {self.status_code}"

def connect_to_endpoint(url, headers, params):
	count=0
	while True:
		try:
			response = requests.request("GET", url, headers=headers, params=params)
			if response.status_code == 200:
				return response.json()
			else:
				print(response.text)
				raise BadStatusCode(response.status_code)
		except Exception as e:
			count+=1
			if (isinstance(e,BadStatusCode) and e.status_code==400):
				print("Error. count={}, {}".format(count,e))
				quit()
			if (isinstance(e,BadStatusCode) and e.status_code==429):
				print("Quota up. Waiting 15 minutes.")
				# we'll wait 15 minutes and try again
				time.sleep(15*60)
				response = requests.request("GET", url, headers=headers, params=params)
				if response.status_code == 200:
					return response.json()
				else:
					print(response.text)
					raise BadStatusCode(response.status_code)
		
			if count<2 or (isinstance(e,BadStatusCode) and e.status_code==503):
				#https://developer.twitter.com/en/support/twitter-api/error-troubleshooting
				#503 is "Service Unavailable"  (temporarily overloaded) -- we'll ignore this any number of times
				print("Error. count={}, {}".format(count,e))
				time.sleep(count*5)
			else:
				raise e
			
	return response.json()

def hydrate(ids,token):
    headers = create_headers(token)
    url = "https://api.twitter.com/2/tweets"
    params={
            "ids":",".join(ids),
			"tweet.fields":"attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld",
			"user.fields":"created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld",
			"expansions":"attachments.poll_ids,attachments.media_keys,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id",
			"place.fields":"contained_within,country,country_code,full_name,geo,id,name,place_type",
			"media.fields":"duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics,non_public_metrics,organic_metrics,promoted_metrics",
			
		}
    json_response = connect_to_endpoint(url, headers, params)
    return json_response

def get_tweets(query,start_time,end_time,directory,token):
	print(directory)
	url = "https://api.twitter.com/2/tweets/search/all"
	headers = create_headers(token)
	params={
			"query":query,
			"max_results":500,
			"start_time":start_time,
			"end_time":end_time,
			"tweet.fields":"attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld",
			"user.fields":"created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld",
			"expansions":"attachments.poll_ids,attachments.media_keys,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id",
			"place.fields":"contained_within,country,country_code,full_name,geo,id,name,place_type",
			"media.fields":"duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics,non_public_metrics,organic_metrics,promoted_metrics",
			"poll.fields":"duration_minutes,end_datetime,id,options,voting_status",
		}
	i=0
	while True:
		outfile="{}/output_fogocruzado{:05d}.json".format(directory,i)
		# ADDED
		replies="{}/output_testimonial{:05d}.json".format(directory,i)
		try:
			with open(outfile,"r") as fh:
				json_response=json.load(fh)
		except:
			json_response = connect_to_endpoint(url, headers, params)
			with open(outfile,"w") as fh:
				json.dump(json_response, fh, indent=4, sort_keys=True)
				time.sleep(2)
			# ADDED
			with open(replies,"w") as fh:
					testimonial_ids=[]
					try:
						for tweet in json_response["data"]:
							testimonial_ids.append(tweet['referenced_tweets'][0]['id'])
					except:
						print(f"No data in {outfile}")
					# get replies
					if len(testimonial_ids)>0 and len(testimonial_ids)<=100:
						json_response_reply = hydrate(testimonial_ids,token)
						json.dump(json_response_reply, fh, indent=4, sort_keys=True)
					elif len(testimonial_ids)==0:
						print(f"No replies in {outfile}")
					elif len(testimonial_ids)>100:
						# split into chunks of 100
						testimonial_ids_chunks = [testimonial_ids[x:x+100] for x in range(0, len(testimonial_ids), 100)]
						for chunk in testimonial_ids_chunks:
							json_response_reply = hydrate(chunk,token)
							json.dump(json_response_reply, fh, indent=4, sort_keys=True)
					time.sleep(2)
		i+=1
		if i%100==0:
			print(directory,i)
		#if i>...:
		#	print("Ending to avoid quota")
		#	return i
		try:
			params["next_token"]=json_response["meta"]["next_token"]
		except:
			print("No next_token!")
			return i



def main():
	token = auth()
	
	dates=[]
	# start=datetime(2023,4,8)
	start = datetime.now()+timedelta(days=-1)
	end=datetime(2016,1,1)
	date=start
	while date>end:
		dates.append(date)
		date+=timedelta(days=-1)
	# print(dates)
	global_count=0
	query="(from:fogocruzadorj is:reply)"
	print(query)
	assert len(query)<1024, f"Query too long {len(query)}"
	#quit()
	for day in dates:
		str_startday=day.strftime("%Y-%m-%d")
		str_endday=(day+timedelta(days=1)).strftime("%Y-%m-%d")
		os.makedirs(str_startday, exist_ok=True)
		global_count+=get_tweets(query,start_time=str_startday+"T00:00:00Z",end_time=str_endday+"T00:00:00Z",directory=str_startday,token=token)
		if global_count>100000:
			print("Quota up! Stopping!")
			break
	
	print("DONE",global_count)


if __name__ == "__main__":
	main()

