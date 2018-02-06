#!/usr/bin/python

import requests
import json
from time import sleep

def runJob(function, data):
  r = requests.post("http://localhost:2004/v2/api/functions/" + function + "/jobs", json = data)
  job_id = r.json()["id"]
  finished = False
  while not finished:
    details = requests.get("http://localhost:2004/v2/api/jobs/" + job_id).json()
    status = details["status"]
    if status == "finished":
      return {"id": job_id, "result": details["jobResult"]}
    elif status == "failed":
      raise Exception("Job is failed with:" + details["jobResult"])
    elif status == "canceled":
      raise Exception("Job is canceled")
    else:
      sleep(1)

result = runJob("simple-context", {"numbers": [1,2,3,4,5]})
print("Result is: " + json.dumps(result))
