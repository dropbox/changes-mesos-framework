#!/usr/bin/env python

from __future__ import print_function

import json
import random
from pprint import pprint

from flask import jsonify, Flask, Response, request


app = Flask(__name__)

@app.route("/")
def index():
  return "Mesos HTTP Proxy test service."


@app.route("/offer", methods = ['POST'])
def offer():
  print("Received resource offer:")
  print(json.dumps(request.get_json(), sort_keys=True, indent=2, separators=(',', ': ')))

  REQUIRED_MEM = 500
  REQUIRED_CPU = 0.5

  tasks_to_run = []

  info = request.get_json()

  if info["resources"]["cpus"] >= REQUIRED_CPU \
     and info["resources"]["mem"] >= REQUIRED_MEM:

    tasks_to_run.append(
      {
        "id": "my_job_" + str(random.randint(0, 1000)),
        "cmd": "pwd && /bin/sleep " + str(random.randint(10, 60)),
        "resources": {
          "cpus": REQUIRED_CPU,
          "mem": REQUIRED_MEM
        }
      }
    )

  print("Responding with the following tasks:")
  print(json.dumps(tasks_to_run, sort_keys=True, indent=2, separators=(',', ': ')))
  return Response(json.dumps(tasks_to_run),  mimetype='application/json')


@app.route("/status", methods = ['POST'])
def status():
  print("Received status update:")
  print(json.dumps(request.get_json(), sort_keys=True, indent=2, separators=(',', ': ')))
  return "OK"


if __name__ == "__main__":
  app.debug = True
  app.run(host='0.0.0.0')
