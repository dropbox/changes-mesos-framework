#!/usr/bin/env python

import json
from pprint import pprint

from flask import jsonify, Flask, Response, request


app = Flask(__name__)

@app.route("/")
def index():
  return "Mesos HTTP Proxy test service."

@app.route("/offer", methods = ['POST'])
def offer():
  pprint(request.form)
  tasks_to_run = []

  tasks_to_run.append(
    {
      "id": "my_job",
      "cmd": ["echo hello world"],
      "resources": {
        "cpus": 0.25,
        "mem": 64
      }
    }
  )

  return Response(json.dumps(tasks_to_run),  mimetype='application/json')

if __name__ == "__main__":
  app.debug = True
  app.run(host='0.0.0.0')
