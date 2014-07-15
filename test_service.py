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
  REQUIRED_MEM = 800
  REQUIRED_CPU = 0.5

  tasks_to_run = []

  info = request.get_json()

  if info["resources"]["cpus"] > REQUIRED_CPU \
     and info["resources"]["mem"] > REQUIRED_MEM:

    tasks_to_run.append(
      {
        "id": "my_job",
        "cmd": "pwd && /bin/sleep 300",
        "resources": {
          "cpus": REQUIRED_CPU,
          "mem": REQUIRED_MEM
        }
      }
    )

  return Response(json.dumps(tasks_to_run),  mimetype='application/json')

if __name__ == "__main__":
  app.debug = True
  app.run(host='0.0.0.0')
