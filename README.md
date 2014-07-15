Mesos HTTP Proxy
================

```shell
git clone git@github.com:mesosphere/playa-mesos.git
cd playa-mesos
vagrant up
vagrant ssh

sudo easy_install http://downloads.mesosphere.io/master/ubuntu/14.04/mesos-0.19.0_rc2-py2.7-linux-x86_64.egg
```

Example run:

```shell
./scheduler.py --help
./scheduler.py --service-url http://127.0.0.1:5000/
```

This proxy will periodically `POST` a payload like this to the `/offer` endpoint of your service:

```json
{
  "attributes": [],
  "executor_ids": [
    "default"
  ],
  "framework_id": "20140715-151922-16842879-5050-6648-0011",
  "hostname": "10.141.141.10",
  "id": "20140715-151922-16842879-5050-6648-1942",
  "resources": {
    "cpus": 1.75,
    "disk": 34068.0,
    "mem": 936.0,
    "ports": [
      {
        "begin": 31000,
        "end": 32000
      }
    ]
  },
  "slave_id": "20140714-215541-16842879-5050-1243-0"
}
```

You will likely only care about the `resources` and `attributes` (if you set any in Mesos) keys. Your server should respond with a payload like:

```json
[]
```

...to not run any jobs. Or:

```json
[
  {
    "id": "my_job",
    "resources": {
      "cpus": 0.25,
      "mem": 64
    }
  }
]
```

...to run `my_job`, claiming the stated amount of resources.

The proxy will also periodically `POST` status updates about job to the `/status` endpoint of your service:

```json
{
  "id": "my_job",
  "state": "running"
}
```

The `id` will correspond to the `id` you returned in your `/offer` endpoint.

Possible states correspond directly to the Protocol Buffers version:

```python
states = {
  0: "starting",
  1: "running",
  2: "finished", # terminal
  3: "failed", # terminal
  4: "killed", # terminal
  5: "lost", # terminal
  6: "staging",
}
```

Terminal means the job is no longer running or attempting to run.
