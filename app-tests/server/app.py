import sys, os

cwd = "".join(reversed(os.getcwd()))
test_dir = "stset-ppa"
try:
    base_dir = "".join(reversed(cwd[cwd.index(test_dir) + len(test_dir):]))
except ValueError:
    base_dir = os.getcwd()

if base_dir not in sys.path:
    sys.path.append(base_dir)

import json
from flask import Flask, request
from . import models

from dbsync import models as synchmodels, server


app = Flask(__name__)


def enc(string):
    table = {"<": "&lt;",
             ">": "&gt;"}
    return "".join(table.get(c, c) for c in string)


@app.route("/")
def root():
    return 'Ping: any method <a href="/ping">/ping</a><br />'\
        'Repair: GET <a href="/repair">/repair</a><br />'\
        'Register: POST /register<br />'\
        'Pull: POST /pull<br />'\
        'Push: POST /push<br />'\
        'Query: GET <a href="/query">/query</a><br />'\
        'Inspect: GET <a href="/inspect">/inspect</a><br />'\
        'Synch query: GET <a href="/synch">/synch</a>'


@app.route("/ping")
def ping():
    return ""


@app.route("/repair", methods=["GET"])
def repair():
    return (json.dumps(server.handle_repair(request.args)),
            200,
            {"Content-Type": "application/json"})


@app.route("/register", methods=["POST"])
def register():
    return (json.dumps(server.handle_register()),
            200,
            {"Content-Type": "application/json"})


@app.route("/pull", methods=["POST"])
def pull():
    print(json.dumps(request.json, indent=2))
    try:
        return (json.dumps(server.handle_pull(request.json)),
                200,
                {"Content-Type": "application/json"})
    except server.handlers.PullRejected as e:
        return (json.dumps({'error': [repr(arg) for arg in e.args]}),
                400,
                {"Content-Type": "application/json"})


@app.route("/push", methods=["POST"])
def push():
    print(json.dumps(request.json, indent=2))
    try:
        return (json.dumps(server.handle_push(request.json)),
                200,
                {"Content-Type": "application/json"})
    except server.handlers.PullSuggested as e:
        return (json.dumps({'error': [repr(arg) for arg in e.args],
                            'suggest_pull': True}),
                400,
                {"Content-Type": "application/json"})
    except server.handlers.PushRejected as e:
        return (json.dumps({'error': [repr(arg) for arg in e.args]}),
                400,
                {"Content-Type": "application/json"})


@app.route("/query", methods=["GET"])
def query():
    return (json.dumps(server.handle_query(request.args)),
            200,
            {"Content-Type": "application/json"})


@app.route("/inspect", methods=["GET"])
def inspect():
    session = models.Session()
    return "<strong>Cities:</strong><pre>{0}</pre><hr />"\
        "<strong>Houses:</strong><pre>{1}</pre><hr />"\
        "<strong>Persons:</strong><pre>{2}</pre><hr />".format(
        "\n".join(enc(repr(x)) for x in session.query(models.City)),
        "\n".join(enc(repr(x)) for x in session.query(models.House)),
        "\n".join(enc(repr(x)) for x in session.query(models.Person)))


@app.route("/synch", methods=["GET"])
def synch():
    session = models.Session()
    return "<strong>Content Types:</strong><pre>{0}</pre><hr />"\
        "<strong>Nodes:</strong><pre>{1}</pre><hr />"\
        "<strong>Versions:</strong><pre>{2}</pre><hr />"\
        "<strong>Operations:</strong><pre>{3}</pre><hr />".format(
        "\n".join(enc(repr(x)) for x in session.query(synchmodels.ContentType)),
        "\n".join(enc(repr(x)) for x in session.query(synchmodels.Node)),
        "\n".join(enc(repr(x)) for x in session.query(synchmodels.Version)),
        "\n".join(enc(repr(x)) for x in session.query(synchmodels.Operation)))


if __name__ == "__main__":
    app.run(debug=True)
