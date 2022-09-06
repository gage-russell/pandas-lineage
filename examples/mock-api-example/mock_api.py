import json
from flask import Flask, request

app = Flask(__name__)
@app.route('/<path:text>', methods=['GET', 'POST'])
def index(text):
    print(text)
    record = json.loads(request.data)
    print(record)
    return json.dumps({})

app.run(debug=True)