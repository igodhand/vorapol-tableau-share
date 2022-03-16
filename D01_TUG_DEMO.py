import requests
import json
from datetime import datetime
from tableauhyperapi import *

req = requests.get("https://covid19.th-stat.com/api/open/timeline")
res = json.loads(req.content)['Data']

for record in res:
    print(record)