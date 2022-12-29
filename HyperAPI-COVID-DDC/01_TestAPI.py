# Sample Code by Vorapol S. (Ping)

import requests
import json


req = requests.get("https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all")
res = json.loads(req.content)

for record in res:
    print(record)
