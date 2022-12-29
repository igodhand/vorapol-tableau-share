# Sample Code by Vorapol S. (Ping)

import requests
import json
from tableauhyperapi import *

req = requests.get("https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all")
res = json.loads(req.content)

hyper = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
print("The HyperProcess has started.")

connection = Connection(hyper.endpoint, 'COVID.hyper', CreateMode.CREATE_AND_REPLACE)
print("The Connection to the Hyper file is open.")

connection.catalog.create_schema('Extract')
example_table = TableDefinition(TableName('Extract', 'COVID'), [
    TableDefinition.Column('year', SqlType.big_int()),
    TableDefinition.Column('weeknum', SqlType.big_int()),
    TableDefinition.Column('new_case', SqlType.big_int()),
    TableDefinition.Column('total_case', SqlType.big_int()),
    TableDefinition.Column('new_case_excludeabroad', SqlType.big_int()),
    TableDefinition.Column('total_case_excludeabroad', SqlType.big_int()),
    TableDefinition.Column('new_death', SqlType.big_int()),
    TableDefinition.Column('total_death', SqlType.big_int()),
    TableDefinition.Column('new_recovered', SqlType.big_int()),
    TableDefinition.Column('total_recovered', SqlType.big_int()),
    TableDefinition.Column('update_date', SqlType.varchar(20)),
])
print("The table is defined.")

connection.catalog.create_table(example_table)
print("The table created.")

with Inserter(connection, example_table) as inserter:
    for record in res:
        inserter.add_row([
            record['year'],
            record['weeknum'],
            record['new_case'],
            record['total_case'],
            record['new_case_excludeabroad'],
            record['total_case_excludeabroad'],
            record['new_death'],
            record['total_death'],
            record['new_recovered'],
            record['total_recovered'],
            record['update_date']
        ])

    inserter.execute()
print("Finished Insert")

connection.close()
hyper.close()
