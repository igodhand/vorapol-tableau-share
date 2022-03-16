# Sample Code by Vorapol S. (Ping)

import requests
import json
from datetime import datetime
from tableauhyperapi import *

req = requests.get("https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all")
res = json.loads(req.content)

with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, 'COVID.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        print("The connection to the Hyper file is open.")

        #########################
        # START EXAMPLE TABLE
        #########################

        connection.catalog.create_schema('Extract')
        example_table = TableDefinition(TableName('Extract', 'COVID'), [
            TableDefinition.Column('txn_date', SqlType.date()),
            TableDefinition.Column('new_case', SqlType.big_int()),
            TableDefinition.Column('total_case', SqlType.big_int()),
            TableDefinition.Column('new_case_excludeabroad', SqlType.big_int()),
            TableDefinition.Column('total_case_excludeabroad', SqlType.big_int()),
            TableDefinition.Column('new_death', SqlType.big_int()),
            TableDefinition.Column('total_death', SqlType.big_int()),
            TableDefinition.Column('new_recovered', SqlType.big_int()),
            TableDefinition.Column('total_recovered', SqlType.big_int()),
            TableDefinition.Column('update_date', SqlType.timestamp()),
        ])
        print("The table is defined.")
        connection.catalog.create_table(example_table)

        with Inserter(connection, example_table) as inserter:
            for record in res:
                inserter.add_row([
                    datetime.strptime(record['txn_date'], "%Y-%m-%d"),
                    record['new_case'],
                    record['total_case'],
                    record['new_case_excludeabroad'],
                    record['total_case_excludeabroad'],
                    record['new_death'],
                    record['total_death'],
                    record['new_recovered'],
                    record['total_recovered'],
                    datetime.strptime(record['update_date'], "%Y-%m-%d %H:%M:%S"),
                ])

            inserter.execute()
        print("The data was added to the table.")
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")
