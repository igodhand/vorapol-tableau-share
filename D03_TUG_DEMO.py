import requests
import json
from datetime import datetime
from tableauhyperapi import *

req = requests.get("https://covid19.th-stat.com/api/open/timeline")
res = json.loads(req.content)['Data']

with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, 'COVID.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        print("The connection to the Hyper file is open.")

        #########################
        # START EXAMPLE TABLE
        #########################

        connection.catalog.create_schema('Extract')
        example_table = TableDefinition(TableName('Extract', 'COVID'), [
            TableDefinition.Column('Date', SqlType.date()),
            TableDefinition.Column('NewConfirmed', SqlType.big_int()),
            TableDefinition.Column('NewRecovered', SqlType.big_int()),            
            TableDefinition.Column('NewHospitalized', SqlType.big_int()),            
            TableDefinition.Column('NewDeaths', SqlType.big_int()),            
        ])
        print("The table is defined.")
        connection.catalog.create_table(example_table)

        with Inserter(connection, example_table) as inserter:
            for record in res:
                d = datetime.strptime(record['Date'], "%m/%d/%Y")
                inserter.add_row(
                    [d, record['NewConfirmed'], record['NewRecovered'], record['NewHospitalized'], record['NewDeaths']]
                )
                
            inserter.execute()
        print("The data was added to the table.")
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")
