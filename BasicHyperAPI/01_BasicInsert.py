# Sample Code by Vorapol S. (Ping)
# https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_create_update.html

from tableauhyperapi import *

with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, 'Demo.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        print("The connection to the Hyper file is open.")
        connection.catalog.create_schema('Extract')
        example_table = TableDefinition(TableName('Extract','Extract'), [
            TableDefinition.Column('rowID', SqlType.big_int()),
            TableDefinition.Column('value', SqlType.big_int()),
         ])
        print("The table is defined.")
        connection.catalog.create_table(example_table)

        with Inserter(connection, example_table) as inserter:
            for i in range (1, 101):
                inserter.add_row(
                    [ i, i ]
            )
            inserter.execute()

        print("The data was added to the table.")
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")


