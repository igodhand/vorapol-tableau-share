# Sample Code by Vorapol S. (Ping)
# https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_create_update.html

from tableauhyperapi import *

with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, 'CustomTable.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        print("The connection to the Hyper file is open.")

        #########################
        # START EXAMPLE TABLE
        #########################

        connection.catalog.create_schema('Extract')

        example_table = TableDefinition(TableName('Extract', 'Demo'), [
            TableDefinition.Column('CustomerID', SqlType.big_int()),
            TableDefinition.Column('CustomerName', SqlType.varchar(200)),
            TableDefinition.Column('SalesAmount', SqlType.double()),
        ])
        print("The table is defined.")
        connection.catalog.create_table(example_table)

        with Inserter(connection, example_table) as inserter:
            inserter.add_row([1, 'Vorapol', 1000])
            inserter.add_row([2, 'Komes', 2000])
            inserter.add_row([3, 'Methee', 3000])
            inserter.execute()

        #########################
        # END EXAMPLE TABLE
        #########################

        print("The data was added to the table.")
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")
