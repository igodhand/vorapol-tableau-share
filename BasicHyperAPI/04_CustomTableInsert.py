# Sample Code by Vorapol S. (Ping)
# https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_create_update.html

from tableauhyperapi import *

with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, 'CustomTable.hyper', CreateMode.CREATE_IF_NOT_EXISTS) as connection:
        print("The connection to the Hyper file is open.")

        #########################
        # START EXAMPLE TABLE
        #########################

        example_table = TableName('Extract', 'Demo')

        with Inserter(connection, example_table) as inserter:
            inserter.add_row([4, 'Siroros', 4000])
            inserter.add_row([5, 'Wasan', 5000])
            inserter.add_row([6, 'Kajornsak', 6000])
            inserter.execute()

        #########################
        # END EXAMPLE TABLE
        #########################

        print("The data was added to the table.")
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")
