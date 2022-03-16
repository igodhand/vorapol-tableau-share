# https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_create_update.html

from tableauhyperapi import *

with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, 'TUG.hyper', CreateMode.CREATE_IF_NOT_EXISTS) as connection:
        print("The connection to the Hyper file is open.")

        #########################
        # START EXAMPLE TABLE
        #########################

        example_table = TableName('Extract', 'Demo')

        connection.execute_command(command=F'''
           DELETE FROM "Extract"."Demo" WHERE "CustomerID" >= 6
        ''')

        #########################
        # END EXAMPLE TABLE
        #########################

        print("The data was added to the table.")
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")
