import _CONFIG as config
from tableauhyperapi import *
from UTILS import Authen, Logger
import requests
import os
import json
import pandas as pd
from pprint import pprint

logger = Logger.logger(log_path=config.log_path)
auth = Authen.authen(client_id=config.client_id,
                     username=config.username,
                     password=config.password,
                     tenant=config.tenant,
                     logger=logger)
auth.get_token(resource_url="https://mydemo.sharepoint.com")

##############################
#### CONFIGURATION FOLDER ####
##############################

api_endpoint = "https://mydemo.sharepoint.com/sites/ShareDocument/_api/"

list_endpoint = F"{api_endpoint}/lists/getbytitle('MS FORM IT')/items"
res = requests.get(list_endpoint, headers=auth.headers)
j = json.loads(res.content)
df = pd.DataFrame(j['value'])
list_col_not_use = ['odata.type', 'odata.etag',
                     'FileSystemObjectType', 'ServerRedirectedEmbedUri',
                     'ServerRedirectedEmbedUrl', 'ContentTypeId', 'Title',
                     'ComplianceAssetId', 'ID', 'Modified',
                     'AuthorId', 'EditorId', 'OData__UIVersionString', 'Attachments',
                     'GUID']
list_col_to_use = [c for c in df.columns if c not in list_col_not_use]
df_form = df[list_col_to_use]

list_not_pivot = ['odata.id','odata.editLink','Id','Created']
list_pivot = [c for c in df_form.columns if c not in list_not_pivot]
df_form_pivot = pd.melt(df_form, id_vars=list_not_pivot, value_vars=list_pivot)
df_form_pivot['Created'] = pd.to_datetime(df_form_pivot['Created'])
df_form_pivot.columns = ['odata_id','odata_link','item_id','item_created','odata_question','response']


field_endpoint = F"{api_endpoint}/lists/getbytitle('SP_LIST')/Fields"
res = requests.get(field_endpoint, headers=auth.headers)
j = json.loads(res.content)
df = pd.DataFrame(j['value'])
df = df[(df["FromBaseType"] == False) & (df["Hidden"] == False)]
df = df[['odata.type', 'EntityPropertyName', 'Title']]
df.columns = ['odata_type','odata_question','question_name']






with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, 'SHAREPOINT.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        print("The connection to the Hyper file is open.")

        #########################
        # START EXAMPLE TABLE
        #########################

        connection.catalog.create_schema('Extract')
        example_table = TableDefinition(TableName('Extract', 'SHAREPOINT'), [
            TableDefinition.Column('odata_type', SqlType.varchar(100)),
            TableDefinition.Column('odata_question', SqlType.big_int()),
            TableDefinition.Column('question_name', SqlType.big_int()),                    
        ])
        print("The table is defined.")
        connection.catalog.create_table(example_table)

        #########################
        # INSERT DATA
        #########################

        with Inserter(connection, example_table) as inserter:
            for row in df.iterrows():
                inserter.add_row(
                    [row['odata_type'], row['odata_question'], row['question_name']]
                )
            inserter.execute()


        print("The data was added to the table.")
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")








