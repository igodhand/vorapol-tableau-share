# Sample Code by Vorapol S. (Ping)
# Need to use Personal Access Token to Authenticate
# https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm
# Need to rename _CONFIG.template.py as _CONFIG.py before using this code

import _CONFIG as config
import tableauserverclient as TSC

# Optional Suppress Warning

import urllib3
urllib3.disable_warnings()

# Authenticate with Tableau Server

tableau_auth = TSC.PersonalAccessTokenAuth(
    config.tableau_token_name, config.personal_access_token, site_id=config.tableau_site_id
)
server = TSC.Server(config.tableau_server)
server.add_http_options({'verify': False})
server.auth.sign_in(tableau_auth)

# Get Project ID from Tableau Server

dict_proj = {}
items, pagination_item = server.projects.get()
for i in items:
    dict_proj[i.name] = i.id

proj_id = dict_proj.get("Default")
print(f'"Default" Project ID = {proj_id}')

# Publish Hyper File to Tableau Server

hyper_file = "COVID.hyper"
new_ds = TSC.DatasourceItem(proj_id)
new_ds = server.datasources.publish(new_ds, hyper_file, "Overwrite")
print("Publish Success")
