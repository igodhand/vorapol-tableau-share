import tableauserverclient as TSC

tableau_auth = TSC.TableauAuth('admin', 'bac6343737!', site_id='demo')
server = TSC.Server('https://www.bac.co.th:8000')
server.auth.sign_in(tableau_auth)

dict_proj = {}
items, pagination_item = server.projects.get()
for i in items:
    dict_proj[i.name] = i.id

proj_id = dict_proj.get("Default")
print(proj_id)

hyper_file = "D:/CLOUD/DROPBOX/PYTHON/TABLEAU_API/TUG_DEMO/COVID.hyper"
new_ds = TSC.DatasourceItem(proj_id)
new_ds = server.datasources.publish(new_ds, hyper_file, "Overwrite")
print("Success")




