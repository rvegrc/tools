
For import in jupyter notebook use

```
# import tools
# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))

# find tools in parent dir
if os.path.isdir(os.path.join(parent_dir, 'tools')):
    # Add parent directory to sys.path if found
    sys.path.append(parent_dir)
    
else:
    # for run in spark
    parent_dir = os.path.abspath(os.path.join(os.getcwd(), "../airflow/airflow_data"))
    
    # Add parent directory to sys.path
    sys.path.append(parent_dir)


from tools import pd_tools
from tools.paths import Paths
from tools.db_tools import DbTools
from tools.custom_transformers import SafePowerTransformer

# import paths
root_path = '.' # for local folder
paths = Paths(root_path)
data_path = paths.data_path
tmp_path = paths.tmp_path
prod_data_path = paths.prod_data_path
dev_data_path = paths.dev_data_path
prod_db = paths.prod_db
dev_db = paths.dev_db
tmp_db = paths.tmp_db
tmp_data_path = paths.tmp_data_path

db_tools = DbTools(data_path, tmp_path, client)
```