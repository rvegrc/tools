
class Paths:
    def __init__(self, root_path):
        self.root_path = root_path
        self.tmp_path = f'{root_path}/tmp'
        self.data_path = f'{root_path}/data'
        self.tmp_data_path = f'{root_path}/data/tmp_data'
        self.prod_data_path = f'{root_path}/data/prod_data'
        self.dev_data_path = f'{root_path}/data/dev_data'
        self.new_data_path = f'{root_path}/data/new_data'
        self.prod_db = 'prod_transerv_cont'
        self.dev_db = 'dev_transerv_cont'
        self.tmp_db = 'tmp'