from nyc_taxi.uploading.config.config import S3Config
import os
from dotenv import load_dotenv

load_dotenv()

class LocalFilesManager(S3Config()):

    def __init__(self):
        self.my_loacl_path = os.getenv("MY_LOCAL_PATH",None)


    def get_list_of_files_from_laptop(self, my_loacl_path:str)-> list[str]:
        '''Get a list of all files waiting to be loaded on your local laptop directory'''
        pass

    def save_file_list_into_table(file_lst:list[str],DB:str, schema:str, table:str)-> None:
        '''Save the list of new files that will be upload in the next steps into S3 from local laptop in a Snowflake table
            to be used in the next stage.
        '''
        pass
    
    def devide_file_list_into_exists_not_exists(self, files_from_laptop:list[str])-> tuple[list[str],list[str]]:
        ''' Send Snowflake the list of files that are waiting to be loadedfrom your local laptop,
            Snowflake reture two lists: exists and do not exists in the alreay loaded table.
        Return:
            Exists (list) - list of file name that alredy been loaded.
            Not exists (list) - list of new files that haven`t been loaded.
        '''        
        pass


    def archive_files_in_laptop_directory(self, my_loacl_path, archive_lst:list[str])-> None:
        '''Archive multiple files on the local laptop'''
        pass

    def run(self)-> None:
        '''get the new files list and store the loaded ones, if exists'''

        local_fils_lst = self.get_list_of_files_from_laptop('')
        exist, not_exists = self.archive_files_in_laptop_directory(local_fils_lst)        
        self.archive_files_in_laptop_directory(exist)
        self.save_file_list_into_table('Not_exists', 'database', 'schema', 'target_table')