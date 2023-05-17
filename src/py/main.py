# %%
# Import libraries
import subprocess, os, sys
from collections import Counter
            
class socialmedia():
    # - Change Directory to top level folder
    top_level_folder = 'Social_Media_Pipeline'
    if(os.getcwd().split(os.sep)[-1] != top_level_folder):
        infinite_limit, infinity_check = 10, 0
        try:
            while(os.getcwd().split(os.sep)[-1] != top_level_folder):
                os.chdir('..') # move up a directory
                infinity_check += 1
                if(infinity_check > infinite_limit):
                    raise Exception("cwd: {os.getcwd()}: {infinite_limit} directories up from {top_level_folder}")
            print(f"cwd: {os.getcwd()}", sep = '\n')
            # add path to system path for running in terminal
            if(os.getcwd() not in sys.path):
                sys.path.append(os.getcwd())
        except Exception as e:
            print(f"cwd: {os.getcwd()}", sep = '\n')
            print(f"{e}\n:Please start current working directory from {top_level_folder}")
    # def remove_duplicate_paths(paths):
    #     d = dict(Counter(paths))
    #     for i in paths:
    #         if(d[i]>1):
    #             paths.remove(i)
    # remove_duplicate_paths(sys.path)
    
    # extract
    from src.py.extract import extract_twitter
    # load
    from src.py.load import load_extract
# %%
def main():
    app = socialmedia()
    
# %%
if __name__ == '__main__':
    main()
# %%