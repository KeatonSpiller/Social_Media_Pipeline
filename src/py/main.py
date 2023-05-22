# %%
# Import libraries
import os, sys, pandas as pd
from flask import Flask, render_template, request
from fileinput import filename

def main():
    # %%            
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

    app = Flask(__name__)
    
    # Root endpoint
    @app.get('/')
    def upload():
        return render_template('upload-excel.html')
    
    
    @app.post('/view')
    def view():
    
        # Read the File using Flask request
        file = request.files['file']
        # save file in local directory
        file.save('./user_input/' + file.filename)
        print(file)
        print(filename)
        print('./user_input/' + file.filename)
        # Parse the data as a Pandas DataFrame type
        data = pd.read_excel(file)
    
        # Return HTML snippet that will render the table
        return data.to_html()

    app.run(port=5001, debug=True)

    # extract
    # import extract_twitter
    # load
    # import load_extract

if __name__ == '__main__':
    main()