# %%
# Import libraries
import os, sys, pandas as pd, webbrowser, subprocess
from flask import Flask, render_template, request

# %%
def main():            
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
    @app.route("/")
    @app.route("/home")
    def home():
        return render_template("index.html")
    
    @app.route("/openuserfolder", methods=["POST"])
    def openuserfolder():
        file = os.path.normpath('./user_input')
        if(request.method == 'POST'):
            data = request.form
            if 'openuserfolder' in data:
                print("Open User Folder")
                if(os.path.exists(file)):
                    subprocess.Popen(f"explorer {file}")
                else:
                    os.mkdir(file)
                    subprocess.Popen(f"explorer {file}")
        return render_template("index.html")
    
    @app.route("/opencredfolder", methods=["POST"])
    def opencredfolder():
        file = os.path.normpath('./credentials')
        if(request.method == 'POST'):
            data = request.form
            if 'opencredfolder' in data:
                print("Open Credential Folder")
                if(os.path.exists(file)):
                    subprocess.Popen(f"explorer {file}")
                else:
                    os.mkdir(file)
                    subprocess.Popen(f"explorer {file}")
        return render_template("index.html")
    
    @app.route("/userupload", methods=["POST"])
    def userupload():
    
        # Read the File using Flask request
        file = request.files['file']
        # save file in local directory
        UPLOAD_FOLDER = os.path.normpath('./user_input')
        if(not os.path.exists(UPLOAD_FOLDER)):
            os.mkdir(UPLOAD_FOLDER)
        app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], 'twitter_users.xlsx'))
        
        return render_template("index.html") 
    
    @app.route("/tickerupload", methods=["POST"])
    def tickerupload():
    
        # Read the File using Flask request
        file = request.files['file']
        # save file in local directory
        UPLOAD_FOLDER = os.path.normpath('./user_input')
        if(not os.path.exists(UPLOAD_FOLDER)):
            os.mkdir(UPLOAD_FOLDER)
        app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], 'stock_tickers.xlsx'))
        
        return render_template("index.html") 
    
    @app.route("/etl", methods=["POST"])
    def etl():
        if(request.method == 'POST'):
            data = request.form
            if 'extract_twitter' in data:
                print("Extract Twitter")
                import extract_twitter
            elif 'extract_stocks' in data:
                print("Extract Stocks")
                import extract_stocks
            elif 'load_mysql_extract' in data:
                print("load raw twitter to MYSQL")
                import load_extract
            elif 'load_mysql_transform' in data:
                print("load raw twitter to MYSQL")
                # import load_extract
            elif 'transform' in data:
                print("transform_twitter")
                import transform_twitter
            else:
                print("neither") # unknown
        return render_template("index.html")
    
    @app.route("/opendatafolder", methods=["POST"])
    def opendatafolder():
        file = os.path.normpath('./data')
        if(request.method == 'POST'):
            data = request.form
            if 'opendatafolder' in data:
                print("Open Data Folder")
                if(os.path.exists(file)):
                    subprocess.Popen(f"explorer {file}")
                else:
                    os.mkdir(file)
                    subprocess.Popen(f"explorer {file}")
        return render_template("index.html")
    
    # Open a Webpage and run
    port = 5000
    if not os.environ.get("WERKZEUG_RUN_MAIN"):
        webbrowser.open_new(f"http://127.0.0.1:{port}")
    app.run(port=port, debug=True)

# %%
if __name__ == '__main__':
    main()
# %%