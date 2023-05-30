# %%
# Import libraries
import os, sys, pandas as pd
from flask import Flask, render_template, request
import webbrowser

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
    
    @app.route("/upload", methods=["POST"])
    def upload():
    
        # Read the File using Flask request
        file = request.files['file']
    
        # save file in local directory
        UPLOAD_FOLDER = os.path.normpath('./user_input')
        if(not os.path.exists(UPLOAD_FOLDER)):
            os.mkdir(UPLOAD_FOLDER)
        app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
        
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], file.filename))
        
        # Parse the data as a Pandas DataFrame type
        # data = pd.read_excel(file)
        # Return HTML snippet that will render the table
        # return data.to_html()
        
        return render_template("index.html") 
    
    @app.route("/etl", methods=["POST"])
    def etl():
        if(request.method == 'POST'):
            data = request.form
            if 'extract' in data:
                print("Extract Twitter")
                import extract_twitter
                print("Extract Stocks")
                import extract_stocks
                print("load raw twitter to MYSQL")
                import load_extract
            elif 'transform' in data:
                print("Transform")
                import transform
            else:
                print("neither") # unknown
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