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
    
    @app.route("/twitterkey", methods=["GET","POST"])
    def twitterkey():
        folder = os.path.normpath('./credentials')
        file = os.path.normpath('./credentials/twitter.csv')
        if(request.method == 'POST'):
            print("Add Twitter Key's")
            
            # check if credential folder exits
            if(os.path.exists(folder)):
                pass
            else:
                os.mkdir(folder)
            if(os.path.exists(file)): 
                pass
            else:                
                tokens = ['consumer_key','consumer_secret','access_token','access_token_secret','beaker_token']
                df = pd.DataFrame([[""]*len(tokens)], columns=tokens)
                df.to_csv(file, index=False)
                
            df = pd.read_csv(file, header=0)
            key_type = list(request.form)[0]
            key_value = request.form[key_type]
            match key_type:
                case 'consumer_key': 
                    df['consumer_key'][0] = key_value 
                case 'consumer_secret': 
                    df['consumer_secret'][0] = key_value 
                case 'access_token': 
                    df['access_token'][0] = key_value 
                case 'access_token_secret': 
                    df['access_token_secret'][0] = key_value       
                case 'beaker_token': 
                    df['beaker_token'][0] = key_value  
                    
            df.to_csv(file, index=False)        
            
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
            data = list(request.form)[0]
            match data:
                case 'extract_twitter':
                    print("Extract Twitter")
                    import extract_twitter
                case 'transform_twitter':
                    print("Transform Twitter\n")
                    import transform_twitter
                case 'extract_stocks':
                    print("Extract Stocks\n")
                    import extract_stocks
                case 'load_mysql_twitter_extract':
                    print("load raw twitter to MYSQL")
                    import load_extract
                case 'load_mysql_twitter_transform':
                    print("load transfomred twitter to MYSQL")
                    # import load_extract
                case 'stock_prediction':
                    print("stock_prediction")
                    import stock_prediction
                case 'run_all':
                    print(f"Run All\n****************************************************************\n")
                    
                    try:
                        print("Extract Twitter", end="\n")
                        import extract_twitter
                    except Exception as e:
                        print("ERROR Extracting Twitter", end="\n")
                        print(f'Exception: {e}',end ='\n')
                        
                    try:
                        print("Transform Twitter", end="\n")
                        import transform_twitter
                    except Exception as e:
                        print("ERROR Transforming Twitter", end="\n")
                        print(f'Exception: {e}',end ='\n')
                        
                    try:   
                        print("Extract Stocks", end="\n")
                        import extract_stocks
                    except Exception as e:
                        print("ERROR Extracting Stocks", end="\n")
                        print(f'Exception: {e}',end ='\n')
                    # print("load raw twitter to MYSQL", end="\n")
                    # import load_extract
                    try:
                        print("Stock Prediction", end="\n")
                        import stock_prediction 
                    except Exception as e:
                        print("ERROR Predicting Stocks", end="\n")
                        print(f'Exception: {e}',end ='\n')
                case _:
                    print("neither", end="\n") # unknown

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