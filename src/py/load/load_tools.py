import argon2, pandas as pd, getpass, mysql.connector

def create_credentials(file):
    # Ask for credentials and store as a parquet file
    create_credentials = input("Create Root Credentials?\nY/N: ")
    if(create_credentials in ["Y","y","yes","Yes", "YES"]):
        # Ask for Credentials
        user,password,host,port = str(input("user: ")), str(getpass.getpass("password: ")), str(input("host: ")), str(input("port: "))
        hashed_password = argon_hash(password)
        # Store as DataFrame
        cred_df = pd.DataFrame( data = {"user": [user],"password": [hashed_password],"host": [host],"port": [port]},
                                columns= ['user','password','host', 'port'])
        # Convert to pyarrow
        cred_df.astype({"user":"string[pyarrow]",
                        "password":"string[pyarrow]",
                        "host":"string[pyarrow]",
                        "port":"int64[pyarrow]"})
        # Save to parquet
        cred_df.to_parquet(file, engine='pyarrow')

def argon_hash(text):
    # Argon2 ID Hash text with random salt
    binary_text = str.encode(text, 'utf-8')
    # Argon2 -> options: SHA-1, SHA-256, MD-5, Argon2, scrypt, bcrypt, and Argon2
    ph = argon2.PasswordHasher( time_cost=16, 
                                memory_cost=2**15, 
                                parallelism=4, 
                                hash_len=32, 
                                salt_len= 32,
                                type= argon2.Type.ID)
    hash = ph.hash(binary_text)
    print(hash)
    return hash

def verify_password(hash, text):
    # Check if text matches hashed values
    ph = argon2.PasswordHasher( time_cost=16, 
                                memory_cost=2**15, 
                                parallelism=4, 
                                hash_len=32, 
                                salt_len= 32,
                                type= argon2.Type.ID)
    return (ph.verify(hash, text))
        
def mysql_execute(credentials, query, type='default', db=None):
    # Connect Python to MYSQL
    try:
        if(type == "CREATE_DB"):
            print(f"CREATE_DB: \n")
            conn = mysql.connector.connect( user=credentials.user[0], password=credentials.password[0],
                                            host = credentials.host[0],
                                            port = int(credentials.port[0]))
        if(type == 'SSH'):
            print(f"SSH: \n")
            # with sshtunnel.SSHTunnelForwarder(
            # (_host, _ssh_port),
            # ssh_username=_username,
            # ssh_password=_password,
            # remote_bind_address=(_remote_bind_address, _remote_mysql_port),
            # local_bind_address=(_local_bind_address, _local_mysql_port)
            # ) as tunnel:
            # conn = mysql.connector.connect( user=credentials.user, password=credentials.password,
            #                                 host = credentials.host,
            #                                 database = db, port = int(credentials.port))
        if(type=='default'):
            print(f"default: \n")
            conn = mysql.connector.connect( user=credentials.user[0], password=credentials.password[0],
                                            host = credentials.host[0],
                                            port = int(credentials.port[0]),
                                            db=db)                     
        if conn.is_connected():
            db_Info = conn.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = conn.cursor()
        try:
            # execute SQL command
            cursor.execute(query)
            # commit changes in database
            conn.commit()
            print("Query Executed")
        except:
            # rollback in case of errors
            conn.rollback()
            print("rolling back connection in case of errors: \n")
        # close Connection
        conn.close()
    # Why did the database not connect?
    except mysql.connector.errors as error:
        print(error)
    else:
        conn.close()
        
def mysql_connect(credentials, db):
    try:
        conn = mysql.connector.connect( user=credentials.user[0], password=credentials.password[0],
                                                host = credentials.host[0],
                                                port = int(credentials.port[0]),
                                                db=db) 
    except mysql.connector.errors as error:
        print(error)
    else:
        conn.close()
    return conn

def dataframe_astypes():
    """_summary_
    
    cleanly access dataframe conversions
    
    Returns:
        dictionary: column names and pandas dataframe conversions
        
        { 'id': 'int64',
            'created_at': 'datetime64[ns, UTC]',
            'user':'object',
            'group':'object',
            'url': 'object',
            'favorite_count': 'int64',
            'retweet_count': 'int64',
            'hashtags':'object',
            'emojis': 'object',
            'emoji_text':'object',
            'usernames': 'object',
            'links': 'object',
            'text': 'object'}
    """
    return { 'id': 'int64',
            'created_at': 'datetime64[ns, UTC]',
            'user':'object',
            'group':'object',
            'url': 'object',
            'favorite_count': 'int64',
            'retweet_count': 'int64',
            'hashtags':'object',
            'emojis': 'object',
            'emoji_text':'object',
            'usernames': 'object',
            'links': 'object',
            'text': 'object'}