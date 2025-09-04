import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
import pandas as pd
from gen_code_with_city import main


# I'm redoing execute/procedure to learn how it works. 

def load_credentials():
    # load info from env 
    load_dotenv()
     # snowflake credentials
    return { 
            'account' : os.getenv('SF_ACCOUNT'),
            'user' :   os.getenv('SF_USER'),
            'role': os.getenv('SF_ROLE'),
            'warehouse': os.getenv('SF_WAREHOUSE'),
            'database' : os.getenv('SF_DATABASE'),
            'schema' : os.getenv('SF_SCHEMA'),
            'authenticator' : 'externalbrowser'}


if __name__ == "__main__":
    session = Session.builder.configs(load_credentials()).create()
    main(session)
    print("test table suceeded")
    session.close()