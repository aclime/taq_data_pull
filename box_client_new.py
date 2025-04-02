import os
import json
import webbrowser
import threading
from flask import Flask, redirect, request
from boxsdk import OAuth2, Client
from box_creds import client_id, client_secret  # Your Box credentials

TOKEN_FILE = "box_tokens.json"
CLIENT_ID = client_id
CLIENT_SECRET = client_secret
#REDIRECT_URI = "http://localhost:8080/callback"
REDIRECT_URI = "http://localhost:5080/callback"

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Required for Flask session management

# Initialize OAuth2
oauth2 = OAuth2(CLIENT_ID, CLIENT_SECRET)


def save_tokens(access_token, refresh_token):
    """Save OAuth tokens to a file."""
    with open(TOKEN_FILE, "w") as f:
        json.dump({"access_token": access_token, "refresh_token": refresh_token}, f)


def load_tokens():
    """Load OAuth tokens from a file."""
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    return None


@app.route("/")
def index():
    """Redirect user to Box authentication page."""
    auth_url, csrf_token = oauth2.get_authorization_url(REDIRECT_URI)
    return redirect(auth_url)


TOKEN_FILE = "box_tokens.json"  # File to store tokens

@app.route("/callback")
def callback():
    """Handles OAuth 2.0 callback and saves tokens."""
    code = request.args.get("code")
    csrf_token = request.args.get("state")

    if csrf_token != session.get("csrf_token"):
        return "CSRF token mismatch. Authentication failed.", 400

    # Exchange code for access & refresh tokens
    access_token, refresh_token = oauth2.authenticate(code)

    # Save tokens to a file instead of session
    with open(TOKEN_FILE, "w") as f:
        json.dump({"access_token": access_token, "refresh_token": refresh_token}, f)

    return "Authentication successful. You can close this window."



def run_flask():
    """Runs the Flask server for handling OAuth 2.0 authentication."""
    app.run(port=8080, debug=False, use_reloader=False)


def make_client():
    """Handles OAuth 2.0 flow and returns an authenticated Box client."""
    
    # Check if tokens exist in the file
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            tokens = json.load(f)
            access_token = tokens.get("access_token")
            refresh_token = tokens.get("refresh_token")
        
        if access_token and refresh_token:
            # Initialize OAuth2 with stored tokens
            oauth2 = OAuth2(
                CLIENT_ID, CLIENT_SECRET,
                access_token=access_token,
                refresh_token=refresh_token,
                store_tokens=lambda at, rt: json.dump({"access_token": at, "refresh_token": rt}, open(TOKEN_FILE, "w"))
            )
            return Client(oauth2)  # Return authenticated Box client

    # If tokens are missing, start the OAuth flow
    thread = threading.Thread(target=run_flask, daemon=True)
    thread.start()

    webbrowser.open(oauth2.get_authorization_url(REDIRECT_URI)[0])
    thread.join()

    # After authentication, try to load tokens again
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            tokens = json.load(f)
            access_token = tokens.get("access_token")
            refresh_token = tokens.get("refresh_token")

        if access_token and refresh_token:
            oauth2 = OAuth2(
                CLIENT_ID, CLIENT_SECRET,
                access_token=access_token,
                refresh_token=refresh_token,
                store_tokens=lambda at, rt: json.dump({"access_token": at, "refresh_token": rt}, open(TOKEN_FILE, "w"))
            )
            return Client(oauth2)

    raise Exception("Authentication failed. No access token found.")

client=make_client()

def get_folder_id(query='taq'):
    #client=make_client()
    taq_folder_srch=client.search().query(query=query, type='folder')
    taq_folder_id=[item.id for item in taq_folder_srch][0]
    return taq_folder_id

def upload_to_box(path,name):
    #update file
        #https://github.com/box/box-python-sdk/blob/main/docs/usage/files.md#upload-a-new-version-of-a-file
    taq_folder_id=get_folder_id('taq')
    client.folder(taq_folder_id).upload(file_path=path,file_name=name) 
    
    #for  large uplaods
    #chunked_uploader=client.folder(taq_folder_id).get_chunked_uploader(path,name) 
    #uploaded_file = chunked_uploader.start()


def pull_data_off_box(periods_dict):
    client=make_client()
    om_folder_srch=client.search().query(query='OptionMetrics', type='folder')
    om_folder_id=[item.id for item in om_folder_srch][0]
    res=client.folder(om_folder_id).get_items(limit=None)

    for r in res:
        file_name=r.name
        year=file_name.split('.')[0].split('_')[1]
        month=file_name.split('.')[0].split('_')[-1]
        for k,v in periods_dict.items():
            #print(v)
            #print(int(v['year']) == year)
            #print(int(v['month']) , month)
            #print('----')
            """
            if (int(v['year'])==int(year)) and (int(v['month'])==int(month)):
                print(year,month)
                with open('test_csv.csv', 'wb') as open_file:
                    r.download_to(open_file)
                    table = pv.read_csv(open_file)
                    open_file.close()
            """
            
            if (int(v['year'])==int(year)) and (int(v['month'])==int(month)):
                file_content = r.content()
                s=str(file_content,'utf-8')
                data = StringIO(s)
                df = pd.read_csv(data)
                #dest=r'vix_approx_research/white_paper_replicate'
                #df.to_csv(rf'{dest}/{r.name}')

