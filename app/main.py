# import requirements needed
from flask import Flask, render_template, request
from utils import *

# setup the webserver
# port may need to be changed if there are multiple flask servers running on same server
port = 29999
base_url = get_base_url(port)

# if the base url is not empty, then the server is running in development, and we need to specify the static folder so that the static files are served
if base_url == '/':
    app = Flask(__name__)
else:
    app = Flask(__name__, static_url_path=base_url+'static')

# set up the routes and logic for the webserver
@app.route(f'{base_url}')
def home():
    return render_template('index.html', content=render_template('terminal.html'))

@app.route(f'/terminal')
def term():
    return render_template('index.html', content=render_template('terminal.html'))

@app.route(f'/file_explorer')
def file_explorer():
    return render_template('index.html', content=render_template('file_explorer.html'))

@app.route(f'/example')
def example():
    return render_template('example.html')


# define additional routes here
# for example:
# @app.route(f'{base_url}/team_members')
# def team_members():
#     return render_template('team_members.html') # would need to actually make this page

@app.route(f'/command', methods=['POST'])
def command_caller():
    imp = request.form['imp']
    if imp == 'firebase':
        import edfs.firebase.commands as com
    elif imp == 'mongo':
        import edfs.mongodb.commands as com
    elif imp == 'mysql':
        import edfs.mysql.commands as com
    else:
        return 'INVALID INPUT' 
     
    split = request.form['comm'].split(' ')
    call = split[0]
    if call == 'mkdir':
        if len(split) != 2:
            return 'Incorrect Number of Arguments'
        else:
            return com.mkdir(split[1])
    elif call == 'ls':
        if len(split) != 2:
            return 'Incorrect Number of Arguments'
        else:
            print(split[1])
            return com.ls(split[1]).replace('\n', '</br>')
    elif call == 'cat':
        if len(split) != 2:
            return 'Incorrect Number of Arguments'
        else:
            return com.cat(split[1]).replace('\n', '</br>')
    elif call == 'rm':
        if len(split) != 2:
            return 'Incorrect Number of Arguments'
        else:
            return com.rm(split[1])
    elif call == 'put':
        if len(split) != 4:
            return 'Incorrect Number of Arguments'
        else:
            return com.put(split[1], split[2], split[3])
    elif call == 'getPartitionLocations':
        if len(split) != 2:
            return 'Incorrect Number of Arguments'
        else:
            return com.getPartitionLocations(split[1]).replace('\n', '</br>')
    elif call == 'readPartition':
        if len(split) != 3:
            return 'Incorrect Number of Arguments'
        else:
            return com.readPartition(split[1], split[2]).replace('\n', '</br>')
    else:
        return ('Invalid Command.')
    
    return 'DONE'


if __name__ == '__main__':
    # IMPORTANT: change url to the site where you are editing this file.
    # website_url = 'cocalc4.ai-camp.org'
    website_url = f'localhost:{port}'

    print(f'Try to open\n\n    {website_url}' + base_url + '\n\n')
    app.run(host='0.0.0.0', port=port, debug=True)
