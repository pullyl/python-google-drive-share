# walk_gdrive.py - os.walk variation with Google Drive API

import os
import json
import argparse
import pandas as pd

from apiclient.discovery import build  # pip install google-api-python-client
from oauth2client import file, client, tools

FOLDER = 'application/vnd.google-apps.folder'
PERMISSION_DICT = []

def get_credentials(scopes, flags, secrets='client_secret.json', storage='~/.credentials/google-drive-share.json'):

    store = file.Storage(os.path.expanduser(storage))
    creds = store.get()
    if creds is None or creds.invalid:
        flow = client.flow_from_clientsecrets(os.path.expanduser(secrets), scopes)
        creds = tools.run_flow(flow, store, flags)
    return creds

parser = argparse.ArgumentParser(
    description='Walks Google Drive folder and emits csv with file/folders sharing permissions',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    parents=[tools.argparser])
parser.add_argument('--folderId', dest='folderId', required=True, help='Google Drive folderId (found in url when folder is open')
parser.add_argument('--emailAddressWhitelist', dest='emailAddressWhitelist', nargs='*', default=[], help='if present will attempt to remove permissions from all emailAddresses not in the whitelist or the owner, including public share links')
args = parser.parse_args()
creds = get_credentials('https://www.googleapis.com/auth/drive', args)
service = build('drive', version='v3', credentials=creds)

def iterfiles(name=None, is_folder=None, parent=None, order_by='folder,name,createdTime'):
    q = []
    if name is not None:
        q.append("name = '%s'" % name.replace("'", "\\'"))
    if is_folder is not None:
        q.append("mimeType %s '%s'" % ('=' if is_folder else '!=', FOLDER))
    if parent is not None:
        q.append("'%s' in parents" % parent.replace("'", "\\'"))
    params = {'pageToken': None, 'orderBy': order_by}
    if q:
        params['q'] = ' and '.join(q)

    count = 0
    while True and count < 100:
        count += 1
        response = service.files().list(**params).execute()
        for f in response['files']:
            permissionList = service.permissions().list(fileId=f['id'],fields='*').execute()
            permissions = []
            permissionIdsToRemove = []
            for p in permissionList['permissions']:
                if 'emailAddress' in p:
                    permissions.append('%s=%s' % (p['role'],p['emailAddress']))
                    if p['role'] != 'owner' and p['emailAddress'] not in args.emailAddressWhitelist:
                        permissionIdsToRemove.append(p['id'])
                else:
                    permissions.append('%s=%s' % (p['role'],p['type']))
                    permissionIdsToRemove.append(p['id'])

            for p in permissions:
                PERMISSION_DICT.append({'id': f['id'], 'name': f['name'], 'type': p.split('=')[0], 'person': p.split('=')[1]})

            if args.emailAddressWhitelist:
                for permissionId in permissionIdsToRemove:
                    print('removing permissions: %s' % ','.join(permissionIdsToRemove))
                    permissionList = service.permissions().delete(fileId=f['id'],permissionId=permissionId).execute()

            yield f
        try:
            params['pageToken'] = response['nextPageToken']
        except KeyError:
            return

def walk(folderId):
    top = service.files().get(fileId=folderId).execute()
    stack = [((top['name'],), [top])]
    while stack:
        path, tops = stack.pop()
        for top in tops:
            dirs, files = is_file = [], []
            for f in iterfiles(parent=top['id']):
                is_file[f['mimeType'] != FOLDER].append(f)
            yield path, top, dirs, files
            if dirs:
                stack.append((path + (top['name'],), dirs))

    df = pd.DataFrame(PERMISSION_DICT)
    df.to_csv('permissions.csv')
    print('exported {num} permissions'.format(num(len(df))))

print('id,name,permissions')
results_count=[]
for path, root, dirs, files in walk(args.folderId):
    results_count.append('%s\t%d %d' % ('/'.join(path), len(dirs), len(files)))
