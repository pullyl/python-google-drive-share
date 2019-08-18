# walk_gdrive.py - os.walk variation with Google Drive API

import os
import argparse
import pandas as pd
import gspread

from apiclient.discovery import build  # pip install google-api-python-client
from oauth2client import file, client, tools

FOLDER = 'application/vnd.google-apps.folder'
PERMISSION_DICT = []
PERMISSION_TO_REMOVE = []

def get_credentials(scopes, flags, secrets='client_secret.json', storage='~/.credentials/google-drive-share.json'):

    store = file.Storage(os.path.expanduser(storage))
    creds = store.get()
    if creds is None or creds.invalid:
        flow = client.flow_from_clientsecrets(os.path.expanduser(secrets), scopes)
        creds = tools.run_flow(flow, store, flags)
    return creds

def iterfiles(service, args, email_blacklist, name=None, is_folder=None, parent=None, order_by='folder,name,createdTime'):
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
    while True:
        count += 1
        response = service.files().list(**params).execute()
        for f in response['files']:
            try:
                permissionList = service.permissions().list(fileId=f['id'],fields='*').execute()
            except:
                print('error pulling {id}'.format(id=f['id']))
                continue

            permissions = []
            permissionIdsToRemove = []
            removed = False

            #Only allow whitelisted permissions
            if len(args.emailAddressWhitelist):
                for p in permissionList['permissions']:
                    if 'emailAddress' in p:
                        permissions.append('%s=%s' % (p['role'],p['emailAddress']))
                        if p['role'] != 'owner' and p['emailAddress'] not in args.emailAddressWhitelist:
                            permissionIdsToRemove.append(p['id'])
                    else:
                        permissions.append('%s=%s' % (p['role'],p['type']))
                        permissionIdsToRemove.append(p['id'])

            #Remove blacklisted permissions if exist
            if len(email_blacklist):
                removed = True
                for p in permissionList['permissions']:
                    if 'emailAddress' in p:
                        permissions.append('%s=%s' % (p['role'],p['emailAddress']))
                        if p['role'] != 'owner' and p['emailAddress'] in email_blacklist:
                            permissionIdsToRemove.append(p['id'])
                            PERMISSION_TO_REMOVE.append(
                                {'id': f['id'], 'name': f['name'], 'displayName': p['displayName'], 'email': p['emailAddress']})

            #Remove some if necessary
            if args.prod and len(permissionIdsToRemove):
                removed = True
                for permissionId in permissionIdsToRemove:
                    print('removing permissions: %s' % ','.join(permissionIdsToRemove))
                    permissionList = service.permissions().delete(fileId=f['id'],permissionId=permissionId).execute()


            #Add permissions to dict
            if removed:
                permissionList = service.permissions().list(fileId=f['id'],fields='*').execute()
            for p in permissionList['permissions']:
                if 'emailAddress' in p:
                    permissions.append('%s=%s' % (p['role'], p['emailAddress']))
            for p in permissions:
                PERMISSION_DICT.append({'id': f['id'], 'name': f['name'], 'type': p.split('=')[0], 'person': p.split('=')[1]})

            yield f
        try:
            params['pageToken'] = response['nextPageToken']
        except KeyError:
            return

def walk(folderId, service, args, creds):
    top = service.files().get(fileId=folderId).execute()
    stack = [((top['name'],), [top])]

    #get emails for blacklist
    gc = gspread.authorize(creds)
    sheet = gc.open_by_url(args.emailAddressBlacklist).sheet1
    blacklist_emails = sheet.col_values(1)

    while stack:
        path, tops = stack.pop()
        for top in tops:
            dirs, files = is_file = [], []
            for f in iterfiles(service, args, blacklist_emails, parent=top['id']):
                is_file[f['mimeType'] != FOLDER].append(f)
            yield path, top, dirs, files
            if dirs:
                stack.append((path + (top['name'],), dirs))

    df = pd.DataFrame(PERMISSION_DICT)
    df.to_csv('all-folder-permissions.csv')
    print('exported {num} total permissions'.format(num=len(df)))

    df = pd.DataFrame(PERMISSION_TO_REMOVE)
    df.to_csv('permissions_to_remove.csv')
    print('exported {num} permissions to remove'.format(num=len(df)))


def main():
    parser = argparse.ArgumentParser(
        description='Walks Google Drive folder and emits csv with file/folders sharing permissions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    parser.add_argument('--folderId', dest='folderId', required=True,
                        help='Google Drive folderId (found in url when folder is open')
    parser.add_argument('--emailAddressWhitelist', dest='emailAddressWhitelist', nargs='*', default=[],
                        help='if present will attempt to remove permissions from all emailAddresses not in the whitelist or the owner, including public share links')
    parser.add_argument('--emailAddressBlacklist', dest='emailAddressBlacklist', default=None,
                        help='if present will attempt to remove permissions from these email addresses')
    parser.add_argument('--prod', dest='prod', default=False, help='if true will remove permissions, otherwise just export what needs to be removed')
    args = parser.parse_args()
    scope = ['https://www.googleapis.com/auth/drive', 'https://spreadsheets.google.com/feeds',
              'https://www.googleapis.com/auth/spreadsheets.readonly',
              'https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive.readonly',
              'https://www.googleapis.com/auth/drive',
              'https://www.googleapis.com/auth/drive.file']
    creds = get_credentials(scope, args)
    service = build('drive', version='v3', credentials=creds)

    results_count=[]
    for path, root, dirs, files in walk(args.folderId, service, args, creds):
        results_count.append('%s\t%d %d' % ('/'.join(path), len(dirs), len(files)))

if __name__ == '__main__':
    main()