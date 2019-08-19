# walk_gdrive.py - os.walk variation with Google Drive API

import os
import argparse
import pandas as pd
import gspread

from apiclient.discovery import build  # pip install google-api-python-client
from oauth2client import file, client, tools
from google.oauth2 import service_account

FOLDER = 'application/vnd.google-apps.folder'
PERMISSION_DICT = []
PERMISSION_TO_REMOVE = []

def get_credentials(scopes, flags, owner, secrets='service-account-credentials.json', storage='~/.credentials/google-drive-share.json'):

    creds = service_account.Credentials.from_service_account_file(secrets, scopes=scopes)
    delegated_credentials = creds.with_subject(owner)
    return delegated_credentials

def walk(service, args, creds, owner):
    #get emails for blacklist
    gc = gspread.authorize(creds)
    sheet = gc.open_by_url(args.emailAddressBlacklist).sheet1
    blacklist_emails = sheet.col_values(1)

    for email in blacklist_emails:
        print(email)
        a(service, args, email, owner)

    df = pd.DataFrame(PERMISSION_DICT)
    df.to_csv('all-folder-permissions.csv')
    print('exported {num} total permissions'.format(num=len(df)))

    df = pd.DataFrame(PERMISSION_TO_REMOVE)
    df.to_csv('permissions_to_remove.csv')
    print('exported {num} permissions to remove'.format(num=len(df)))


def a(service, args, email, owner):
    q = "'{o}' in owners and '{w}' in writers".format(o=owner, w=email)

    params = {'q': q}
    response = service.files().list(**params).execute()

    for f in response['files']:
        permissionList = service.permissions().list(fileId=f['id'], fields='*').execute()

        permissions = []
        permissionIdsToRemove = []
        removed = False

        # Remove blacklisted permissions if exist
        for p in permissionList['permissions']:
            if 'emailAddress' in p:
                permissions.append('%s=%s' % (p['role'], p['emailAddress']))
                if p['role'] != 'owner' and p['emailAddress'] == email:
                    permissionIdsToRemove.append(p['id'])
                    PERMISSION_TO_REMOVE.append(
                        {'id': f['id'], 'name': f['name'], 'displayName': p['displayName'],
                         'email': p['emailAddress']})


        # Remove some if necessary
        if args.prod and len(permissionIdsToRemove):
            removed = True
            for permissionId in permissionIdsToRemove:
                print('removing permissions: %s' % ','.join(permissionIdsToRemove))
                try:
                    permissionList = service.permissions().delete(fileId=f['id'], permissionId=permissionId).execute()
                except:
                    print('unable to remove permissions for file: {f} for {u}'.format(f=f['id'], u=email))

        # Add permissions to dict
        if removed:
            permissionList = service.permissions().list(fileId=f['id'], fields='*').execute()
        for p in permissionList['permissions']:
            if 'emailAddress' in p:
                permissions.append('%s=%s' % (p['role'], p['emailAddress']))
        for p in permissions:
            PERMISSION_DICT.append(
                {'id': f['id'], 'name': f['name'], 'type': p.split('=')[0], 'person': p.split('=')[1]})

def main():
    parser = argparse.ArgumentParser(
        description='Walks Google Drive folder and emits csv with file/folders sharing permissions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    parser.add_argument('--emailAddressBlacklist', dest='emailAddressBlacklist', default=None,
                        help='if present will attempt to remove permissions from these email addresses')
    parser.add_argument('--prod', dest='prod', default=False, help='if true will remove permissions, otherwise just export what needs to be removed')
    parser.add_argument('--owners', dest='owners', nargs='*', default=[],
                        help='which files to query on')
    args = parser.parse_args()
    scope = ['https://www.googleapis.com/auth/drive', 'https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/spreadsheets.readonly',
             'https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive.readonly',
             'https://www.googleapis.com/auth/drive',
             'https://www.googleapis.com/auth/drive.file']

    for owner in args.owners:
        print('looking at drive for {o}'.format(o=owner))
        creds = get_credentials(scope, args, owner)
        service = build('drive', version='v3', credentials=creds)
        walk(service, args, get_gspread_creds(scope, args), owner)

def get_gspread_creds(scopes, flags, secrets='client_secret.json', storage='~/.credentials/google-drive-share.json'):
    store = file.Storage(os.path.expanduser(storage))
    creds = store.get()
    if creds is None or creds.invalid:
        flow = client.flow_from_clientsecrets(os.path.expanduser(secrets), scopes)
        creds = tools.run_flow(flow, store, flags)
    return creds

if __name__ == '__main__':
    main()