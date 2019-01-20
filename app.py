import sys
import sqlite3
from argparse import ArgumentParser
from db import DBSchemaManager
from db import DBManager

parser = ArgumentParser(description='Test task application')

parser.add_argument("-i", "--init", dest="init", help="Command to start initialization of app", action='store_true')

parser.add_argument("-uid", "--user_id", dest="user_id", type=int,
                    help="USER ID for search all closest users", metavar="USER_ID")

args = parser.parse_args()
if args.init is True:
    print('Are you sure you want to continue initialization (Y/n)')
    ans = input()
    if ans == 'Y':
        schema = DBSchemaManager()
        schema.restore()
        schema.save_json_data()
    else:
        sys.exit('Exit init')

if isinstance(args.user_id, int):
    try:
        db_manager = DBManager()
        colleagues = db_manager.get_colleagues_by_user_id(args.user_id)
        for colleague in colleagues.fetchall():
            print(colleague['Name'])
    except sqlite3.OperationalError:
        print('Please, initialize the application!')
