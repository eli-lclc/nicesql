from sqlalchemy import create_engine

'''
CONFIGURE EACH ENGINE
follow the format: 'mysql+pymysql://MySQLusername:MySQLpassword@host/database_name'

for example: 'mysql+pymysql://eli:password@LCLCN092/tasks'
        where LCLC is the handy label above my keyboard

note: i had some issues trying to get the root account to work, 
so I recommend creating another user with all the admin privileges 

PURPOSE OF EACH DATABASE
(you don't need to have/use all four, this is just how I divvy them up)
civicore: files downloaded from civicore (except for documents)
documents: stores all files relating to documents
participants: tables of who is on a particular grant/program
tasks: misc
'''


task_engine = create_engine('mysql+pymysql://MySQLusername:MySQLpassword@host/tasks', isolation_level="AUTOCOMMIT")
civicore_engine = create_engine('mysql+pymysql://MySQLusername:MySQLpassword@host/civicore', isolation_level="AUTOCOMMIT")
documents_engine = create_engine('mysql+pymysql://MySQLusername:MySQLpassword@host/documents', isolation_level="AUTOCOMMIT")
participants_engine = create_engine('mysql+pymysql://MySQLusername:MySQLpassword@host/participants', isolation_level="AUTOCOMMIT")