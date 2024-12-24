"""
     File:
       - MyDB Class

     Purpose:
        - Uniform library with call custom tailored to my environment
        - Calls can be made with database nicknames
        - Easy access to logging functions
        - Easy access to function to verify if a table exists

     Example:
       - test_db = MyDB('client_metadata', 'dev99')
       - test_result = test_db.runTheQuery('Select * from automation.client_name_lkp order by id', returnSomething=True)

"""
import psycopg2  # postgres module
import time
import socket
import boto3
from botocore.exceptions import ClientError
import json


class MyDB:
    def __init__(self, dbname, hostnameOrNickname, username='dbpython', password=None):
        self.libVersion = '3_1'
        self.conn = None
        self.cur = None
        self.errorCount = 0
        self.host = None
        self.username = username
        self.password = password
        self.hostNickname = MyDB.parseHostOrNickname(hostnameOrNickname)
        if hostnameOrNickname != self.hostNickname:
            # if nickname does not match the parameter then a hostname was passed in
            self.host = hostnameOrNickname
        self._lookupHostInformation()

        self.ip = socket.gethostbyname(self.host)

        self.dbname = dbname
        self.batchId = f"Host: {self.host} DBName: {self.dbname}"

    def _lookupHostInformation(self):
        key = f"DBLookup-{self.libVersion}-{self.hostNickname}-{self.username}"
        print(f"Checking for key ({key})")
        hostInfo = self.getHostInfo(key)
        hostInfo = json.loads(hostInfo)
        hostInfo = json.loads(hostInfo[key])
        self.username = hostInfo['username']
        self.password = hostInfo['password']
        self.host = hostInfo['host']
        self.port = hostInfo['port']

    @staticmethod
    def parseHostOrNickname(hostnameOrNickname):
        assert hostnameOrNickname is not None, "Host nickname not set"
        if '.' in hostnameOrNickname:
            nickname = hostnameOrNickname.split('.')[0]  # if a full hostname was passed in then create a nickname
            nickname = nickname.replace('db-', '')  # change something like db-dev99 to dev99
        else:
            nickname = hostnameOrNickname
        return nickname

    def getHostInfo(self, key):
        assert key is not None, "Key is not set for get_password()"
        print(f"Checking for {key}")
        # get password from Secrets Manager
        try:
            session = boto3.Session(profile_name='my_profile_name')
            client = session.client('secretsmanager')
            response = client.get_secret_value(
                SecretId=key
            )
            response = response['SecretString']
        except ClientError as e:
            response = None

        assert response is not None and response != 'None'
        return response

    @staticmethod
    def set_password(secret_string, key):
        assert key is not None, "Key is not set for set_password()"

        # save password in Secrets Manager
        session = boto3.Session(profile_name='my_profile_name')
        client = session.client('secretsmanager')
        response = client.create_secret(
            Name=key,
            SecretString=secret_string,
        )

        return response

    # Calling destructor
    def __del__(self):
        # print("Closing DB Connection")
        self.close()

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            self.cur = None

    def runTheQuery(self, theQuery, returnSomething=True, appendColname=False):
        data = []
        col_names = []
        start = time.time()
        if self.cur is None:
            self._cursor_client()

        try:
            self.cur.execute(theQuery)
            rowcount = self.cur.rowcount

            end = time.time()
            # print(f'Processing time: {(end - start):.1f} sec')
            if appendColname:
                for colName in self.cur.description:
                    col_names.append(colName[0])
                data.append(col_names)

            if returnSomething:
                for tuple_row in self.cur.fetchall():
                    tempList = list(tuple_row)
                    data.append(list(tempList))
                return data

        except psycopg2.Error as e:
            errMsg = "ERROR: Skipping QUERY due to postgres error " + str(e)
            print(errMsg)
            self.errorCount = self.errorCount + 1
            raise psycopg2.Error(errMsg)

        return rowcount

    def executeQuery(self, theQuery):
        data = []
        start = time.time()
        if self.cur is None:
            self._cursor_client()

        try:
            self.cur.execute(theQuery)
            end = time.time()
            print(f'Processing time: {(end - start):.1f} sec')

            return True

        except psycopg2.Error as e:
            errMsg = "ERROR: Skipping SP Call due to postgres error " + str(e)
            print(errMsg)
            self.errorCount = self.errorCount + 1
            exit(-1)

        return False

    def getConnectionInfo(self):
        # connection string variables set in class initializer
        return {"username": self.username, "password": self.password, "host": self.host, "db_name": self.dbname,
                "host_nickname": self.hostNickname, "ip": self.ip}

    def _cursor_client(self):
        assert self.password is not None, "Password is not set in _cursor_client"
        conn_string = f"host={self.host} dbname={self.dbname} user={self.username} password={self.password}"
        self.conn = psycopg2.connect(conn_string)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def getCursor(self):
        if self.cur is None:
            self._cursor_client()

        return self.cur

    def sp_insert_job_step(self, section, step):
        theQuery = f"CALL logs.sp_insert_job_step('{section}', '{step}');"
        try:
            self.runTheQuery(theQuery=theQuery, returnSomething=False)
            print(theQuery)
        except Exception as e:
            print('Unable to write to log')
        return

    def sp_update_job_step(self, section, step):
        try:
            theQuery = f"CALL logs.sp_update_job_step('{section}', '{step}');"
            self.runTheQuery(theQuery=theQuery, returnSomething=False)
            print(theQuery)
        except Exception as e:
            print("Unable to update the log")
        return

    def sp_update_set_error_message(self, section, step, errorMessage):
        theQuery = f"CALL logs.sp_update_set_error_message('{section}', '{step}', '{errorMessage}');"
        self.runTheQuery(theQuery=theQuery, returnSomething=False)
        print(theQuery)
        return

    def table_exists_with_data(self, table_name=None, min_rows=100):
        assert table_name is not None, "table_name not specified for table_exists_with_data()"
        assert '.' in table_name, "table_name must specify the schema"
        test_sql = f"""SELECT * FROM {table_name} LIMIT {min_rows}"""
        returned_rows = self.runTheQuery(theQuery=test_sql, returnSomething=True)

        if len(returned_rows) >= min_rows:
            return True
        else:
            return False
