"""
     Class:
       MoveData

     Purpose:
        Moves table or schema from on DB to another DB or to S3. Can perform various specified transformations
        during the move.

     Example:
        params = DotMap()
        params.no_control_table   = True
        params.password           = None
        params.drop_fts           = False
        params.source_db          = None
        params.processing_threads = 2
        params.control_table_name = 'promotion.promotion_metadata_test'
        jobInfo = {'id': 1, 'devDBInfo': devDBInfo, 'session': type,
                   'objectName': test_schema,
                   'objectType': 'schema', 'remoteDBInfo': remoteDBInfo,
                   'schemaNameDest': None, 'tempLocation': 'network', 'startDate': '00000000', 'args':params}

        # simple schema move
        pipeline = MoveData(jobInfo)
        pipeline.dump(jobInfo['devDBInfo']).hashDumpFile().restore(jobInfo['remoteDBInfo']).final()

    Environment:
        C:/Users/<user_home_dir>/.aws/credentials    # file for access to AWS SecretsManager
        conda activate c:/envs/prod/DataMover        # has all the necessary modules loaded

"""
import os
import re
import boto3  # for AWS operations
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from myUtils import MyDB
import hashlib
import string
import secrets
import winreg
import tempfile
from random import randint
from time import sleep
import subprocess


class MoveData(object):

    def __init__(self, jobInfo):
        assert jobInfo['session'] is not None, "Session type not set"
        assert os.environ['MY_SECRET_CODE_HERE'] is not None, "Env var MY_SECRET_CODE_HERE not set"
        self.errLog              = 'ShippingAndReceiving Errors:'
        self.pgRestoreErrorCount = 0
        self.pgDumpErrorCount    = 0
        self.jobArgs             = jobInfo['args']
        if self.jobArgs.logging == False:
            print('Not logging to staging')
        self.billsOfLading = self.jobArgs.control_table_name
        devDBInfo          = jobInfo['devDBInfo']
        remoteDBInfo       = jobInfo['remoteDBInfo']
        sleep(randint(10, 30))  # sleep 10 to 30 seconds on startup
        self._id           = jobInfo['id']
        self._devDatabase  = devDBInfo['db_name']
        self._devServer    = devDBInfo['host']
        self._username     = devDBInfo['username']
        self._password     = devDBInfo['password']
        if remoteDBInfo is not None:
            self._databaseDest = remoteDBInfo['db_name']
            self._destDB       = MyDB(remoteDBInfo['db_name'], remoteDBInfo['host'],
                                        username=remoteDBInfo['username'], password=remoteDBInfo['password'])
            self._hostDest     = remoteDBInfo['host']
        else:
            self._databaseDest = None

        self._sessionType     = jobInfo['session']
        self._objectName      = jobInfo['objectName']
        self._objectType      = jobInfo['objectType']
        self._typeOfTempSpace = jobInfo['args'].temp_location

        if 's3:' not in self._devServer:
            self._devDB = MyDB(self._devDatabase, self._devServer,
                                    username=self._username, password=self._password)
        self._host               = self._devServer
        os.environ['PGPASSWORD'] = self._password
        self._startDate          = jobInfo['startDate']
        self.PASSWORD_LENGTH     = 128
        self._AES_KEY            = self._devDatabase + os.environ['MY_SECRET_CODE_HERE']

        if self.jobArgs.no_control_table:  # NOTE: This is required because client not set in v1_standard
            print("Not using control table")
            self._client       = self.jobArgs.client
            self._log_events   = False
            self._clientData   = None
            if hasattr(self.jobArgs, 'backup_prefix'):
                self._backupPrefix = self.jobArgs.backup_prefix
                self._s3BasePath   = self.jobArgs.s3_base_path
                self._s3FilePrefix = self.jobArgs.s3_file_prefix
                self._bucket       = self.jobArgs.bucket
            self._uploadedETag = None
            if hasattr(self.jobArgs, 'archive_password'):
                self.generatedPassword = self.jobArgs.archive_password
                assert self.generatedPassword is not None, "Archive password not set"
        else:  # Do these things if this is for a client
            print("Using control table")
            self._log_events     = True
            client_name_response = self._devDB.runTheQuery("""
                SELECT value 
                FROM client.processing_parameters 
                WHERE category='client_name';""")
            self._client = client_name_response[0][0]
            if self._client is None:
                raise Exception(f"Client not set in client.processing_parameters")
            self._client = self._client.lower()

            # do these checks:
            # 1 - verify client specified is a known client (will raise exception if unknown)
            self._clientData  = self.get_client_metadata()
            print(self._clientData)

            # 2 - verify any transfer source and target db match
            if remoteDBInfo is not None:
                debug_str = f"Client: {self._client} DBDest: {self._databaseDest} StagingDB: {self._clientData['staging_db']}"
                print(debug_str)
                assert self._client in self._databaseDest or self._databaseDest == self._clientData['staging_db'],  "db destination name must contain client str or be the staging db"

            # 3 - verify client specified in client.processing parameter is part of DB name
            assert self._client in self._devDatabase or self._devDatabase == 'dev_demo' or self._devDatabase == 'metadata', "db name must contain client str or be in ['dev_demo', 'metadata']"

            backup_prefix_response = self._devDB.runTheQuery("""Select value 
                                                                from client.processing_parameters 
                                                                where category='backup_prefix'""")
            self._backupPrefix = backup_prefix_response[0][0]
            self._s3BasePath   = self.generateS3Filepath()['basePath']
            self._s3FilePrefix = self.generateS3Filepath()['filenamePrefix']
            self.generatedPassword = self.setSavedPassword(
                jobInfo.get('thePassword'))  # if a password has been sent then use that, otherwise None is set
            assert self.generatedPassword is not None, "Archive password not set"

        # override logging flag set above
        if self.jobArgs.logging == False:
            self._log_events = False

        self.FILE_BASE_DIR = self.generateLocalFilepath()
        self.FILENAME = None
        # check for install path in same 32/64 bit as OS
        self.PG_DUMP_DIR = MoveData.getToolLocation() + '\\runtime\\'
        print(f"Tool location is {self.PG_DUMP_DIR}")
        self.ZIP_DIR = 'C:\\Program Files\\7-Zip\\'
        assert os.path.exists(self.ZIP_DIR + '7z.exe'), "7zip is not installed at " + self.ZIP_DIR + '7z.exe'
        assert os.path.exists(
            self.PG_DUMP_DIR + 'pg_dump.exe'), "pg_dump is not installed at " + self.PG_DUMP_DIR + 'pg_dump.exe'


        self.UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB -- 8388608 bytes
        if self._clientData:  # clientData will only exist if moving to S3
            self._bucket           = self._clientData['backup_bucket']
            self._uploadedETag     = None

        self._fullyQualifiedDumpFile = self._objectName.replace('.', '_')
        self._fullyQualifiedDumpFile = f"{self.FILE_BASE_DIR}\\{self._fullyQualifiedDumpFile}.dump"
        self._fullyQualifiedZipFile  = self._fullyQualifiedDumpFile.replace('.dump', '.7z')
        self._zipHash   = None
        self._localETag = None
        self._dumpHash  = None

        self.writeTableValue('start_time', 'CURRENT_TIMESTAMP')

    def get_client_metadata(self):
        assert self._client is not None, "Client name is not set. Unable to determine correct backup bucket"
        metadataDB  = MyDB('metadata', 'dev1')
        client_data = metadataDB.runTheQuery(f"""
                                    SELECT * 
                                    FROM automation.client_name_lkp
                                    WHERE client_name='{self._client}';""")[0]

        if len(client_data) > 0:
            client_metadata = {    'id':             client_data[0],
                                   'client_name':    client_data[1],
                                   'active':         client_data[2],
                                   'transfer_to':    client_data[3],
                                   'backup_bucket':  client_data[4],
                                   'staging_db':     client_data[5]
            }
        else:
            raise Exception(f"Unable to locate metadata for client {self._client} in automation.client_name_lkp")

        return client_metadata

    def test_fail(self):
        assert True is False, "test_fail() testing method called"

        return self

    @staticmethod
    def getToolLocation(root=winreg.HKEY_LOCAL_MACHINE):
        check_paths = [r'SOFTWARE\WOW6432Node\pgAdmin 4\v4\InstallPath',
                       r'SOFTWARE\WOW6432Node\pgAdmin 4\v5\InstallPath',
                       r'SOFTWARE\WOW6432Node\pgAdmin 4\v6\InstallPath',
                       r'SOFTWARE\WOW6432Node\pgAdmin 4\v7\InstallPath',
                       r'SOFTWARE\WOW6432Node\pgAdmin 4\v8\InstallPath',
                       r'SOFTWARE\pgAdmin 4\v4\InstallPath',
                       r'SOFTWARE\pgAdmin 4\v5\InstallPath',
                       r'SOFTWARE\pgAdmin 4\v6\InstallPath',
                       r'SOFTWARE\pgAdmin 4\v7\InstallPath',
                       r'SOFTWARE\pgAdmin 4\v8\InstallPath'
                       ]
        for path in check_paths:
            try:
                path, name = os.path.split(path)
                with winreg.OpenKey(root, path) as key:
                    value = winreg.QueryValueEx(key, name)[0]
                    if value is not None:
                        return value
            except FileNotFoundError:
                pass

    @staticmethod
    def generatePassword(PASSWORD_LENGTH):
        alphabet = string.ascii_letters + string.digits
        #  symbols = '~!@#$%^&*()-_=+[{]}\|;:/?.>,<'
        alphabet = alphabet.replace('0', '').replace('O', '').replace('l', '')  # remove confusing characters

        while True:
            password = ''.join(secrets.choice(alphabet) for _ in range(PASSWORD_LENGTH))
            if (any(c.islower() for c in password)
                    and any(c.isupper() for c in password)
                    and sum(c.isdigit() for c in password) >= 3):
                passList = [password[i:i + 4] for i in range(0, len(password), 4)]
                splitPass = '.'
                splitPass = splitPass.join(passList)

                return splitPass

    def writeTableValue(self, attribute, value):
        if self._log_events == False:  # if we're not logging any events then skip
            print("No events logged for this data move")
            return

        if 'CURRENT_TIMESTAMP' in value or 'DATE_PART' in value:
            pass
        else:
            value = "'" + value + "'"
        theSQL = f"""UPDATE {self.billsOfLading} SET {attribute}={value} WHERE id={self._id}"""
        self._devDB.runTheQuery(theSQL, returnSomething=False)

    def writeResultsToBackupLog(self, stage_host=None):
        if stage_host is None:
            stage_host = 'dev1'
            print(f'Defaulting to default staging server: {stage_host}')

        if self.jobArgs.logging == False:
            print(f'logging turned off decrypt password is {self.generatedPassword}')
            return self

        # transfer the zip file to S3
        splitPath = os.path.split(self._fullyQualifiedZipFile)
        s3DestinationFilename = splitPath[1]
        s3DestinationFilename = self._s3FilePrefix + s3DestinationFilename

        # we have all the log information we need and save it with an update query
        updateSQL = f"""INSERT INTO client.data_backup_log
                        (results, session_type, source_db, object_name, 
                        object_type, s3_location, start_date, end_time, 
                         error_message, encrypted_password)
                        VALUES('Success', '{self._sessionType}', '{self._devDatabase}', '{self._objectName}', 
                        '{self._objectType}', '{self._s3BasePath}{s3DestinationFilename}', '{self._startDate}', NOW(), '', 
                        PGP_SYM_ENCRYPT('{self.generatedPassword}','{self._AES_KEY}'));
                        """
        # print(updateSQL)
        if self._databaseDest is None:
            staging_db_name = self._clientData['staging_db']
        else:
            staging_db_name = self._databaseDest
            assert self._client.lower() in staging_db_name or self._databaseDest in self._clientData['staging_db'], f"Staging Data Backup Log {staging_db_name} does not appear to be correct for Client {self._client.lower()}"

        print(f"Logging data backup to {stage_host}.{staging_db_name}")
        staging_session = MyDB(staging_db_name, stage_host)
        rows_affected   = staging_session.runTheQuery(updateSQL, returnSomething=False)
        if rows_affected == 0:
            raise Exception('Unable to store backup log')

        return self

    def generateS3Filepath(self):
        assert self._backupPrefix is not None, "Backup prefix not set"
        basePath = f'{self._sessionType}/{self._devDatabase}/{self._startDate}/'
        filenamePrefix = f'{self._backupPrefix}_{self._sessionType}_'
        return {'basePath': basePath, 'filenamePrefix': filenamePrefix}

    def generateLocalFilepath(self):
        if self._typeOfTempSpace == 'network':
            thePath = f'X:\\Clients\\{self._client}\\moveData'
        else:
            thePath = f'X:\\Clients\\{self._client}\\moveData'
        if not os.path.isdir(thePath):
            os.makedirs(thePath)
        thePath = tempfile.mkdtemp(dir=thePath, prefix=self._sessionType + '_')
        if not os.path.isdir(thePath):
            os.makedirs(thePath)

        return thePath

    def cleanupTempDir(self):
        try:
            print(f"Removing {self._fullyQualifiedDumpFile} and any related zip files")
            if self._fullyQualifiedDumpFile is not None and os.path.exists(self._fullyQualifiedDumpFile):
                os.remove(self._fullyQualifiedDumpFile)
            if self._fullyQualifiedZipFile is not None and os.path.exists(self._fullyQualifiedZipFile):
                os.remove(self._fullyQualifiedZipFile)
            if len(os.listdir(self.FILE_BASE_DIR)) == 0:  # check to see if directory is empty to remove it
                os.rmdir(self.FILE_BASE_DIR)
        except OSError as e:
            print("Error: %s" % e.strerror)

    def setSavedPassword(self, suggestedPass=None):
        # Logic: suggestedPass = abc  storedPass = abc  => return abc
        #        suggestedPass = abc  storedPass = None => save suggested
        #        suggestedPass = None storedPass = abc  => retrieve stored
        #        suggestedPass = None storedPass = None => generate new one
        #        suggestedPass = abc  storedPass = xyz  => throw exception

        # Try to decrypt the password in client.processing_parameters
        theSql = f"""Select PGP_SYM_DECRYPT(value::bytea,'{self._AES_KEY}')
                     from client.processing_parameters pp 
                     where category='backup_key';
                                            """

        storedPass = self._devDB.runTheQuery(theSql, returnSomething=True)
        storedPass = storedPass[0][0]

        if suggestedPass is not None and storedPass is not None:
            if suggestedPass != storedPass:
                print('WARNING: Suggested and stored password do not match.')
                raise IOError('Stored password and command line password do not match')
            return suggestedPass
        elif suggestedPass is not None and storedPass is None:
            # store suggestedPass
            pass
        elif suggestedPass is None and storedPass is not None:
            return storedPass
        elif suggestedPass is None and storedPass is None:
            # if password not set in DB then generate one and save it
            print("Generating and saving encrypted password to client.processing_parameters")
            suggestedPass = MoveData.generatePassword(self.PASSWORD_LENGTH)

        theSql = f"""UPDATE client.processing_parameters
                     SET value=( PGP_SYM_ENCRYPT('{suggestedPass}','{self._AES_KEY}') )
                     WHERE category='backup_key';
                                """
        # print(theSql)
        storedPass = self._devDB.runTheQuery(theSql, returnSomething=False)
        assert storedPass is not False, 'Unable to store or retrieve backup_key'

        return suggestedPass

    def findForeignTablesInSchema(self, schemaName):
        findTablesSQL = f"""select foreign_table_name from information_schema.foreign_tables
            where foreign_table_name not like '%_ft'
            and foreign_table_schema = '{schemaName}';"""
        tablesToExclude = self._devDB.runTheQuery(findTablesSQL, returnSomething=True)
        tableExclusionString = " -T '*_ft' "
        if len(tablesToExclude):  # if the query found some tables to exclude
            for excludedTable in tablesToExclude:
                tableExclusionString += f" -T {excludedTable[0]}"

        return tableExclusionString

    def purgeIdleSessions(self):
        try:
            self._devDB.runTheQuery(
                f"""CALL base.kill_1_day_idle_sessions();""",
                returnSomething=False)

            self._devDB.runTheQuery(
                f"""CALL base.kill_idle_in_transaction_sessions();""",
                returnSomething=False)

        except Exception as e:
            errMsg = f"An error occurred with purging idle sessions {e}"
            print(errMsg)
            self.writeTableValue('error_message', 'Purge Idle Sessions Error')
            self.writeTableValue('results', 'Error')
            raise IOError

        finally:
            return self

    def dump(self, dbInfo, schemaOnly=False, tableExcludePattern=None):
        # set table exclusion flag
        tableExcludeFlag = ''
        if tableExcludePattern is not None and self._objectType == 'schema':
            tableExcludeFlag = f'-T {self._objectName}.{tableExcludePattern}'

        if schemaOnly:
            assert self._objectType != 'table', f"Cannot dump a table with --schema-only"
            print(f"Schema-Only Dump called for {self._objectName}")
            dataFlag = ' --schema-only '
        else:
            print(f"Dump called for {self._objectName}")
            dataFlag = ' '
        os.chdir(self.PG_DUMP_DIR)
        print(f"Dumping {self._objectType} {self._objectName}")

        if self._objectType == 'table':
            objectFlag = '-t'
        elif self._objectType == 'schema':
            objectFlag = '-n'
        else:
            errMsg = f"'Unknown object type ({self._objectType}) to dump'"
            self.writeTableValue('error_message', 'Dump Error')
            self.writeTableValue('results', 'Error')
            raise Exception(errMsg)
        # NOTE use exclude flag to remove views from dump file e.g. c:\Program Files\pgAdmin 4\v4\runtime>pg_dump -Fc -h my-dev99.abcd123.us-east-1.rds.amazonaws.com -d v99_dev_demo -U dbpython -n logs --exclude-table=logs.vw* -f C:\temp\demo_log_without_view5.dump
        theCommand = f"pg_dump -Fc -h {dbInfo['host']} -d {dbInfo['db_name']} -U dbpython {dataFlag} {objectFlag} {self._objectName} {tableExcludeFlag} -f {self._fullyQualifiedDumpFile}"
        print(f"Dumping object at {self.PG_DUMP_DIR} with {theCommand}")

        try:
            # subprocess.check_output(theCommand)
            print(theCommand)
            commandOutput = subprocess.run(theCommand, text=True, capture_output=True)

            print(commandOutput.stdout)
            print(commandOutput.stderr)

            pgDumpErrLog = 'PG_DUMP Output \r\n'
            pgDumpErrLog += commandOutput.stdout
            pgDumpErrLog += commandOutput.stderr

            self.errLog += pgDumpErrLog
            self.pgDumpErrorCount = len(re.findall('(?= error:)', pgDumpErrLog.lower()))
        except Exception as e:
            errMsg = f"An error occurred with pg_dump {e}"
            print(errMsg)
            self.writeTableValue('error_message', 'Dump Error')
            self.writeTableValue('results', 'Error')
            raise IOError
        finally:
            return self

    def hashDumpFile(self):
        print(f"hashDumpFile called for {self._objectName}")
        self._dumpHash = MoveData.calculateLocalMD5(self._fullyQualifiedDumpFile)
        print(f"The dump file hash was {self._dumpHash}")
        self.writeTableValue('dump_hash', f'{self._dumpHash}')
        return self

    def zip(self):
        assert self.generatedPassword is not None, "Generated password is not set"
        print(f"zip called for {self._objectName}")
        print(f"Zipping up {self._fullyQualifiedDumpFile}")
        # zip up the files with a password
        self._fullyQualifiedZipFile = self._fullyQualifiedDumpFile.replace('.dump', '.7z')
        myPassword = self.generatedPassword
        os.chdir(self.ZIP_DIR)
        theCommand = f'7z a -bt -mx3 -p{myPassword} {self._fullyQualifiedZipFile} {self._fullyQualifiedDumpFile}'
        try:
            subprocess.check_output(theCommand)
        except Exception as e:
            errMsg = f"An error occurred with zipping/encrypting {e}"
            print(errMsg)
            self.writeTableValue('error_message', 'Zip Error')
            self.writeTableValue('results', 'Error')
            raise IOError

        # expectation: zip file exists at location and has a password
        assert os.path.exists(
            self._fullyQualifiedZipFile), f"Generated zip file ({self._fullyQualifiedZipFile}) does not exist"
        return self

    def finalWithErrorLogs(self):
        # just return the error log summary for now

        return f"Restore Error Count: {self.pgRestoreErrorCount} Dump Error Count: {self.pgDumpErrorCount}"

    def final(self, singleRun=True):
        # that's a wrap! set the end time
        self.writeTableValue('end_time', 'CURRENT_TIMESTAMP')

        # set the running_time
        diffSQL = """(DATE_PART('day', end_time::timestamp - start_time::timestamp) * 24 + 
               DATE_PART('hour', end_time::timestamp - start_time::timestamp)) * 60 +
               DATE_PART('minute', end_time::timestamp - start_time::timestamp)"""
        self.writeTableValue('running_time', diffSQL)

        # if it hasn't died by this point then assume success
        self.writeTableValue('results', 'Success')

        if not singleRun:
            # when only run once, set the include_flag to 'N'
            self.writeTableValue('include_flag', 'N')

        # delete the dump file
        self.cleanupTempDir()
        return 'Success'

    def hashZipFile(self):
        print(f"hashZipFile called for {self._objectName}")
        self._zipHash = MoveData.calculateLocalMD5(self._fullyQualifiedZipFile)
        print(f"The zip file hash was {self._zipHash}")
        self.writeTableValue('zip_hash', f'{self._zipHash}')
        return self

    def eTagHashZipFile(self):
        print(f"eTagHashZipFile called for {self._objectName}")
        self._localETag = MoveData.calculateEtagChecksum(self._fullyQualifiedZipFile, self.UPLOAD_CHUNK_SIZE)
        print(f"S3's eTag file hash predicted to be {self._localETag}")
        self.writeTableValue('s3_hash', f'{self._localETag}')
        return self

    def uploadToS3(self):
        print(f"uploadToS3 called for {self._objectName}")
        print(f"Backup Password will be ({'*'*16})")
        try:
            _s3Session = boto3.session.Session(profile_name='backup')
            _s3 = _s3Session.resource('s3')
        except ClientError as e:
            errMsg = str(e)
            print('Error: ' + errMsg)
            print(
                f"ERROR: The bucket {self._bucket} may not exist, you may not have access, or you may need to set your MFA key")
            self.writeTableValue('error_message', 'S3 Upload Error')
            self.writeTableValue('results', 'Error')
            raise IOError

        # Set the desired multipart threshold value (5GB)
        config = TransferConfig(multipart_threshold=self.UPLOAD_CHUNK_SIZE,
                                multipart_chunksize=self.UPLOAD_CHUNK_SIZE)

        # transfer the zip file to S3
        splitPath = os.path.split(self._fullyQualifiedZipFile)
        s3DestinationFilename = splitPath[1]
        s3DestinationFilename = self._s3FilePrefix + s3DestinationFilename

        print(
            f"Uploading file {self._fullyQualifiedZipFile} to {self._bucket} at {self._s3BasePath + s3DestinationFilename}")
        _s3.meta.client.upload_file(self._fullyQualifiedZipFile, self._bucket,
                                    self._s3BasePath + s3DestinationFilename, Config=config)

        # expectation: file exists in S3 -- tested when eTag is retrieved
        # expectation: uploaded MD5 matches single or multipart S3 MD5. More info -> https://stackoverflow.com/questions/26415923/boto-get-md5-s3-file
        obj = _s3.Object(bucket_name=self._bucket, key=self._s3BasePath + s3DestinationFilename)
        s3ETag = str(obj.e_tag).replace('"', '')
        self._uploadedETag = s3ETag
        if MoveData.etagCompare(s3ETag, self._zipHash, self._localETag):
            self.writeTableValue('s3_location', self._s3BasePath + s3DestinationFilename)
        else:
            raise IOError(f"Hash does not match S3 hash for uploaded file {self._fullyQualifiedZipFile}")

        return self

    def sql(self, dbInfo, theSQL):
        print(f"sql script called for {self._objectName}")
        targetDB = MyDB(dbInfo['db_name'], dbInfo['host'])
        targetDB.runTheQuery(theSQL, returnSomething=False)

    def dropNewTables(self, dbInfo, tableList):
        # iterate through list of tables
        for tableName in tableList:
            # table name must contain _new
            if '_new.' in tableName:
                # drop the table
                sql = f"DROP TABLE IF EXISTS {tableName};"
                print(f"Pretending to execute {sql}")
                # returnValue = self._devDB.runTheQuery(sql)
            else:
                print(f"Skipping {tableName} because does not contain substring '_new.' ")

        return self

    def restore(self, dbInfo, tableList=None, verifyRestore=False):
        os.environ['PGPASSWORD'] = dbInfo['password']
        print(f"restore called for {self._objectName}")

        # process_to_staging and staging move types drop the table before the restore
        targetDB = MyDB(dbInfo['db_name'], dbInfo['host'])
        if (self._sessionType == 'process_to_staging' and 'staging' in dbInfo['db_name'])\
                or ('_new.' in self._objectName):
            cleanCommand = f"DROP TABLE IF EXISTS {self._objectName}"
            targetDB.runTheQuery(cleanCommand, returnSomething=False)

        # if only restoring table then schema must exist
        if self._objectType == 'table':
            schema_name = self._objectName.split('.')
            schema_name = schema_name[0]
            print(f"Creating schema {schema_name}")
            targetDB.runTheQuery(f'CREATE SCHEMA IF NOT EXISTS {schema_name}', returnSomething=False)

        # special processing for partial restores
        tableParams = ' '
        if tableList is not None:
            tableParams += ' -t '
            tableParams += ' -t '.join(tableList[self._objectName])
            tableParams += ' '
            # table restore will fail if the schema doesn't exist
            print(f"Creating schema {self._objectName}")
            targetDB.runTheQuery(f'CREATE SCHEMA IF NOT EXISTS {self._objectName}', returnSomething=False)

        outputFilename = self._objectName.replace('.', '_')
        outputFilename = f"{self.FILE_BASE_DIR}\\{outputFilename}.dump"
        os.chdir(self.PG_DUMP_DIR)
        print(f"Restoring table {self._objectName} on {dbInfo['host']}.{dbInfo['db_name']}")
        theCommand = f"pg_restore -v --no-data-for-failed-tables -j 4 -d {dbInfo['db_name']} -h {dbInfo['host']} -U dbpython {tableParams} {outputFilename}"

        try:
            print(theCommand)
            commandOutput = subprocess.run(theCommand, text=True, capture_output=True)

            print(commandOutput.stdout)
            print(commandOutput.stderr)

            pgRestoreErrLog = 'PG_RESTORE Output \r\n'
            pgRestoreErrLog += commandOutput.stdout
            pgRestoreErrLog += commandOutput.stderr

            self.errLog += pgRestoreErrLog
            self.pgRestoreErrorCount = len(re.findall('(?= error:)', pgRestoreErrLog.lower()))

        except Exception as e:
            errMsg = f"An error occurred with restoring {e}"
            print(errMsg)
            self.writeTableValue('error_message', 'Restore Error')
            self.writeTableValue('results', 'Error')

        return self

    def moveSchema(self, dbInfo, newSchema):
        if newSchema is None:
            return self
        # if it's safe to drop the table at the destination, then do that to prevent automation from halting
        dropQuery = ''
        if self._sessionType == 'process_to_staging' and 'staging' in dbInfo['db_name']:
            newTablename = newSchema + "." + self._objectName.split('.', 1)[1]
            dropQuery = f"drop table if exists {newTablename};"
        myDB = MyDB(dbInfo['db_name'], dbInfo['host'], host=dbInfo['host'], username=dbInfo['username'],
                         password=dbInfo['password'])
        print(f'Moving {self._objectName} to {newSchema}')
        theSql = f"{dropQuery} ALTER TABLE {self._objectName} SET SCHEMA {newSchema};"
        print(theSql)
        myDB.runTheQuery(theSql, returnSomething=False)
        return self

    @staticmethod
    def calculateLocalMD5(zipFilename):
        with open(zipFilename, "rb") as f:
            file_hash = hashlib.md5()
            chunk = f.read(8192)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(8192)

        theHash = file_hash.hexdigest()

        return theHash

    @staticmethod
    def calculateEtagChecksum(filename, chunk_size):
        print(f"Calculating Etag hash for {filename}")

        md5s = []
        with open(filename, 'rb') as f:
            for data in iter(lambda: f.read(chunk_size), b''):
                md5s.append(hashlib.md5(data).digest())
        m = hashlib.md5(b"".join(md5s))

        return '{}-{}'.format(m.hexdigest(), len(md5s))

    @staticmethod
    def etagCompare(etag, zipHash, localETag):
        if not etag or not zipHash or not localETag:
            print("WARNING: Not all hash values present for comparison in etagCompare( ). Skipping.")
            return True
        et = etag.replace('"', '')  # strip quotes

        if etag == zipHash:
            return True
        elif et == localETag:
            return True
        else:
            return False

    def downloadFromS3(self, s3Location=None):
        print(f"downloadFromS3 called for {self._objectName}")
        print(f"Backup Password should be ({'*'*16})")
        try:
            _s3Session = boto3.session.Session(profile_name='backup')
            _s3 = _s3Session.resource('s3')
        except ClientError as e:
            errMsg = str(e)
            print('Error: ' + errMsg)
            print(
                f"ERROR: The bucket {self._bucket} may not exist, you may not have access, or you may need to set your MFA key")
            self.writeTableValue('error_message', 'S3 Download Error')
            self.writeTableValue('results', 'Error')
            raise IOError

        # calculate the path to the S3 object
        if s3Location is None:
            sourceLocationSql = f"""SELECT s3_location FROM client.data_backup_log 
                                    WHERE object_name ='{self._objectName}'
                                    AND id = {self._id} 
                                    LIMIT 1"""
            print(sourceLocationSql)
            s3Location = self._devDB.runTheQuery(sourceLocationSql)[0][0]
        assert self._fullyQualifiedZipFile is not None

        print(f"Downloading file {s3Location} from {self._bucket}/{s3Location} as {self._fullyQualifiedZipFile}")
        _s3.meta.client.download_file(self._bucket, s3Location, self._fullyQualifiedZipFile)

        # expectation: partially downloaded / corrupt zip file won't unzip without error

        return self

    def unzip(self,  storedPass):
        assert self._fullyQualifiedZipFile is not None
        assert self._fullyQualifiedDumpFile is not None
        assert self._password is not None

        print(f"unzipping {self._fullyQualifiedZipFile}")
        # unzip archive with a password
        print(f"Changing to {self.ZIP_DIR}")
        os.chdir(self.FILE_BASE_DIR)

        try:
            system = subprocess.Popen([f"{self.ZIP_DIR}7z", "e", self._fullyQualifiedZipFile, f"-p{storedPass}" ])
            print(system.communicate())
        except Exception as e:
            errMsg = f"An error occurred with unzipping: {e}"
            print(errMsg)
            self.writeTableValue('error_message', 'Zip Error')
            self.writeTableValue('results', 'Error')
            raise IOError

        # expectation: zip file exists at location and has a password
        assert os.path.exists(
            self._fullyQualifiedZipFile), f"Generated zip file ({self._fullyQualifiedZipFile}) does not exist"

        return self
