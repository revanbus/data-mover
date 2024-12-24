from multiprocessing import Pool
import os
import argparse  # for CLI options
from datetime import datetime
from shippingAndReceiving import MoveData
import logging
import sys
sys.path.insert(1, '.')
from myUtils import MyDB
"""
     Class:
       DataMovers

     Purpose:
        Configurations for moving data between RDS and S3 instances

     Inputs:
           # optional parameters
            --control_table_name:     Control table name
            --standard_template_name: Standard template name, default='v1_standard'
            --no_control_table:       Control table not used for this move default=False
            --single_run:             Job not run multiple times, default=False
            --remote_host:            Remote host, default=None
            --remote_db:              Remote DB, default=None
            --dev_host:               Host with bill of lading, default=None
            --dev_db:                 DB with bill of lading, default=None
            --temp_location:          Temp dump space type, default='network'
            --password:               Backup password, default=None
            --drop_fts:               Drop foreign tables before backup, default=True
            --source_db:              Name of database that was originally backed up, default=None
            --processing_threads:     Number of processing threads for backup and/or restore, default=2
            --version:                Production version
            --table:                  Multiple table names

            # required parameters
            --control_host:           Host with data move control table
            --control_db:             DB with data move control table
            --type:                   Move type

   """


class DataMovers:
    """ Refactored moveData with an Abstract Factory pattern """

    def __init__(self):
        pass

    @staticmethod
    def create(devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None, args=None):
        if theType == 'backup_runner' or theType == 'backup_lake' or theType == 'dev_databases' or \
                theType == 'raw_files' or theType == 'staging_database' or theType == 'production':
            data_mover_factory = BackupToS3(devDB=devDB, devHost=devHost, theType=theType, theRemoteDB=theRemoteDB,
                                            remoteHost=remoteHost, args=args)
        elif theType == 'runner_to_lake' or theType == 'move_schemas':
            data_mover_factory = MoveRDSToRDS(devDB=devDB, devHost=devHost, theType=theType, theRemoteDB=theRemoteDB,
                                              remoteHost=remoteHost, args=args)
        elif theType == 's3_to_lake' or theType == 's3_to_lake_partial':
            data_mover_factory = S3ToLake(devDB=devDB, devHost=devHost, theType=theType, theRemoteDB=theRemoteDB,
                                          remoteHost=remoteHost, args=args)
        elif theType == 'create_database':
            data_mover_factory = CreateDatabase(devDB=devDB, devHost=devHost, theType=theType, args=args)
        elif theType == 'structure_backup':
            data_mover_factory = StructureBackup(devDB=devDB, devHost=devHost, theType=theType, args=args)
        elif theType == 'build_runner_server':
            data_mover_factory = BuildRunnerServer(devDB=devDB, devHost=devHost, theType=theType,
                                                   theRemoteDB=theRemoteDB,
                                                   remoteHost=remoteHost, args=args)
        elif theType == 'staging_to_process':
            data_mover_factory = StagingToProcess(devDB=devDB, devHost=devHost, theType=theType,
                                                  theRemoteDB=theRemoteDB,
                                                  remoteHost=remoteHost, args=args)
        elif theType == 'process_to_staging':
            data_mover_factory = ProcessToStaging(devDB=devDB, devHost=devHost, theType=theType,
                                                  theRemoteDB=theRemoteDB,
                                                  remoteHost=remoteHost, args=args)
        else:
            raise Exception("Unknown mover type specified")

        return data_mover_factory


class AbstractTransport:
    """Abstract Class overridden by concrete data movers"""

    def __init__(self, devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None,
                 theArgs=None):
        self.devDB = devDB
        self.devHost = devHost
        self.theType = theType
        self.remoteDB = theRemoteDB
        self.remoteHost = remoteHost
        self.tempLocation = theArgs.temp_location
        self.password = theArgs.password
        self.args = theArgs
        self.jobList = None
        self.jobInfoList = None
        self.processingThreads = 3  # NOTE: value can range from 3-12 with 3 as a conservative default
        self.validateParameters()
        self.myDB = MyDB(self.devDB, self.devHost)
        self.devDBInfo = self.myDB.getConnectionInfo()
        self._logName = None
        self.version = theArgs.version
        self.tables = None
        self.templateDB = theArgs.standard_template_name
        self.billsOfLading = theArgs.control_table_name

    def validateParameters(self):
        assert (self.devDB is not None and
                self.devHost is not None and
                self.theType is not None), f"Missing required parameters for {self.theType} move"

    def getJobsFromControlTable(self):
        theSQL = f"""select id, table_name, schema_name, new_schema_name from {self.billsOfLading}
		                where include_flag='Y' and session_type = '{self.theType}'
		                order by sequence_id"""
        tableList = []
        tableData = self.myDB.runTheQuery(theSQL)
        for theRow in tableData:
            if theRow[1] is not None:  # if tablename is set
                theTableNames = {'id': theRow[0], 'objectName': theRow[1], 'objectType': 'table',
                                 'newSchemaName': theRow[3]}
            else:
                theTableNames = {'id': theRow[0], 'objectName': theRow[2], 'objectType': 'schema',
                                 'newSchemaName': theRow[3]}
            tableList.append(theTableNames)
        self.jobList = tableList

    def createJobInfoList(self):
        print("Configuring and Running Pipelines")

        # get the list of objects to transfer
        # send the job info as a dictionary because multiprocessing map function only supports a single parameter
        numJobs = len(self.jobList)
        assert numJobs > 0, "No jobs to process"

        print(f"Processing {numJobs} job(s):")
        startDate = datetime.today().strftime('%Y%m%d')
        jobInfoList = []
        for theJob in self.jobList:
            jobInfoList.append(
                {'id': theJob['id'], 'devDBInfo': self.devDBInfo, 'session': self.theType,
                 'objectName': theJob['objectName'],
                 'objectType': theJob['objectType'], 'remoteDBInfo': self.remoteDBInfo,
                 'schemaNameDest': theJob['newSchemaName'], 'startDate': startDate,
                 'args': self.args})
            print(f"{theJob['objectName']} ({theJob['objectType']})")
        self.jobInfoList = jobInfoList

    @staticmethod
    def transport(jobInfo):
        raise Exception("transport not overridden")

    def __str__(self):
        raise Exception("__str__ not overridden")

    def prepDataMoveSession(self, dropForeignTables):
        pass

    def cleanupDataMoveSession(self, recreateForeignTables=False):
        pass

    def describeTransport(self):
        raise Exception("describe_transport not overridden")

    def prepDatabase(self, dropForeignTables=False):
        print(f"Prepping the database {self.devHost}.{self.devDB}")

        # change table owner to my_superuser
        alterTableViewOwnerQueriesSQL = """
        select 'ALTER TABLE ' || nsp.nspname || '."' || cls.relname || '" OWNER TO my_superuser;'
	    from pg_class cls
	    join pg_roles rol on rol.oid = cls.relowner
	    join pg_namespace nsp on nsp.oid = cls.relnamespace
	    where nsp.nspname not in ('information_schema', 'pg_catalog')
	    and nsp.nspname not like 'pg_toast%'
	    and rol.rolname <> 'my_superuser' and rol.rolname <> 'rdsadmin'
	    and relkind <> 'c'
	    order by nsp.nspname, cls.relname;"""

        alterFunctionOwnerQueriesSQL = """   
        select  'ALTER FUNCTION ' || nsp.nspname || '.' || p.proname || '( ' || pg_get_function_identity_arguments(p.oid) || ' ) OWNER TO my_superuser;'
	        from pg_proc p
	            join pg_roles rol on rol.oid = p.proowner
	            left join pg_namespace nsp on p.pronamespace = nsp.oid
	        where nsp.nspname not in ('pg_catalog', 'information_schema')
	            and p.prokind = 'f'  -- NOTE: f for function and p for stored procedure
	            and rol.rolname <> 'my_superuser' and rol.rolname <> 'rdsadmin'
	        order by nsp.nspname,
	            p.proname;"""

        alterProcedureOwnerQueriesSQL = """   
        select  'ALTER PROCEDURE ' || nsp.nspname || '.' || p.proname || '( ' || pg_get_function_identity_arguments(p.oid) || ' ) OWNER TO my_superuser;'
	        from pg_proc p
	            join pg_roles rol on rol.oid = p.proowner
	            left join pg_namespace nsp on p.pronamespace = nsp.oid
	        where nsp.nspname not in ('pg_catalog', 'information_schema')
	            and p.prokind = 'p'  -- NOTE: f for function and p for stored procedure
	            and rol.rolname <> 'my_superuser' and rol.rolname <> 'rdsadmin'
	        order by nsp.nspname,
	            p.proname;  
	    """

        # generate the commands and save to alterQueries
        changeTableOwnerQueryList = self.myDB.runTheQuery(alterTableViewOwnerQueriesSQL)
        changeFunctionOwnerQueryList = self.myDB.runTheQuery(alterFunctionOwnerQueriesSQL)
        changeProcedureOwnerQueryList = self.myDB.runTheQuery(alterProcedureOwnerQueriesSQL)
        alterQueries = changeTableOwnerQueryList + changeFunctionOwnerQueryList + changeProcedureOwnerQueryList
        # alterQueries.insert(0, ['REASSIGN OWNED BY my_superuser TO rds_superuser;'])  # NOTE: this does not reassign all the needed objects

        print(alterQueries)
        for theQuery in alterQueries:
            theQuery = theQuery[0]
            print(theQuery)
            try:
                self.myDB.runTheQuery(theQuery, returnSomething=False)  # change owner to my_superuser
            except Exception as e:
                print(f"Postgres ERROR {e}")
                print(f"Unable to execute {theQuery}")
                print("Exception type:", type(e))

        # drop foreign tables (e.g. for a backup)
        if dropForeignTables:
            checkForFTLogTable = """SELECT EXISTS(
	            SELECT * 
	            FROM information_schema.tables 
	           WHERE  table_schema = 'logs' AND 
	              table_name = 'table_create_scripts_ft'
	        );"""
            tableLogExists = self.myDB.runTheQuery(checkForFTLogTable, returnSomething=True)
            if tableLogExists[0][0]:
                dropFTSQL = """call logs.sp_drop_foreign_tables();"""
                try:
                    self.myDB.runTheQuery(dropFTSQL, returnSomething=False)
                except Exception as e:
                    print(f"Postgres ERROR {e}")
                    print(f"Unable to execute {dropFTSQL}")
                    print("Exception type:", type(e))

    def recreateForeignTables(self, myDB):
        connInfo = myDB.getConnectionInfo()
        print(f"Recreating foreign tables for {connInfo['host']}.{connInfo['db_name']}")

        myDB.runTheQuery("CALL logs.sp_pre_processing();", returnSomething=False)

    def logStart(self):
        if self.args.logging is False:
            return
        self._logName = self.describeTransport()
        try:
            self.myDB.sp_insert_job_step('MoveData', self._logName)
        except Exception as e:
            print(f"Postgres ERROR {e}")
            print(f"Unable to execute sp_insert_job_step")
            print("Exception type:", type(e))
        finally:
            self._logName = None

    def logStop(self):
        if self.args.logging is False:
            return
        if self._logName is not None:
            self.myDB.sp_update_job_step('MoveData', self._logName)


# #############################################################################
#
#   MoveRDSToRDS Class
#
# #############################################################################
class MoveRDSToRDS(AbstractTransport):
    """Class for RDS-to-RDS transfers"""

    def __init__(self, devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None, args=None):
        AbstractTransport.__init__(self, devDB, devHost, theType, theRemoteDB, remoteHost, theArgs=args)

    def __str__(self):
        return "MoveRDSToRDS"

    def validateParameters(self):
        assert (self.devDB is not None and
                self.devHost is not None and
                self.theType is not None and
                self.remoteDB is not None and
                self.remoteHost is not None), f"Missing required parameters for {self.theType} move"

    def describeTransport(self):
        print(f"MoveRDSToRDS moves from {self.devHost}.{self.devDB} to {self.remoteHost}.{self.remoteDB} ")

    def prepDataMoveSession(self, dropForeignTables):
        self.prepDatabase(dropForeignTables=dropForeignTables)

    def cleanupDataMoveSession(self, recreateForeignTables=False):
        print("Cleaning up the move session")
        self.recreateForeignTables(self.myDB)


# #############################################################################
#
#   StagingToProcess Class
#
# #############################################################################
class StagingToProcess(AbstractTransport):
    """Class for moving staging data to seed the process server"""

    def __init__(self, devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None, args=None):
        AbstractTransport.__init__(self, devDB, devHost, theType, theRemoteDB, remoteHost, theArgs=args)
        self.myDBRemote = MyDB(self.remoteDB, self.remoteHost)
        self.remoteDBInfo = self.myDBRemote.getConnectionInfo()
        self.processingThreads = int(args.processing_threads)

    @staticmethod
    def transport(jobInfo):
        pipeline = MoveData(jobInfo)
        pipeline.dump(jobInfo['remoteDBInfo']).hashDumpFile().restore(jobInfo['devDBInfo']).moveSchema(
            jobInfo['devDBInfo'], jobInfo['schemaNameDest']).final()

    def __str__(self):
        return "StagingToProcess"

    def describeTransport(self):
        description = f"StagingToProcess moves from {self.remoteHost}.{self.remoteDB} to {self.devHost}.{self.devDB} "
        print(description)
        return description

    def prepDataMoveSession(self, dropForeignTables):
        self.prepDatabase(dropForeignTables=False)

    def run(self):
        self.logStart()
        self.getJobsFromControlTable()
        self.createJobInfoList()
        jobInfoList = self.jobInfoList  # NOTE: using a local variable and static method to avoid pickling class
        assert self.jobInfoList is not None, "No jobs to process"

        with Pool(self.processingThreads) as p:
            print(p.map(StagingToProcess.transport, jobInfoList))

        self.logStop()

    def cleanupDataMoveSession(self, recreateForeignTables=False):
        print("Cleaning up the move session")
    #  self.recreateForeignTables(self.myDB)


# #############################################################################
#
#   ProcessToStaging Class
#
# #############################################################################
class ProcessToStaging(AbstractTransport):
    """Class for saving process data back to the staging server"""

    def __init__(self, devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None, args=None):
        AbstractTransport.__init__(self, devDB, devHost, theType, theRemoteDB, remoteHost, theArgs=args)
        self.myDBRemote = MyDB(self.remoteDB, self.remoteHost)
        self.remoteDBInfo = self.myDBRemote.getConnectionInfo()
        self.processingThreads = int(args.processing_threads)

    @staticmethod
    def transport(jobInfo):
        pipeline = MoveData(jobInfo)
        pipeline.dump(jobInfo['devDBInfo']).hashDumpFile().restore(jobInfo['remoteDBInfo']).moveSchema(
            jobInfo['remoteDBInfo'], jobInfo['schemaNameDest']).final()

    def __str__(self):
        return "ProcessToStaging"

    def describeTransport(self):
        description = f"ProcessToStaging moves from  {self.devHost}.{self.devDB} to {self.remoteHost}.{self.remoteDB} "
        print(description)
        return description

    def prepDataMoveSession(self, dropForeignTables):
        self.prepDatabase(dropForeignTables=False)

    def run(self):
        self.logStart()
        self.getJobsFromControlTable()
        self.createJobInfoList()
        jobInfoList = self.jobInfoList  # NOTE: using a local variable and static method to avoid pickling class
        assert self.jobInfoList is not None, "No jobs to process"

        with Pool(self.processingThreads) as p:
            print(p.map(ProcessToStaging.transport, jobInfoList))

        self.logStop()

    def cleanupDataMoveSession(self, recreateForeignTables=False):
        print("Cleaning up the move session")
    #  self.recreateForeignTables(self.myDB)


# #############################################################################
#
#   BackupToS3 Class
#
# #############################################################################
class BackupToS3(AbstractTransport):
    """Class for Backing Up and Encrypting Data to S3"""

    def __init__(self, devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None, args=None):
        AbstractTransport.__init__(self, devDB, devHost, theType, theRemoteDB, remoteHost, theArgs=args)
        self.myDBRemote = MyDB(self.remoteDB, self.remoteHost)
        self.remoteDBInfo = self.myDBRemote.getConnectionInfo()
        self.processingThreads = int(args.processing_threads)
        # done in abstract class self.version = args.version
        if theType == 'production':
            assert self.version is not None, "Version not set for production run"

    @staticmethod
    def transport(jobInfo):
        remoteDBInfo = jobInfo['remoteDBInfo']
        remoteHost = remoteDBInfo['host']
        pipeline = MoveData(jobInfo)
        pipeline.dump(jobInfo['devDBInfo']).hashDumpFile().zip().hashZipFile().eTagHashZipFile().uploadToS3() \
            .writeResultsToBackupLog(remoteHost).final(singleRun=jobInfo['singleRun'])

    def __str__(self):
        return "BackupToS3"

    def describeTransport(self):
        print(f"BackupToS3 moves from {self.devHost}.{self.devDB} to S3 ")

    def prepDataMoveSession(self, dropForeignTables):
        self.prepDatabase(dropForeignTables=dropForeignTables)

    def run(self):
        self.logStart()
        self.getJobsFromControlTable()
        self.createJobInfoList()
        jobInfoList = self.jobInfoList  # NOTE: using a local variable and static method to avoid pickling class
        assert self.jobInfoList is not None, "No jobs to process"

        for i in range(0, len(jobInfoList)):
            jobInfoList[i]['singleRun'] = self.args.single_run
        with Pool(self.processingThreads) as p:
            print(p.map(BackupToS3.transport, jobInfoList))

        self.logStop()

    def cleanupDataMoveSession(self, recreateForeignTables=False):
        print("Cleaning up the move session")
        if recreateForeignTables:
            print('Recreating foreign tables')
            self.recreateForeignTables(self.myDB)


# #############################################################################
#
#   BuildRunnerServer Class
#
# #############################################################################
class BuildRunnerServer(AbstractTransport):
    """Class for populating a runner server"""

    def __init__(self, devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None, args=None):
        AbstractTransport.__init__(self, devDB, devHost, theType, theRemoteDB, remoteHost, theArgs=args)
        self.myDBRemote = MyDB(self.remoteDB, self.remoteHost)
        self.remoteDBInfo = self.myDBRemote.getConnectionInfo()

    @staticmethod
    def transport(jobInfo):
        jobInfo['devDBInfo']['db_name'] = jobInfo['args'].standard_template_name
        pipeline = MoveData(jobInfo)
        pipeline.dump(jobInfo['devDBInfo']).hashDumpFile().restore(jobInfo['remoteDBInfo']).final()

    def __str__(self):
        return "Build runner server"

    def describeTransport(self):
        print(f"Building {self.remoteHost}.{self.remoteDB} ")

    def run(self):
        self.getJobsFromControlTable()
        self.createJobInfoList()
        jobInfoList = self.jobInfoList  # NOTE: using a local variable and static method to avoid pickling class
        assert self.jobInfoList is not None, "No jobs to process"

        for theJob in jobInfoList:
            self.transport(theJob)

    def cleanupDataMoveSession(self, recreateForeignTables=False):
        afterSQL = """UPDATE client.processing_parameters
		SET value = NULL
		WHERE category = 'backup_key';"""

        self.myDB.runTheQuery(afterSQL, returnSomething=False)

    def prepDataMoveSession(self, dropForeignTables):
        greatSuccessSQL = f"""
		select success_flags = total_rows as great_success from (
			select sum(case when results= 'Success' then 1 else 0 end) as success_flags, count(*) as total_rows
			from {self.billsOfLading} 
			where session_type = 'build_runner_server'
			) a"""
        greatSuccessResults = self.myDB.runTheQuery(greatSuccessSQL, returnSomething=True)[0][0]
        includeFlagReset = ''
        if greatSuccessResults is True:
            includeFlagReset = f" include_flag='Y', "

        beforeSQL = f"""UPDATE {self.billsOfLading}
			SET {includeFlagReset} dump_hash=NULL, s3_location=NULL, start_time=NULL, end_time=NULL, running_time=NULL, 
			error_message=NULL, encrypted_password=NULL, s3_hash=NULL, zip_hash=NULL, new_schema_name=NULL, results=NULL
			WHERE session_type='build_runner_server';
			"""
        self.myDB.runTheQuery(beforeSQL, returnSomething=False)

    def validateParameters(self):
        assert (self.devDB is not None and
                self.devHost is not None and
                self.theType is not None and
                self.remoteDB is not None and
                self.remoteHost is not None), f"Missing required parameters for {self.theType} move"


# #############################################################################
#
#   TestFail Class
#
# #############################################################################
class TestFail(AbstractTransport):
    """Class for to test with that always fail"""

    def __init__(self, devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None, args=None):
        AbstractTransport.__init__(self, devDB, devHost, theType, theRemoteDB, remoteHost, theArgs=args)

    @staticmethod
    def transport(jobInfo):
        pipeline = MoveData(jobInfo)
        pipeline.test_fail().final()

    def __str__(self):
        return "Test Object Always Fails"

    def describeTransport(self):
        print(f"Test Fail object called for {self.devHost}.{self.devDB}")

    def prepDataMoveSession(self, dropForeignTables):
        pass

    def run(self):
        self.getJobsFromControlTable()
        self.createJobInfoList()
        jobInfoList = self.jobInfoList  # NOTE: using a local variable and static method to avoid pickling class
        assert self.jobInfoList is not None, "No jobs to process"

        with Pool(self.processingThreads) as p:
            print(p.map(TestFail.transport, jobInfoList))

    def cleanupDataMoveSession(self, recreateForeignTables=False):
        pass


# #############################################################################
#
#   StructureBackup Class
#
# #############################################################################
class StructureBackup(AbstractTransport):
    """Class for Backing Up and Encrypting Postgres Table DDLs and Functions to S3"""

    def __init__(self, devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None, args=None):
        AbstractTransport.__init__(self, devDB, devHost, theType, theRemoteDB, remoteHost, theArgs=args)
        self.remoteDBInfo = None

    @staticmethod
    def transport(jobInfo):
        pipeline = MoveData(jobInfo)
        pipeline.dump(jobInfo['devDBInfo'],
                      schemaOnly=True).hashDumpFile().zip().hashZipFile().eTagHashZipFile().uploadToS3().final(
            singleRun=False)

    def __str__(self):
        return "Backs up only the structure and not the data"

    def describeTransport(self):
        print(f"backup_structure called for {self.devHost}.{self.devDB}")

    def run(self):
        self.getJobsFromControlTable()
        self.createJobInfoList()
        jobInfoList = self.jobInfoList  # NOTE: using a local variable and static method to avoid pickling class
        assert self.jobInfoList is not None, "No jobs to process"

        with Pool(self.processingThreads) as p:
            print(p.map(StructureBackup.transport, jobInfoList))


# #############################################################################
#
#   DownloadFromS3 Class
#
# #############################################################################
class S3ToLake(AbstractTransport):
    """Class for Restoring Encrypted Backups from S3"""

    def __init__(self, devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None, args=None):
        AbstractTransport.__init__(self, devDB, devHost, theType, theRemoteDB, remoteHost, theArgs=args)
        remoteDBConnection = MyDB(theRemoteDB, remoteHost)
        self.remoteDBInfo = remoteDBConnection.getConnectionInfo()
        if args.processing_threads is None:
            self.processingThreads = 2
        else:
            self.processingThreads = args.processing_threads

        self.tempLocation = args.temp_location

        # process table information if specified
        if args.table is not None:
            self.tables = {}
            for fqualTable in args.table:
                fullTablename = fqualTable.split('.')
                if fullTablename[0] not in self.tables:
                    self.tables[fullTablename[0]] = []
                self.tables[fullTablename[0]].append(fullTablename[1])

        if self.args.source_db is None:
            self.target_db = self.remoteDB
        else:
            self.target_db = self.args.source_db

        if self.args.where_clause is None:
            self.where_clause = f"""session_type = 'backup_lake' and source_db = '{self.target_db}'"""
        else:
            self.where_clause = self.args.where_clause
            print(f"WHERE {self.where_clause}")

        print('DownloadFromS3 initialized')

    def __str__(self):
        return 'DownloadFromS3'

    def getJobsFromControlTable(self):

        AES_KEY = self.target_db + os.environ['BACKUP_SECRET']
        print(AES_KEY)
        theSQL = f"""SELECT max(id), object_name, object_type, PGP_SYM_DECRYPT(encrypted_password::bytea,'{AES_KEY}') 
                        as the_pwd
		             FROM client.data_backup_log
		             WHERE {self.where_clause}
		             GROUP BY object_name, object_type, the_pwd
		            ;"""
        tableList = []
        print(theSQL)
        tableData = self.myDB.runTheQuery(theSQL)
        for theRow in tableData:
            theTableNames = {'id': theRow[0], 'objectName': theRow[1], 'objectType': theRow[2],
                             'newSchemaName': None, 'thePassword': theRow[3]}
            tableList.append(theTableNames)
        self.jobList = tableList

    def createJobInfoList(self):
        print("Configuring and Running Pipelines")

        # get the list of objects to transfer
        # send the job info as a dictionary because multiprocessing map function only supports a single parameter
        numJobs = len(self.jobList)
        assert numJobs > 0, "No jobs to process"

        print(f"Processing {numJobs} job(s):")
        startDate = datetime.today().strftime('%Y%m%d')
        jobInfoList = []
        for theJob in self.jobList:
            if self.tables is not None and theJob['objectName'] not in self.tables:
                # if we are only doing a partial restore and the schema isn't one of the keys then skip
                continue
            if self.tables is not None:
                tableList = self.tables[theJob['objectName']]
            else:
                tableList = None
            jobInfoList.append(
                {'id': theJob['id'], 'devDBInfo': self.devDBInfo, 'session': self.theType,
                 'objectName': theJob['objectName'],
                 'objectType': theJob['objectType'], 'remoteDBInfo': self.remoteDBInfo,
                 'schemaNameDest': theJob['newSchemaName'], 'tempLocation': self.tempLocation, 'startDate': startDate,
                 'thePassword': theJob['thePassword'], 'theTables': tableList,
                 'args': self.args})
            print(f"{theJob['objectName']} ({theJob['objectType']})")
        self.jobInfoList = jobInfoList

    @staticmethod
    def transport(jobInfo):
        assert jobInfo['remoteDBInfo'] is not None, "No target database to restore to"
        pipeline = MoveData(jobInfo)
        pipeline.downloadFromS3().unzip(jobInfo['thePassword']).restore(jobInfo['remoteDBInfo'], jobInfo['tables'])

    def run(self):
        self.getJobsFromControlTable()
        self.createJobInfoList()
        jobInfoList = self.jobInfoList  # NOTE: using a local variable and static method to avoid pickling class
        assert self.jobInfoList is not None, "No jobs to process"

        # switching from multiprocessing to running in serial
        # with Pool(self.processingThreads) as p:
        # 	print(p.map(DownloadFromS3.transport, jobInfoList))
        for theJob in jobInfoList:
            theJob['remoteDBInfo'] = self.remoteDBInfo
            theJob['tables'] = self.tables
            self.transport(theJob)

    def describeTransport(self):
        print(f"DownloadFromS3 moves from S3 to {self.remoteHost}.{self.remoteDB} using control table "
              f"at {self.devHost}.{self.devDB}")

    def cleanupDataMoveSession(self, recreateForeignTables=False):
        print("Cleaning up the move session")


# #############################################################################
#
#   CreateDatabase Class
#
# #############################################################################
class CreateDatabase(AbstractTransport):
    """Class for Creating a New Database"""

    def __init__(self, devDB=None, devHost=None, theType=None, theRemoteDB=None, remoteHost=None, args=None):
        AbstractTransport.__init__(self, devDB, devHost, theType, theRemoteDB, remoteHost, theArgs=args)

    def run(self):
        self.createDatabase(createRoles=False)

    def describeTransport(self):
        print(f"Creating database {self.devHost}.{self.devDB}")

    def createDatabase(self, createRoles=False):
        theDevHost = self.devHost
        theDevDB = self.devDB

        print(f"Creating a new database {theDevDB} on {theDevHost}")
        myDB = MyDB('postgres',
                    theDevHost)  # NOTE: we know that postgres db will exist so connect there to create the new db
        # create the database using a connection to postgres
        createDatabaseSQL = f"""CREATE DATABASE {theDevDB};
	    """
        myDB.runTheQuery(createDatabaseSQL, returnSomething=False)
        alterDatabaseSQL = f"""ALTER DATABASE {theDevDB} OWNER TO my_superuser;
	        """
        myDB.runTheQuery(alterDatabaseSQL, returnSomething=False)

        # change the owner of public and add in the extensions
        myNewDB = MyDB(theDevDB, theDevHost)
        extensionsAndPrepSQL = """-- change public schema owner to my_superuser
	    ALTER SCHEMA public OWNER TO "my_superuser";

	    -- add in extensions
	    create extension if not exists dblink;
	    create extension if not exists tablefunc;
	    create extension if not exists postgis;
	    create extension if not exists "uuid-ossp";
	    create extension if not exists pgcrypto;
	    create extension if not exists postgres_fdw;
	    grant usage on FOREIGN DATA WRAPPER postgres_fdw to my_superuser;"""
        myNewDB.runTheQuery(extensionsAndPrepSQL, returnSomething=False)

        # optionally create the roles on the database
        if createRoles:
            roleSQL = """
	        -- create roles
	        -- Role: user1
	        DROP ROLE IF EXISTS user1;

	        CREATE ROLE user1 WITH
	            LOGIN
	            NOSUPERUSER
	            INHERIT
	            NOCREATEDB
	            NOCREATEROLE
	            NOREPLICATION;

	        GRANT my_superuser TO user1;
	        GRANT rds_superuser TO user1;

	        -- Role: user2
	        DROP ROLE IF EXISTS user2;

	        CREATE ROLE user2 WITH
	            LOGIN
	            NOSUPERUSER
	            INHERIT
	            CREATEDB
	            CREATEROLE
	            NOREPLICATION;

	        GRANT my_superuser TO user2 WITH ADMIN OPTION;
	        GRANT rds_superuser TO user2 WITH ADMIN OPTION;


	        """
            myNewDB.runTheQuery(roleSQL, returnSomething=False)
            print('Database created')


def main():
    parser = argparse.ArgumentParser(description='moveData')
    # optional parameters
    parser.add_argument('-ctn', '--control_table_name', help='Control table name',
                        default='client.data_bills_of_lading')
    parser.add_argument('-stn', '--standard_template_name', help='Standard template name', default='v1_standard')
    parser.add_argument('-nct', '--no_control_table', help='Control table not used for this move', default=False)
    parser.add_argument('-sr', '--single_run', help='Job not run multiple times', default=False)
    parser.add_argument('--logging', help='Use to turn off logging', default=True)
    parser.add_argument('-h2', '--remote_host', help='Remote host', default=None)
    parser.add_argument('-d2', '--remote_db', help='Remote DB', default=None)
    parser.add_argument('-h1', '--dev_host', help='Host with bill of lading', default=None)
    parser.add_argument('-d1', '--dev_db', help='DB with bill of lading', default=None)
    parser.add_argument('-m', '--temp_location', help='Temp dump space type', default='network')
    parser.add_argument('-p', '--password', help='Backup password', default=None)
    parser.add_argument('--drop_fts', help='Drop foreign tables before backup', default=True)
    parser.add_argument('--where_clause', help='Specify non-default backup schema(s) to restore from', default=None)
    parser.add_argument('--source_db', help='Name of database that was originally backed up', default=None)
    parser.add_argument('--client', help='Client associated with database', default='test_client')
    parser.add_argument('-j', '--processing_threads', help='Number of processing threads for backup and/or restore',
                        default=2)
    parser.add_argument('-v', '--version', help='Production version')
    parser.add_argument('--table', nargs='+')
    # required parameters
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-hc', '--control_host', help='Host with data move control table', required=True)
    requiredNamed.add_argument('-dc', '--control_db', help='DB with data move control table', required=True)
    requiredNamed.add_argument('-t', '--type', help='Move type', required=True)

    args = parser.parse_args()

    # ############################ TEST HARNESS #############################

    # set up logging
    uniq_filename = str(datetime.now().date()) + '.log'
    uniq_filename.replace('-', '')
    uniq_filename.replace(' ', '')

    level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(uniq_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.getLogger('boto3').setLevel(logging.WARN)
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('sqs_listener').setLevel(logging.WARN)
    logging.getLogger('bcdocs').setLevel(logging.WARN)
    logging.getLogger('urllib3').setLevel(logging.WARN)

    factory = DataMovers()
    dab_object = factory.create(devDB=args.control_db, devHost=args.control_host, theType=args.type,
                                theRemoteDB=args.remote_db, remoteHost=args.remote_host, args=args)

    dab_object.describeTransport()
    dab_object.prepDataMoveSession(dropForeignTables=args.drop_fts)
    dab_object.run()
    dab_object.cleanupDataMoveSession()


if __name__ == '__main__':
    main()
