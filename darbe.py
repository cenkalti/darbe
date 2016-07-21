from __future__ import print_function

import argparse
import random
import string
import subprocess
import time
from datetime import datetime
from contextlib import closing, contextmanager

import boto3
import botocore.exceptions
import mysql.connector

print("checking required programs")
subprocess.check_call(['which', 'mysqldump'])
subprocess.check_call(['which', 'mysql'])

parser = argparse.ArgumentParser()
parser.add_argument("--source-instance-id", required=True)
parser.add_argument("--master-user-name", required=True)
parser.add_argument("--master-user-password", required=True)
parser.add_argument("--databases", required=True)
parser.add_argument("--new-instance-id", required=True)
parser.add_argument("--db-instance-class")
parser.add_argument("--engine-version")
parser.add_argument("--allocated-storage", type=int)
parser.add_argument("--iops", type=int)
parser.add_argument("--binlog-retention-hours", type=int, default=24)
args = parser.parse_args()

rds = boto3.client("rds")
db_instance_available = rds.get_waiter('db_instance_available')


@contextmanager
def connect_db(instance):
    conn = mysql.connector.connect(user=args.master_user_name,
                                   password=args.master_user_password,
                                   host=instance['Endpoint']['Address'],
                                   port=instance['Endpoint']['Port'])
    with closing(conn):
        cursor = conn.cursor()
        with closing(cursor):
            yield cursor


def wait_db_instance_available(instance_id):
    while True:
        try:
            db_instance_available.wait(DBInstanceIdentifier=instance_id)
        except botocore.exceptions.WaiterError:
            continue
        else:
            break


print("getting details of source instance")
source_instance = rds.describe_db_instances(DBInstanceIdentifier=args.source_instance_id)['DBInstances'][0]

print("setting binlog retention hours on source instance to:", args.binlog_retention_hours)
subprocess.check_call([
        'mysql',
        '-h', source_instance['Endpoint']['Address'],
        '-P', str(source_instance['Endpoint']['Port']),
        '-u', args.master_user_name,
        '-p%s' % args.master_user_password,
        '-e', "call mysql.rds_set_configuration('binlog retention hours', %i)" % args.binlog_retention_hours,
    ])

# unique string representing current second like 20160101090500
timestamp = str(datetime.utcnow()).replace('-', '').replace(':', '').replace(' ', '')[:14]
read_replica_instance_id = "%s-readreplica-%s" % (source_instance['DBInstanceIdentifier'], timestamp)
print("crating read replica:", read_replica_instance_id)
rds.create_db_instance_read_replica(DBInstanceIdentifier=read_replica_instance_id,
                                    SourceDBInstanceIdentifier=source_instance['DBInstanceIdentifier'],
                                    DBInstanceClass=source_instance['DBInstanceClass'],
                                    AvailabilityZone=source_instance['AvailabilityZone'])['DBInstance']

print("creating new db instance:", args.new_instance_id)
new_instance_params = dict(
        AllocatedStorage=args.allocated_storage or source_instance['AllocatedStorage'],
        AutoMinorVersionUpgrade=source_instance['AutoMinorVersionUpgrade'],
        AvailabilityZone=source_instance['AvailabilityZone'],
        BackupRetentionPeriod=0,  # will be enabled after import
        CopyTagsToSnapshot=source_instance['CopyTagsToSnapshot'],
        DBInstanceClass=args.db_instance_class or source_instance['DBInstanceClass'],
        DBInstanceIdentifier=args.new_instance_id,
        DBParameterGroupName=source_instance['DBParameterGroups'][0]['DBParameterGroupName'],
        DBSubnetGroupName=source_instance['DBSubnetGroup']['DBSubnetGroupName'],
        Engine=source_instance['Engine'],
        EngineVersion=args.engine_version or source_instance['EngineVersion'],
        LicenseModel=source_instance['LicenseModel'],
        MasterUserPassword=args.master_user_password,
        MasterUsername=args.master_user_name,
        MultiAZ=False,  # should be False for fast import, will change later
        OptionGroupName=source_instance['OptionGroupMemberships'][0]['OptionGroupName'],
        Port=source_instance['Endpoint']['Port'],
        PreferredBackupWindow=source_instance['PreferredBackupWindow'],
        PreferredMaintenanceWindow=source_instance['PreferredMaintenanceWindow'],
        PubliclyAccessible=source_instance['PubliclyAccessible'],
        StorageEncrypted=source_instance['StorageEncrypted'],
        StorageType=source_instance['StorageType'],
        VpcSecurityGroupIds=[g['VpcSecurityGroupId'] for g in source_instance['VpcSecurityGroups']],
    )
if source_instance.get('Iops', 0) > 0:
    new_instance_params['Iops'] = args.iops or source_instance['Iops']
if source_instance.get('MonitoringInterval', 0) > 0:
    new_instance_params['MonitoringInterval'] = source_instance['MonitoringInterval']
    new_instance_params['MonitoringRoleArn'] = source_instance['MonitoringRoleArn']
rds.create_db_instance(**new_instance_params)

print("waiting for read replica to become available")
wait_db_instance_available(read_replica_instance_id)

print("getting details of created read replica")
read_replica_instance = rds.describe_db_instances(DBInstanceIdentifier=read_replica_instance_id)['DBInstances'][0]

print("waiting for new instance to become available")
wait_db_instance_available(args.new_instance_id)

print("getting details of new instance")
new_instance = rds.describe_db_instances(DBInstanceIdentifier=args.new_instance_id)['DBInstances'][0]

print("stopping replication on read replica")
with connect_db(read_replica_instance) as cursor:
    cursor.callproc("mysql.rds_stop_replication")

    print("finding binlog position")
    cursor.execute("SHOW SLAVE STATUS")
    slave_status = dict(zip(cursor.column_names, cursor.fetchone()))

    binlog_filename, binlog_position = slave_status['Relay_Master_Log_File'], slave_status['Exec_Master_Log_Pos']
    print("master status: filename:", binlog_filename, "position:", binlog_position)

print("dumping data from read replica")
args = [
        'mysqldump',
        '-h', read_replica_instance['Endpoint']['Address'],
        '-P', str(read_replica_instance['Endpoint']['Port']),
        '-u', args.master_user_name,
        '-p%s' % args.master_user_password,
        '--single-transaction',
        '--order-by-primary',
        '--databases',
    ]
args.extend(args.databases.split(','))
dump = subprocess.Popen(args, stdout=subprocess.PIPE)

print("loading data to new instance")
load = subprocess.Popen([
        'mysql',
        '-h', new_instance['Endpoint']['Address'],
        '-P', str(new_instance['Endpoint']['Port']),
        '-u', args.master_user_name,
        '-p%s' % args.master_user_password,
    ], stdin=dump.stdout)

print("waiting for data transfer to finish")
load.wait()
assert load.returncode == 0
dump.wait()
assert dump.returncode == 0
print("data transfer is finished")

print("print deleting read replica instance")
rds.delete_db_instance(DBInstanceIdentifier=read_replica_instance_id, SkipFinalSnapshot=True)

print("creating replication user on source instance")
repl_user_name = "darbe"
repl_password = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(20))
with connect_db(source_instance) as cursor:
    cursor.execute("GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%' IDENTIFIED BY '%s'" % (repl_user_name, repl_password))

print("setting master on new instance")
with connect_db(new_instance) as cursor:
    cursor.callproc("mysql.rds_set_external_master",
                    (source_instance['Endpoint']['Address'], source_instance['Endpoint']['Port'], repl_user_name,
                     repl_password, binlog_filename, binlog_position, 0))

    print("starting replication on new instance")
    cursor.callproc("mysql.rds_start_replication")

    print("wating until new instance catches source instance")
    while True:
        cursor.execute("SHOW SLAVE STATUS")
        slave_status = dict(zip(cursor.column_names, cursor.fetchone()))
        seconds_behind_master = slave_status['Seconds_Behind_Master']
        print("seconds behind master:", seconds_behind_master)
        if seconds_behind_master < 1:
            break

        time.sleep(4)

changes = {}
if source_instance['BackupRetentionPeriod'] > 0:
    changes['BackupRetentionPeriod'] = source_instance['BackupRetentionPeriod']
    changes['PreferredBackupWindow'] = source_instance['PreferredBackupWindow']
if source_instance['MultiAZ']:
    changes['MultiAZ'] = source_instance['MultiAZ']
if changes:
    print("modifying new instance last time")
    rds.modify_db_instance(DBInstanceIdentifier=args.new_instance_id, ApplyImmediately=True, **changes)

print("all done")
