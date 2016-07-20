import argparse
import random
import string
from datetime import datetime
from contextlib import closing

import boto3
import mysql.connector

parser = argparse.ArgumentParser()
parser.add_argument("--source-instance-id", required=True)
parser.add_argument("--master-user-name", required=True)
parser.add_argument("--master-user-password", required=True)
parser.add_argument("--new-instance-id", required=True)
parser.add_argument("--db-instance-class")
parser.add_argument("--engine-version")
parser.add_argument("--allocated-storage", type=int)
parser.add_argument("--iops", type=int)
args = parser.parse_args()

# unique string representing current second like 20160101090500
# to append to the name of resources created by this script
timestamp = str(datetime.utcnow()).replace('-', '').replace(':', '').replace(' ', '')[:14]

# TODO always use same timestamp
timestamp = 2

# will put this tag to every intermeatery resource crated by this tool
tag = {'Key': 'darbe'}

rds = boto3.client("rds")

print "getting master instance"
source_instance = rds.describe_db_instances(DBInstanceIdentifier=args.source_instance_id)['DBInstances'][0]
original_parameter_group = source_instance['DBParameterGroups'][0]['DBParameterGroupName']

print "connecting to read source instance to test credentials"
conn = mysql.connector.connect(user=args.master_user_name,
                               password=args.master_user_password,
                               host=source_instance['Endpoint']['Address'],
                               port=source_instance['Endpoint']['Port'])
with closing(conn):
    conn.ping()

new_parameter_group = "%s-writable-%s" % (original_parameter_group, timestamp)
# print "copying parameter group as:", new_parameter_group
# rds.copy_db_parameter_group(
#     SourceDBParameterGroupIdentifier=original_parameter_group,
#     TargetDBParameterGroupIdentifier=new_parameter_group,
#     TargetDBParameterGroupDescription="same with %s but set read_only to 0" % original_parameter_group,
#     Tags=[tag])

print "adding read_only=0 to new parameter group"
rds.modify_db_parameter_group(DBParameterGroupName=new_parameter_group,
                              Parameters=[
                                  {
                                      'ParameterName': 'read_only',
                                      'ParameterValue': '0',
                                      'ApplyMethod': 'pending-reboot',
                                  }
                              ])

read_replica_name = "%s-readreplica-%s" % (source_instance['DBInstanceIdentifier'], timestamp)
# print "crating read replica:", read_replica_name
# rds.create_db_instance_read_replica(
#     DBInstanceIdentifier=read_replica_name,
#     SourceDBInstanceIdentifier=master_instance['DBInstanceIdentifier'],
#     DBInstanceClass=master_instance['DBInstanceClass'],
#     AvailabilityZone=master_instance['AvailabilityZone'],
#     Tags=[tag])['DBInstance']

print "creating new db instance"
new_instance_params = dict(
        AllocatedStorage=args.allocated_storage or source_instance['AllocatedStorage'],
        AutoMinorVersionUpgrade=source_instance['AutoMinorVersionUpgrade'],
        AvailabilityZone=source_instance['AvailabilityZone'],
        BackupRetentionPeriod=source_instance['BackupRetentionPeriod'],
        CopyTagsToSnapshot=source_instance['CopyTagsToSnapshot'],
        DBInstanceClass=args.db_instance_class or source_instance['DBInstanceClass'],
        DBInstanceIdentifier=args.new_instance_id,
        DBName='darbe',  # will be removed after instance is created
        DBParameterGroupName=original_parameter_group,
        DBSubnetGroupName=source_instance['DBSubnetGroup']['DBSubnetGroupName'],
        Engine=source_instance['Engine'],
        EngineVersion=args.engine_version or source_instance['EngineVersion'],
        LicenseModel=source_instance['LicenseModel'],
        MasterUserPassword=args.master_user_password,
        MasterUsername=args.master_user_name,
        MultiAZ=False,  # should be False while importing for performance reason, will modify after import
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
    new_instance_params['Iops'] = source_instance['Iops']
if source_instance.get('MonitoringInterval', 0) > 0:
    new_instance_params['MonitoringInterval'] = source_instance['MonitoringInterval']
    new_instance_params['MonitoringRoleArn'] = source_instance['MonitoringRoleArn']
rds.create_db_instance(**new_instance_params)

print "waiting for read replica to become available"
waiter = rds.get_waiter('db_instance_available')
waiter.wait(DBInstanceIdentifier=read_replica_name)

print "getting details of created read replica"
read_replica_instance = rds.describe_db_instances(DBInstanceIdentifier=read_replica_name)['DBInstances'][0]

# print "modifying read replica instance parameters"
# rds.modify_db_instance(DBInstanceIdentifier=read_replica_name,
#                        DBParameterGroupName=new_parameter_group,
#                        BackupRetentionPeriod=1,
#                        ApplyImmediately=True)

# print "rebooting read replica instance"
# rds.reboot_db_instance(DBInstanceIdentifier=read_replica_name)

print "waiting for read replica to become available"
waiter = rds.get_waiter('db_instance_available')
waiter.wait(DBInstanceIdentifier=read_replica_name)

print "connecting to read replica"
conn = mysql.connector.connect(user=args.master_user_name,
                               password=args.master_user_password,
                               host=read_replica_instance['Endpoint']['Address'],
                               port=read_replica_instance['Endpoint']['Port'])
with closing(conn):
    print "creating replication user on read replica"
    repl_user_name = "darbe"
    repl_password = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(20))
    cursor = conn.cursor()
    with closing(cursor):
        cursor.execute("GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%' IDENTIFIED BY '%s'" %
                       (repl_user_name, repl_password))

    print "stopping replication on read replica"
    cursor = conn.cursor()
    with closing(cursor):
        cursor.callproc("mysql.rds_stop_replication")

    print "getting master status"
    cursor = conn.cursor()
    with closing(cursor):
        cursor.execute("SHOW MASTER STATUS")
        result = cursor.fetchone()
        binlog_filename, binlog_position = result[0], result[1]
        print "master status: filename:", binlog_filename, "position:", binlog_position

print "waiting for new instance to become available"
waiter = rds.get_waiter('db_instance_available')
waiter.wait(DBInstanceIdentifier=args.new_instance_id)

print "connecting to read replica"
conn = mysql.connector.connect(user=args.master_user_name,
                               password=args.master_user_password,
                               host=read_replica_instance['Endpoint']['Address'],
                               port=read_replica_instance['Endpoint']['Port'])
with closing(conn):
    print "getting master status"
    cursor = conn.cursor()
    with closing(cursor):
        cursor.execute("SHOW MASTER STATUS")
        result = cursor.fetchone()
        binlog_filename2, binlog_position2 = result[0], result[1]
        print "master status: filename:", binlog_filename2, "position:", binlog_position2

if (binlog_filename, binlog_position) != (binlog_filename2, binlog_position2):
    raise Exception("changed master position on read replica while dumping data")
