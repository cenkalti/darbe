import argparse
import subprocess
import logging
import time
import re
from datetime import datetime
from contextlib import closing, contextmanager

import boto3
import botocore.exceptions
import mysql.connector

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="print debug logs")
    parser.add_argument("--region", required=True, help="AWS region name")
    parser.add_argument(
        "--source-instance-id",
        required=True,
        help="name of the existing instance (This is going to be master when replication is setup.)")
    parser.add_argument(
        "--new-instance-id", required=True, help="name of the slave instance that is going to be created")
    parser.add_argument(
        "--master-user-name", required=True, help="master username of instance specified with --source-instance-id")
    parser.add_argument(
        "--master-user-password",
        required=True,
        help="master user password of instance specified with --source-instance-id")
    parser.add_argument(
        "--databases", required=True, help="comma separated database names that need to be copied to slave")
    parser.add_argument("--users", help="comma separated user names that need to be copied to slave")
    parser.add_argument("--availability-zone", help="set it if you want slave on different availability zone")
    parser.add_argument("--db-instance-class", help="set it if you want different instance class on slave")
    parser.add_argument("--engine-version", help="set it if you want different engine version on slave")
    parser.add_argument("--parameter-group", help="set it if you want different parameter group on slave")
    parser.add_argument("--option-group", help="set it if you want different option group on slave")
    parser.add_argument(
        "--allocated-storage", type=int, help="set it if you want to grow/shrink storage space on slave")
    parser.add_argument(
        "--iops",
        type=int,
        help="set it if you want different IOPS on slave (must be valid for given --allocated-storage)")
    parser.add_argument(
        "--binlog-retention-hours",
        type=int,
        default=24,
        help="Darbe set 'binlog retention hours' on master to allow enough time for copying data between instances."
        "Increase if your data is too big so that it cannot be copied in 24 hours.")
    args = parser.parse_args()

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    logger.info("checking required programs")
    subprocess.check_call(['which', 'mysqldump'])
    subprocess.check_call(['which', 'mysql'])

    rds = boto3.client('rds', region_name=args.region)
    ec2 = boto3.client('ec2', region_name=args.region)

    db_instance_available = rds.get_waiter('db_instance_available')

    # unique string representing current second like 20160101090500
    timestamp = str(datetime.utcnow()).replace('-', '').replace(':', '').replace(' ', '')[:14]

    @contextmanager
    def connect_db(instance):
        """Yields a cursor on a new connection to a database."""
        conn = mysql.connector.connect(
            user=args.master_user_name,
            password=args.master_user_password,
            host=instance['Endpoint']['Address'],
            port=instance['Endpoint']['Port'])
        with closing(conn):
            cursor = conn.cursor()
            with closing(cursor):
                yield cursor

    def wait_db_instance_available(instance_id):
        """Timeout on waiter cannot be changed. We keep continue to wait on timeout error."""
        while True:
            try:
                db_instance_available.wait(DBInstanceIdentifier=instance_id)
            except botocore.exceptions.WaiterError:
                continue
            else:
                break

    def wait_until_zero_lag(instance):
        """Blocks until replication lag is zero."""
        while True:
            time.sleep(4)
            try:
                with connect_db(instance) as cursor:
                    cursor.execute("SHOW SLAVE STATUS")
                    slave_status = dict(zip(cursor.column_names, cursor.fetchone()))
            except Exception as e:
                logger.error(e)
            else:
                seconds_behind_master = slave_status['Seconds_Behind_Master']
                logger.info("seconds behind master: %s", seconds_behind_master)
                if seconds_behind_master is None:
                    continue

                if seconds_behind_master < 1:
                    break

    logger.info("getting details of source instance")
    source_instance = rds.describe_db_instances(DBInstanceIdentifier=args.source_instance_id)['DBInstances'][0]

    logger.info("creating replication security group")
    vpc_id = source_instance['DBSubnetGroup']['VpcId']
    try:
        response = ec2.create_security_group(
            GroupName="darbe-replication",
            VpcId=vpc_id,
            Description="created by darbe for replication between instances")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'InvalidGroup.Duplicate':
            raise

        logger.info("security group already exists")
        security_group_id = ec2.describe_security_groups(Filters=[{
            'Name': 'vpc-id',
            "Values": [vpc_id]
        }, {
            'Name': 'group-name',
            'Values': ['darbe-replication']
        }])['SecurityGroups'][0]['GroupId']
    else:
        security_group_id = response['GroupId']

    logger.info("modifying security group rules: %s", security_group_id)
    try:
        ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[{
                'IpProtocol': 'tcp',
                'FromPort': 3306,
                'ToPort': 3306,
                'IpRanges': [{
                    'CidrIp': '0.0.0.0/0'
                }]
            }])
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'InvalidPermission.Duplicate':
            raise

        logger.info("security group permission already exists")

    security_group_ids = [g['VpcSecurityGroupId'] for g in source_instance['VpcSecurityGroups']]
    if security_group_id in security_group_ids:
        logger.info("replication security group is already attached to the source instance")
    else:
        logger.info("adding replication security group to the source instance")
        security_group_ids.append(security_group_id)
        rds.modify_db_instance(DBInstanceIdentifier=args.source_instance_id, VpcSecurityGroupIds=security_group_ids)

        logger.info("waiting for source instance to become available")
        time.sleep(60)  # instance state does not switch to "modifying" immediately
        wait_db_instance_available(args.source_instance_id)

    grants = []
    if args.users:
        logger.info("getting grants from source instance")
        with connect_db(source_instance) as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            match = re.match('(\d+)\.(\d+)\.(\d+)', version)
            version = tuple(map(int, match.groups()))
            if version < (5, 7, 6):
                password_column = 'Password'
            else:
                password_column = 'authentication_string'

            users_in = ','.join(map(lambda x: "'%s'" % x, args.users.split(',')))
            sql = "SELECT User, Host, %s FROM mysql.user WHERE User in (%s)" % (password_column, users_in)
            logger.debug("running sql: %s", sql)
            cursor.execute(sql)
            for user, host, password in cursor.fetchall():
                logger.debug("user: %s, host: %s, password: %s", user, host, password)
                full_user = "'%s'%'%s'" % (user, host)
                if version >= (5, 7, 6):
                    cursor.execute("SHOW CREATE USER %s" % full_user)
                    create_user_sql = cursor.fetchall()[0][0]
                    grants.append(create_user_sql)

                cursor.execute("SHOW GRANTS FOR %s" % full_user)
                for grant in cursor.fetchall():
                    grant = str(grant[0])
                    logger.debug("grant: %s", grant)
                    if 'IDENTIFIED BY' in grant:
                        grant = grant.replace("<secret>", "'%s'" % password)

                    grants.append(grant)

    logger.info("setting binlog retention hours on source instance to: %s", args.binlog_retention_hours)
    # setting via mysql.connector gives an error. don't know why.
    subprocess.check_call([
        'mysql',
        '-h',
        source_instance['Endpoint']['Address'],
        '-P',
        str(source_instance['Endpoint']['Port']),
        '-u',
        args.master_user_name,
        '-p%s' % args.master_user_password,
        '--safe-updates=FALSE',
        '-e',
        "call mysql.rds_set_configuration('binlog retention hours', %i)" % args.binlog_retention_hours,
    ])

    original_parameter_group = args.parameter_group or source_instance['DBParameterGroups'][0]['DBParameterGroupName']
    match = re.match('.+-darbe-(\d+)', original_parameter_group)
    if match:
        new_parameter_group = original_parameter_group.replace(match.groups()[0], timestamp)
    else:
        new_parameter_group = "%s-darbe-%s" % (original_parameter_group, timestamp)
    logger.info("copying parameter group as: %s", new_parameter_group)
    rds.copy_db_parameter_group(
        SourceDBParameterGroupIdentifier=original_parameter_group,
        TargetDBParameterGroupIdentifier=new_parameter_group,
        TargetDBParameterGroupDescription="copied from %s then modified" % original_parameter_group)

    logger.info("modifying new parameter group")
    rds.modify_db_parameter_group(
        DBParameterGroupName=new_parameter_group,
        # these parameters makes slave sql thread run faster,
        # otherwise slave may not catch up with the master for write intensive load.
        Parameters=[
            {
                'ParameterName': 'innodb_flush_log_at_trx_commit',
                'ParameterValue': '2',
                'ApplyMethod': 'immediate',
            },
            {
                'ParameterName': 'sync_binlog',
                'ParameterValue': '0',
                'ApplyMethod': 'immediate',
            },
        ])

    logger.info("creating new db instance: %s", args.new_instance_id)
    new_instance_params = dict(
        AllocatedStorage=args.allocated_storage or source_instance['AllocatedStorage'],
        AutoMinorVersionUpgrade=source_instance['AutoMinorVersionUpgrade'],
        AvailabilityZone=args.availability_zone or source_instance['AvailabilityZone'],
        BackupRetentionPeriod=0,  # should be disabled for fast import, will be enabled after import
        CopyTagsToSnapshot=source_instance['CopyTagsToSnapshot'],
        DBInstanceClass=args.db_instance_class or source_instance['DBInstanceClass'],
        DBInstanceIdentifier=args.new_instance_id,
        DBParameterGroupName=new_parameter_group,
        DBSubnetGroupName=source_instance['DBSubnetGroup']['DBSubnetGroupName'],
        Engine=source_instance['Engine'],
        EngineVersion=args.engine_version or source_instance['EngineVersion'],
        LicenseModel=source_instance['LicenseModel'],
        MasterUserPassword=args.master_user_password,
        MasterUsername=args.master_user_name,
        OptionGroupName=args.option_group or source_instance['OptionGroupMemberships'][0]['OptionGroupName'],
        MultiAZ=False,  # should be disabled for fast import, will be enabled after import
        Port=source_instance['Endpoint']['Port'],
        PreferredBackupWindow=source_instance['PreferredBackupWindow'],
        PreferredMaintenanceWindow=source_instance['PreferredMaintenanceWindow'],
        PubliclyAccessible=source_instance['PubliclyAccessible'],
        StorageEncrypted=source_instance['StorageEncrypted'],
        StorageType=source_instance['StorageType'],
        VpcSecurityGroupIds=security_group_ids, )
    if source_instance.get('Iops', 0) > 0:
        new_instance_params['Iops'] = args.iops or source_instance['Iops']
    if source_instance.get('MonitoringInterval', 0) > 0:
        new_instance_params['MonitoringInterval'] = source_instance['MonitoringInterval']
        new_instance_params['MonitoringRoleArn'] = source_instance['MonitoringRoleArn']
    rds.create_db_instance(**new_instance_params)

    read_replica_instance_id = "%s-readreplica-%s" % (source_instance['DBInstanceIdentifier'], timestamp)
    logger.info("crating read replica: %s", read_replica_instance_id)
    rds.create_db_instance_read_replica(
        DBInstanceIdentifier=read_replica_instance_id,
        SourceDBInstanceIdentifier=source_instance['DBInstanceIdentifier'],
        DBInstanceClass=source_instance['DBInstanceClass'],
        AvailabilityZone=source_instance['AvailabilityZone'])['DBInstance']

    logger.info("waiting for new instance to become available")
    wait_db_instance_available(args.new_instance_id)

    logger.info("getting details of new instance")
    new_instance = rds.describe_db_instances(DBInstanceIdentifier=args.new_instance_id)['DBInstances'][0]

    logger.info("waiting for read replica to become available")
    wait_db_instance_available(read_replica_instance_id)

    logger.info("getting details of created read replica")
    read_replica_instance = rds.describe_db_instances(DBInstanceIdentifier=read_replica_instance_id)['DBInstances'][0]

    logger.info("stopping replication on read replica")
    with connect_db(read_replica_instance) as cursor:
        cursor.callproc("mysql.rds_stop_replication")

        logger.info("finding binlog position")
        cursor.execute("SHOW SLAVE STATUS")
        slave_status = dict(zip(cursor.column_names, cursor.fetchone()))

        binlog_filename, binlog_position = slave_status['Relay_Master_Log_File'], slave_status['Exec_Master_Log_Pos']
        logger.info("master status: filename: %s position: %s", binlog_filename, binlog_position)

    logger.info("dumping data from read replica")
    dump_args = [
        'mysqldump',
        '-h',
        read_replica_instance['Endpoint']['Address'],
        '-P',
        str(read_replica_instance['Endpoint']['Port']),
        '-u',
        args.master_user_name,
        '-p%s' % args.master_user_password,
        '--single-transaction',
        '--order-by-primary',
        '--databases',
    ]
    dump_args.extend(args.databases.split(','))
    dump = subprocess.Popen(dump_args, stdout=subprocess.PIPE)

    logger.info("loading data to new instance")
    load = subprocess.Popen(
        [
            'mysql',
            '-h',
            new_instance['Endpoint']['Address'],
            '-P',
            str(new_instance['Endpoint']['Port']),
            '-u',
            args.master_user_name,
            '-p%s' % args.master_user_password,
            '-f',
        ],
        stdin=dump.stdout)

    logger.info("waiting for data transfer to finish")
    load.wait()
    assert load.returncode == 0
    dump.wait()
    assert dump.returncode == 0
    logger.info("data transfer is finished")

    logger.info("deleting read replica instance")
    rds.delete_db_instance(DBInstanceIdentifier=read_replica_instance_id, SkipFinalSnapshot=True)

    logger.info("creating replication user on source instance")
    with connect_db(source_instance) as cursor:
        cursor.execute("GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%' IDENTIFIED BY '%s'" %
                       (args.master_user_name, args.master_user_password))

    logger.info("setting master on new instance")
    with connect_db(new_instance) as cursor:
        cursor.callproc("mysql.rds_set_external_master",
                        (source_instance['Endpoint']['Address'], source_instance['Endpoint']['Port'],
                         args.master_user_name, args.master_user_password, binlog_filename, binlog_position, 0))

        logger.info("starting replication on new instance")
        cursor.callproc("mysql.rds_start_replication")

        if grants:
            logger.info("creating users on new instance")
            for grant in grants:
                logger.debug("executing grant sql: %s", grant)
                cursor.execute(grant)

    logger.info("wating until new instance catches source instance")
    wait_until_zero_lag(new_instance)

    changes = {}
    if source_instance['BackupRetentionPeriod'] > 0:
        changes['BackupRetentionPeriod'] = source_instance['BackupRetentionPeriod']
        changes['PreferredBackupWindow'] = source_instance['PreferredBackupWindow']
    if source_instance['MultiAZ']:
        changes['MultiAZ'] = source_instance['MultiAZ']
    if changes:
        logger.info("modifying new instance last time")
        rds.modify_db_instance(DBInstanceIdentifier=args.new_instance_id, ApplyImmediately=True, **changes)

        logger.info("waiting for new instance to become available")
        time.sleep(60)  # instance state does not switch to "modifying" immediately
        wait_db_instance_available(args.new_instance_id)

        logger.info("wating until new instance catches source instance")
        wait_until_zero_lag(new_instance)

    logger.info("all done")


if __name__ == '__main__':
    main()
