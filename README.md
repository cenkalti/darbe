# Darbe
RDS MySQL replication setup tool

# What is it?
Darbe is for setting up a manual replication between RDS instances of MySQL.
Manual replication has 2 benefits over creating a Read Replica:
* You can **shrink tablespace used by InnoDB engine**.
  * When you create a Read Replica, Amazon RDS takes a snapshot of the source instance and creates a read-only instance from the snapshot so the tablespace used by InnoDB is copied directly to the Read Replica.
  * When you use Darbe, it creates an empty instace and imports the data from source instace with the help of mysqldump tool.
* You can **apply migrations without downtime**.
  * After you apply migrations on a Read Replica, you have to promote it to master in order to detach it from the source instace. Read Replica is restarted in this process.
  * After you apply migrations on a slave created by Darbe, you can switch your application to the slave without need to restart.

# Usage
Install Darbe on a server that is the same availability zone with your source instance:
```
pip install --upgrade darbe
```

Run Darbe and wait for it to copy data between instances.
Basic usage is like:
```
darbe \
  --region eu-west-1 \
  --source-instance-id putio16 \
  --new-instance-id putio17
  --master-user-name root \
  --master-user-password XXX \
  --databases putio,mogilefs \
  --users putio,mogile \
```

You may override some properties on the new instance.
Run `darbe --help` to see available options.

After Darbe successfully exited, a new instance is created and setup as a slave to your source instance.
Replication lag is zero at this point.

Run any migrations and DDL statements on the slave.
While doing it don't break the replication.
See http://dev.mysql.com/doc/refman/5.7/en/replication-features-differing-tables.html to see what is allowed.

If you did run any statement on the slave, wait until the replication lag is zero again.
Run `SHOW SLAVE STATUS;` on the slave to verify `Seconds_Behind_Master` is 0.

Switch your application to use the slave instance.
You must wait for all connections on master instance to end, before writing to the slave instance.
If your application writes to the slave, the changes cannot be seen by the connections in the master instance
because the replication is one-way.
At [put.io](https://put.io) we use [tcpproxy](https://github.com/cenk/tcpproxy) tool in front of our database instances
to guarantee that connections are proxied to only one instance at the same time.

Stop replication by running `CALL mysql.rds_reset_external_master;` on the new instance.

Now, you may delete the old instance.
