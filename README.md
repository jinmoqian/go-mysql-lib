# What is go-mysql-lib?
go-mysql-lib implements mysql binary protocol. It could listen to a mysql master serever, and keep track all changes by binlog.

# How?
1. Grant replication privilege 
> GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicant'@'...' IDENTIFIED BY 'my_pwd';
2. Set your binlog format to ROW
> SET GLOBAL binlog_format = 'ROW';
3. Code:
```
func main() {
 	config := mysql.Config{}
 	config.Host = "localhost"
 	config.Port = "33061"
 	config.User = "repl_user"
 	config.Pass = "my_pwd"
 	config.ServerId = 2
 	config.DumpFrom = mysql.DumpFromBeginning
 	config.Continue = true
 	config.LogTag = mysql.LogTCPStream | mysql.LogPacket | mysql.LogWarning
 
 	sto := Sto{}
 	l := L{}
 	server := mysql.NewMysqlServer(config, sto, l)
 	server.Connect()
 	server.Open()
 	fmt.Println("Open=", err)
 	cb := Callback{}
 	server.Replicate(cb)
 }
 ```

# TODO
1. Add support to Mysql8
2. Support signed/unsigned number
3. Big binary data support
4. Effection improvement
