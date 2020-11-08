# What is go-mysql-lib?
go-mysql-lib implements mysql binary protocol. It could listen to a mysql master serever, and keep track all changes by binlog.

# How?
1. Grant replication privilege 
> GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicant'@'...' IDENTIFIED BY 'my_pwd';
2. Set your binlog format to ROW
> SET GLOBAL binlog_format = 'ROW';
3. Code:
func main() {
	config := mysql.Config{}
	config.Host = "localhost"
	config.Port = "33061"
	config.User = "repl_user"
	config.Pass = "123456"
	config.ServerId = 2
	config.DumpFrom = mysql.DumpFromBeginning
	config.Continue = true
	config.LogTag = mysql.LogTCPStream | mysql.LogPacket | mysql.LogWarning

	sto := Sto{}
	l := L{}
	server := mysql.NewMysqlServer(config, sto, l)
	err := server.Connect()

	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("server=", server)
	err = server.Open()
	fmt.Println("Open=", err)
	cb := Callback{}
	err = server.Replicate(cb)
	fmt.Println("Replication End=", err)
}

# TODO
