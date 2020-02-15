package mysql
import "net"
import "fmt"
import "reflect"
import "time"
import "io"

type MysqlStream struct{
	Stream
	conn net.Conn
}
func NewMysqlStream(conn net.Conn) *MysqlStream{
	this := &MysqlStream{}
	this.Stream.initStream()
	this.conn = conn
	go func(){
		// 从readChannel读server发来的数据
		for{
			select{
				case n, ok := <-this.controlChannel:
				if !ok {
					// 被stream关闭，表示主动终止
					return
				}
				// 读网络
				buf := make([]byte, n)
				conn.SetReadDeadline(time.Now().Add(time.Second * 3))
				//n, err := conn.Read(buf)
				intn, err := io.ReadFull(conn, buf)
				n = int64(intn) // 如果int是32位的，在读入>4G数据时会有问题
				// 写给stream
				bs := byteStream{}
				bs.bytes =buf
				bs.n = n
				bs.err = err
				// Dial.Read如果服务器主动关闭，返回的err=EOF
				// Dial.Read如果超时没有数据，返回的err=read tcp 10.4.12.79:50920->10.21.200.75:3306: i/o timeout
				this.readChannel <- bs
				if err != nil{
					close(this.readChannel)
					this.readChannel = nil
				}
				
				case bytes, ok :=<-this.writeChannel:
				if ok{
					// 如果写入通道关闭，表示只从mysql读
					conn.SetWriteDeadline(time.Now().Add(time.Second * 3))
					n, err := conn.Write(bytes)
					wr := writeResult{}
					wr.n = n
					wr.err = err
					this.writeResultChannel <- wr
					
				}
			}
		}
	}()
	return this	
}

func (this *MysqlStream)readInTimeout() ([]byte, int, error){
	deadline := time.Now().Add(time.Second * 120)
	this.conn.SetReadDeadline(deadline)
	this.conn.SetWriteDeadline(deadline)
	bytes, len, err := this.readNBytes(1024)
	return bytes, len, err
}

// 服务器相关的属性
type TableMapsType map[Uint8] TableMapEventType
type ColumnSignedType            map[string] bool
type TableColumnSignedType       map[string] ColumnSignedType
type SchemaTableColumnSignedType map[string] TableColumnSignedType

// schema => table => set columns => set values
type ColumnSetTypeValuesType            map[string] []string
type TableColumnSetTypeValuesType       map[string] ColumnSetTypeValuesType
type SchemaTableColumnSetTypeValuesType map[string] TableColumnSetTypeValuesType

// schema => table => column names
type ColumnNames           []string
type TableColumnName       map[string] ColumnNames
type SchemaTableColumnName map[string] TableColumnName

// schema => table => column types
type ColumnTypes            []string
type TableColumnTypes       map[string] ColumnTypes
type SchemaTableColumnTypes map[string] TableColumnTypes
type ServerConfigType struct{
	CapabilityFlags       Uint4  // 服务器的特征标志位。一般是服务器决定的(32位)
	StatusFlags           Uint2  // 服务器传来的状态
	BinlogVersion         int
	EventTypeHeaderLength []byte // event头部长度
	TableMaps             TableMapsType// 各个表的结构
	Unsigned              SchemaTableColumnSignedType  // 数值型字段是有符号(false)还是无符号(true)，Schema => TableIcd => Column => true/false
	ColumnSetTypeValues   SchemaTableColumnSetTypeValuesType // 如果某列的类型是SET，后面是值的列表，Schema => TableIcd => Column => []string
	ColumnNames           SchemaTableColumnName  // 库=>表=>列名列表
	ColumnTypes           SchemaTableColumnTypes // 库=>表=>列名
}
func NewServerConfig() *ServerConfigType{
	ret := &ServerConfigType{}
	ret.TableMaps           = make(TableMapsType)
	ret.Unsigned            = make(SchemaTableColumnSignedType)
	ret.ColumnSetTypeValues = make(SchemaTableColumnSetTypeValuesType)
	ret.ColumnNames         = make(SchemaTableColumnName)
	ret.ColumnTypes         = make(SchemaTableColumnTypes)
	return ret
}

// MySql命令状态机：
const (
	UNCONNECTED = iota // 未连接到服务器
	CONNECTED    // TCP连接完成
	HANDSHAKED   // 握手完成
	TABLE_DUMPED // DUMP表完成
	// 开始
)
type MysqlServer struct{
	host string
	port int
	user string
	pass string
	stream *MysqlStream
	state int
	authenticationMethod AuthenticationMethodType
	serverConfig *ServerConfigType
}

type MysqlErrorCodeType int
const (
	NOT_CONNECTED MysqlErrorCodeType = iota + 1
	CONNECTING_FAILED
	MYSQL_ERROR         // Mysql返回了ErrPacket，导致后续无法进行
	NOT_EXPECTED_PACKET // 返回了一个无法识别的回返包，可能是mysql新版不支持等原因
)
type MysqlError struct{
	Code   MysqlErrorCodeType
	server *MysqlServer
	err    error
}
func (this MysqlError) Error() string{
	var ret string
	switch this.Code{
		case NOT_CONNECTED:
		ret = "Not Connected"
		case CONNECTING_FAILED:
		ret = fmt.Sprintf("Connecting failed for %s", this.err.Error())
		case MYSQL_ERROR:
		ret = fmt.Sprintf("Mysql returned an error %s", this.err.Error())		
		default:
		ret = fmt.Sprintf("MysqlError:%v", this.Code)
	}
	ret = fmt.Sprintf("Server=%s:%d %s", this.server.host, this.server.port, ret)
	return ret
}

func NewMysqlServer() *MysqlServer{
	this := &MysqlServer{}
	this.host = ""
	this.port = 3306
	this.user = "repl_user"
	this.pass = "123456"
	this.state = UNCONNECTED
	this.serverConfig = NewServerConfig()
	return this
}
func Test(){
	// S->C  4e 00 00 00 0a 35 2e 35 2e 36 32 2d 6c 6f 67 00  47 00 00 00 32 47 52 43 3c 41 73 3e 00 ff f7 08    N....5.5.62-log. G...2GRC<As>.... 
	//       02 00 0f 80 15 00 00 00 00 00 00 00 00 00 00 7a  6d 35 5d 58 7e 34 28 66 4d 2c 36 00 6d 79 73 71    ...............z m5]X~4(fM,6.mysq 
	//       6c 5f 6e 61 74 69 76 65 5f 70 61 73 73 77 6f 72  64 00                                              l_native_passwor d.               
	// 32 47 52 43 3c 41 73 3e
	// flags = low=ff f7 high=0f 80, CLIENT_PLUGIN_AUTH=1, Length of auth-plugin-data=0x15, CLIENT_SECURE_CONNECTION=1,
	// auth-plugin-data-part-1 = 32 47 52 43 3c 41 73 3e
	// len of auth-plugin-data-part-2 = max(13, 0x15-8) = 13, auth-plugin-data-part-2 = 7a  6d 35 5d 58 7e 34 28 66 4d 2c 36 00
	// C->S  55 00 00 01 85 a6 7f 00 00 00 00 01 21 00 00 00  00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00    U...........!... ................ 
	//       00 00 00 00 74 65 73 74 5f 75 73 65 72 00 14 b1  dc 18 f5 10 f1 c9 9f 79 8f 3e 46 34 74 40 91 bd    ....test_user... .......y.>F4t@.. 
	//       a3 b4 37 6d 79 73 71 6c 5f 6e 61 74 69 76 65 5f  70 61 73 73 77 6f 72 64 00                         ..7mysql_native_ password.
	// flags = 85 a6 7f 00, LENENC_CLIENT_DATA = 1, length of auth-response=0x14, auth response=b1  dc 18 f5 10 f1 c9 9f 79 8f 3e 46 34 74 40 91 bd a3 b4 37
	buf := []byte{0x32, 0x47, 0x52, 0x43, 0x3c, 0x41, 0x73, 0x3e, 0x7a, 0x6d, 0x35, 0x5d, 0x58, 0x7e, 0x34, 0x28, 0x66, 0x4d, 0x2c, 0x36}
	key := string(buf)
	fmt.Println("key=", key, " len=", len(key))
	r := nativeAuth("123456", key)
	x := []byte(r)
	for t := range(x){
		fmt.Println(fmt.Sprintf("%2x", x[t]))
	}
}
func (this *MysqlServer)Connect() error{
	// 设置超时时间等
	this.host = "192.168.1.10"
	this.port = 3306
	fmt.Println(fmt.Sprintf("%v:%v", this.host, this.port))
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", this.host, this.port))
	if err != nil{
		return MysqlError{CONNECTING_FAILED, this, err}
	}
	this.stream = NewMysqlStream(conn)
	this.state = CONNECTED
	this.stream.serverConfig = this.serverConfig
	fmt.Println("state 111=", this.state)
	return nil
}
func (this *MysqlServer)Run() error{
	if this.state != CONNECTED{
		return MysqlError{NOT_CONNECTED, this, nil}
	}
	err := this.handshake(this.user, this.pass, "")
	if err != nil{
		return err
	}
	err = this.replicate()
	return err
	/*
	for{
		packet, err := this.stream.Read()
		this.printPacket(packet)
		if err != nil {
			return err
		}
	}
	return nil*/
}
func (this *MysqlServer)replicate() error{
	// 把自己注册成一个Slave
	registerCom := NewComRegisterSlave()
	writeResultRet := this.stream.WriteCom(registerCom)
	if writeResultRet.err != nil{
		return writeResultRet.err
	}
	reisterComResponse, err := this.stream.Read()
	if err != nil{
		return err
	}
	if errPacket, ok := reisterComResponse.(ErrPacket); ok{
		return this.errorByErrPacket(errPacket)
	}
	if _, ok := reisterComResponse.(OKPacket); !ok{
		return this.errorNotExpectedPacket(reisterComResponse)
	}

	// DUMP表
	// Mysql 5.5.62不支持，会报ErrPacket {Type:ErrPacket, Header:255, ErrorCode:1047, SqlStateMarker:, SqlState:, ErrorMessage:#08S01Unknown command,
	//dumpTableCom := NewComTableDump()
	//writeResultRet = this.stream.WriteCom(dumpTableCom)
	//if writeResultRet.err != nil{
	//return writeResultRet.err
	//}

	dumpBinlogCom := NewComBinlogDump()
	writeResultRet = this.stream.WriteCom(dumpBinlogCom)
	if writeResultRet.err != nil{
		return writeResultRet.err
	}

	for{
		pkt, err := this.stream.ReadEvent()
		fmt.Println(pkt)
		fmt.Println(err)
		fmt.Println("~~~~~~~~~~")
		if err == nil{
			if formatDescriptionEvent, ok := pkt.(FormatDescriptionEventType); ok{
				this.serverConfig.EventTypeHeaderLength = formatDescriptionEvent.EventTypeHeaderLength
			}else if tableMapEvent, ok := pkt.(TableMapEventType); ok{
				this.serverConfig.TableMaps[Uint8(tableMapEvent.TableId)] = tableMapEvent
			}else if rowsEvent, ok := pkt.(RowsEventType); ok{
				if _, ok := this.serverConfig.TableMaps[Uint8(rowsEvent.TableId)]; ok{
					// 这里解析成我们需要的数据
				}else{
					// 这个表的结构未知
				}
			}else if queryEvent, ok := pkt.(QueryEventType); ok{
				//type ColumnSignedType            map[string] bool
				//type TableColumnSignedType       map[Uint8] ColumnSignedType
				//type SchemaTableColumnSignedType map[string] TableColumnSignedType
				//type ColumnSetTypeValuesType            map[string] []string
				//type TableColumnSetTypeValuesType       map[Uint8] ColumnSetTypeValuesType
				//type SchemaTableColumnSetTypeValuesType map[string] TableColumnSetTypeValuesType
				//Signed                SchemaTableColumnSignedType  // 数值型字段是有符号(true)还是无符号(false)，Schema => TableIcd => Column => true/false
				//ColumnSetTypeValues   SchemaTableColumnSetTypeValuesType // 如果某列的类型是SET，后面是值的列表，Schema => TableIcd => Column => []string

				fmt.Println("queryEvent=", queryEvent)
				// queryEvent.Schema 有时是“”，空字符串。怀疑是mysql的bug(5.5.62)。当这里是空字符串时，用下面sql解析出来的schema
				schema := string(queryEvent.Schema)

				x := string(queryEvent.Query)
				fmt.Println("len(queryEvent)=", len(x))
				fmt.Println("queryEvent=", x)
				var tableAsts []*Table
				tableAsts, err = parseSql(x)
				if err != nil{
					// sql 解析失败，有可能是一些无法识别的类型导致的。比如 ruiaylin/sqlparser 不支持geometry类型。这种情况只能先跳过了
					fmt.Println("parseSql err=", err)
				}
				fmt.Println("tableAsts=", tableAsts)
				for _, tableAst := range tableAsts{
					if schema == ""{
						schema = tableAst.Schema
					}else if schema != tableAst.Schema && schema == "" && tableAst.Schema != "" {
						// binlog中的schema与解析出来的不相符？
						panic("binlog schema != tableAstSchema, binlog schema=" + schema + " tableAst.schema=" + tableAst.Schema )
					}
					fmt.Println(tableAst.Schema, tableAst.Name)

					if _, ok := this.serverConfig.ColumnSetTypeValues[schema]; !ok{
						this.serverConfig.ColumnSetTypeValues[schema] = make(TableColumnSetTypeValuesType)
					}
					if _, ok := this.serverConfig.Unsigned[schema]; !ok{
						this.serverConfig.Unsigned[schema] = make(TableColumnSignedType)
					}
					if _, ok := this.serverConfig.ColumnNames[schema]; !ok{
						this.serverConfig.ColumnNames[schema] = make(TableColumnName)
					}
					if _, ok := this.serverConfig.ColumnTypes[schema]; !ok{
						this.serverConfig.ColumnTypes[schema] = make(TableColumnTypes)
					}

					if _, ok := this.serverConfig.ColumnSetTypeValues[schema][string(tableAst.Name)]; !ok{
						this.serverConfig.ColumnSetTypeValues[schema][string(tableAst.Name)] = make(ColumnSetTypeValuesType)
					}
					if _, ok := this.serverConfig.Unsigned[schema][string(tableAst.Name)]; !ok{
						this.serverConfig.Unsigned[schema][string(tableAst.Name)] = make(ColumnSignedType)
					}
					if _, ok := this.serverConfig.ColumnNames[schema][string(tableAst.Name)]; !ok{
						this.serverConfig.ColumnNames[schema][string(tableAst.Name)] = make(ColumnNames, 0)
					}
					if _, ok := this.serverConfig.ColumnTypes[schema][string(tableAst.Name)]; !ok{
						this.serverConfig.ColumnTypes[schema][string(tableAst.Name)] = make(ColumnTypes, 0)
					}

					for _, col := range tableAst.Cols{
						fmt.Println("Schema=", string(queryEvent.Schema), " table=", string(tableAst.Name), " table2=", tableAst.Schema)
						fmt.Println("col.Name=", col.Name)
						fmt.Println("col.ColumnType=", col.ColumnType)
						fmt.Println("col.Unsigned=", col.Unsigned)
						fmt.Println("col.SetParams=", col.SetParams)
						this.serverConfig.ColumnNames[schema][string(tableAst.Name)] = append(this.serverConfig.ColumnNames[schema][string(tableAst.Name)], col.Name)
						fmt.Println("cols=", this.serverConfig.ColumnNames[schema][string(tableAst.Name)])
						this.serverConfig.ColumnTypes[schema][string(tableAst.Name)] = append(this.serverConfig.ColumnTypes[schema][string(tableAst.Name)], col.ColumnType)
						if col.ColumnType == "set" || col.ColumnType == "enum" {
							if _, ok := this.serverConfig.ColumnSetTypeValues[schema][string(tableAst.Name)][col.Name]; !ok{
								this.serverConfig.ColumnSetTypeValues[schema][string(tableAst.Name)][col.Name] = col.SetParams
								fmt.Println("ColumnSetTypeValues=", schema, string(tableAst.Name), col.Name, this.serverConfig.ColumnSetTypeValues[schema][string(tableAst.Name)][col.Name])
							}
						}else if _, ok := numberColumnTypes[col.ColumnType]; ok{
							if _, ok := this.serverConfig.Unsigned[schema][string(tableAst.Name)][col.Name]; !ok{
								this.serverConfig.Unsigned[schema][string(tableAst.Name)][col.Name] = col.Unsigned
								fmt.Println("Unsigned=", schema, string(tableAst.Name), col.Name, this.serverConfig.Unsigned[schema][string(tableAst.Name)][col.Name])
							}
						}
					}
					fmt.Println("----- TABLE END -----")
				}
			}
			//if _, ok := pkt.(RowsEventType); ok{
			//	break;
			//}
		}else{
			fmt.Println(err)
			break;
		}
	}

	bytes, len, err := this.stream.readInTimeout()
	fmt.Println("_______________")
	fmt.Println(bytes)
	fmt.Println(len)
	fmt.Println(err)
	
	this.state = TABLE_DUMPED
	return err
}
func (this *MysqlServer)errorByErrPacket(errPacket ErrPacket) MysqlError{
	err := Error{fmt.Sprintf("ErrPacket Code=%d, Msg=%v", errPacket.ErrorCode, errPacket.ErrorMessage), 0}
	return MysqlError{MYSQL_ERROR, this, err}
}
func (this *MysqlServer)errorNotExpectedPacket(packet interface{}) MysqlError{
	err := Error{fmt.Sprintf("Unexpteced Packet, Type=%v, %v", reflect.TypeOf(packet), packet), 0}
	return MysqlError{NOT_EXPECTED_PACKET, this, err}
}
func (this *MysqlServer)errorMustUse41(packet interface{}) MysqlError{
	err := Error{fmt.Sprintf("CapabilityFlag_CLIENT_PROTOCOL_41 unsetted, %v", packet), 0}
	return MysqlError{NOT_EXPECTED_PACKET, this, err}
}
func (this *MysqlServer)errorUnknownAuthenticationMethodByAuthSwitchRequest(authMethod string) MysqlError{
	err := Error{fmt.Sprintf("errorUnknownAuthenticationMethodByAuthSwitchRequest method= %v", authMethod), 0}
	return MysqlError{NOT_EXPECTED_PACKET, this, err}
}
func (this *MysqlServer)errorUnknownAuthenticationMethod(handshakePacket HandshakeV10) MysqlError{
	err := Error{fmt.Sprintf("Unknown AuthenticationMethod, CLIENT_PROTOCOL_41=%v, CLIENT_SECURE_CONNECTION=%v, CLIENT_PLUGIN_AUTH=%v", CapabilityFlag_CLIENT_PROTOCOL_41.isSet(handshakePacket.CapabilityFlags), CapabilityFlag_CLIENT_SECURE_CONNECTION.isSet(handshakePacket.CapabilityFlags), CapabilityFlag_CLIENT_PLUGIN_AUTH.isSet(handshakePacket.CapabilityFlags)), 0}
	return MysqlError{NOT_EXPECTED_PACKET, this, err}
}
func (this *MysqlServer)handshake(username string, password string, database string) error{
	// Connected -+--------> HandshakePacket -+----> SSLExchange ------> ClientResponse ------> AuthenticationMethodSwitch ------> Disconnect
	//            |                           |                           ↑        |                     ↓
	//            | 连接满等                   +---------------------------+        +----------> AuthenticationExchangeContinuation ------> OK
	//            +--------> ErrPacket                                             |                     |
	//                                                                             +---------------------+-------------------------------> Err
	packet, err := this.stream.Read()
	this.printPacket(packet)
	if err != nil {
		return err
	}
	if errPacket, ok := packet.(ErrPacket); ok{
		return this.errorByErrPacket(errPacket)
	}
	if handshakePacket, ok := packet.(HandshakeV10); !ok{
		return this.errorNotExpectedPacket(packet)
	}else{
		// 只支持Client_4.1
		if Uint4(handshakePacket.CapabilityFlags) & Uint4(CapabilityFlag_CLIENT_PROTOCOL_41) == 0{
			return this.errorMustUse41(packet)
		}

		// 判断Authentication Method
		if !CapabilityFlag_CLIENT_PLUGIN_AUTH.isSet(handshakePacket.CapabilityFlags) {
			if CapabilityFlag_CLIENT_PROTOCOL_41.isSet(handshakePacket.CapabilityFlags) && CapabilityFlag_CLIENT_SECURE_CONNECTION.isSet(handshakePacket.CapabilityFlags){
				this.authenticationMethod = OldPasswordAuthentication
			}else if CapabilityFlag_CLIENT_PROTOCOL_41.isSet(handshakePacket.CapabilityFlags) && CapabilityFlag_CLIENT_SECURE_CONNECTION.isSet(handshakePacket.CapabilityFlags) {
				this.authenticationMethod = SecurePasswordAuthentication
			}else{
				return this.errorUnknownAuthenticationMethod(handshakePacket)
			}
		}else{
			this.authenticationMethod = AuthenticationMethodType(handshakePacket.AuthPluginName)
		}
		handshakeResponse := NewHandshakeResponse41(&handshakePacket, username, password, database, this.authenticationMethod)
		this.printPacket(handshakeResponse)
		writeResultRet := this.stream.Write(handshakeResponse)
		if writeResultRet.err != nil{
			return writeResultRet.err
		}

		packet, err = this.stream.Read()
		this.printPacket(packet)
		if errPacket, ok := packet.(ErrPacket); ok{
			return this.errorByErrPacket(errPacket)
		}
		
		// 缺少OKPacket
		// 切换认证方法（前面是OK或Err就不需要走这里）
		_, ok1 := packet.(OldAuthSwitchRequest)
		authSwitchRequest, ok2 := packet.(AuthSwitchRequest)
		if !ok1 && !ok2{
			return this.errorNotExpectedPacket(packet)
		}

		authSwitchResponse := NewAuthSwitchResponse()
		var pluginData string
		if ok1 {
			this.authenticationMethod = OldPasswordAuthentication
			pluginData = string(handshakePacket.AuthPluginDataPart1)
		}else if ok2{
			this.authenticationMethod = AuthenticationMethodType(authSwitchRequest.PluginName)
			pluginData = string(authSwitchRequest.AuthPluginData)
		}

		if f, ok := authMethod[this.authenticationMethod]; ok && f != nil{
			//fmt.Println("pass=", this.pass, " pluginData=", pluginData)
			authPluginResponseStr := f(this.pass, pluginData)
			authSwitchResponse.AuthPluginResponse = StringEof(authPluginResponseStr)
		}else{
			return this.errorUnknownAuthenticationMethodByAuthSwitchRequest(string(this.authenticationMethod))
		}
		this.printPacket(authSwitchResponse)
		writeResultRet = this.stream.Write(authSwitchResponse)
		if writeResultRet.err != nil{
			return writeResultRet.err
		}

		// 跳过SSL。这里在不支持
		//fmt.Println(handshakePacket)
		packet, err = this.stream.Read()
		this.printPacket(packet)
		if err != nil {
			return err
		}
		if errPacket, ok := packet.(ErrPacket); ok{
			return this.errorByErrPacket(errPacket)
		}
		// OK Packet
		// 其它Packet
	}
	this.state = HANDSHAKED
	return nil
}
func (this *MysqlServer)printPacket(p interface{}){
	if str, ok := p.(fmt.Stringer); ok{
		fmt.Println(str)
	}else{
		fmt.Println(p)
	}
}
