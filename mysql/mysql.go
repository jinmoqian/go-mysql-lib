package mysql

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type MysqlStream struct {
	Stream
	conn net.Conn
}

func NewMysqlStream(conn net.Conn) *MysqlStream {
	this := &MysqlStream{}
	this.Stream.initStream()
	this.conn = conn
	go func() {
		// 从readChannel读server发来的数据
		for {
			select {
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
				bs.bytes = buf
				bs.n = n
				bs.err = err
				// Dial.Read如果服务器主动关闭，返回的err=EOF
				// Dial.Read如果超时没有数据，返回的err=read tcp 10.4.12.79:50920->10.21.200.75:3306: i/o timeout
				this.readChannel <- bs
				if err != nil {
					close(this.readChannel)
					this.readChannel = nil
				}

			case bytes, ok := <-this.writeChannel:
				if ok {
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

func (this *MysqlStream) readInTimeout() ([]byte, int, error) {
	deadline := time.Now().Add(time.Second * 120)
	this.conn.SetReadDeadline(deadline)
	this.conn.SetWriteDeadline(deadline)
	bytes, len, err := this.readNBytes(1024)
	return bytes, len, err
}

type ColumnAttr struct {
	Name          string   // 列名
	Type          string   // 列类型
	SetTypeValues []string // 如果某列的类型是SET，后面是值的列表
	Signed        bool     // 数值型字段是有符号(false)还是无符号(true)
}

func NewColumnAttr(col *TableColumn) ColumnAttr {
	columnAttr := ColumnAttr{}
	columnAttr.Name = col.Name
	columnAttr.Type = col.ColumnType
	if col.ColumnType == "set" || col.ColumnType == "enum" {
		columnAttr.SetTypeValues = col.SetParams
	}
	columnAttr.Signed = col.Unsigned
	return columnAttr
}

type TableAttr []ColumnAttr
type TableAttrs map[string]TableAttr  // 表名=>表属性
type SchemaAttr map[string]TableAttrs // 数据库名=>各表属性
type TableMapsType map[Uint8]TableMapEventType

type ServerConfigType struct {
	Version               string
	CapabilityFlags       Uint4         // 服务器的特征标志位。一般是服务器决定的(32位)
	StatusFlags           Uint2         // 服务器传来的状态
	BinlogVersion         int           // binlog版本，一般=4
	EventTypeHeaderLength []byte        // event头部长度
	TableMaps             TableMapsType // 各个表的结构
	Columns               SchemaAttr    // 库名=>表名=>列名=>属性
	ServerCrc32CheckFlag  bool          // CRC校验标记。从mysql 5.6.0开始支持这个功能。之后为true，之前为false
	CrcSize               int           // CRC用到的长度
	BinlogFilename        string        // 当前读到的binlog文件名
	BinlogPosition        uint32        // 正要读的下一个binlog的位置
}

func NewServerConfig() *ServerConfigType {
	ret := &ServerConfigType{}
	ret.TableMaps = make(TableMapsType)
	ret.Columns = make(SchemaAttr)
	ret.CrcSize = 0
	return ret
}
func (this *ServerConfigType) getColumn(schema, table, column string) *ColumnAttr {
	if schemaAttr, ok := this.Columns[schema]; ok {
		if tableAttr, ok := schemaAttr[table]; ok {
			for _, columnAttr := range tableAttr {
				if columnAttr.Name == column {
					return &columnAttr
				}
			}
		}
	}
	return nil
}
func (this *ServerConfigType) getColumnAt(schema, table string, idx int) *ColumnAttr {
	if schemaAttr, ok := this.Columns[schema]; ok {
		if tableAttr, ok := schemaAttr[table]; ok {
			if idx < len(tableAttr) {
				return &tableAttr[idx]
			}
		}
	}
	return nil
}
func (this *ServerConfigType) compareVersion(ver string) int {
	serverVerionSubstrings := strings.Split(this.Version, ".")
	ver2Substrings := strings.Split(ver, ".")

	serverVerionInts := make([]int64, 0)
	ver2Ints := make([]int64, 0)
	for _, b := range serverVerionSubstrings {
		b = strings.TrimFunc(b, reserveNumFunc)
		i, _ := strconv.ParseInt(b, 0, 64)
		serverVerionInts = append(serverVerionInts, i)
	}
	for _, b := range ver2Substrings {
		b = strings.TrimFunc(b, reserveNumFunc)
		i, _ := strconv.ParseInt(b, 0, 64)
		ver2Ints = append(ver2Ints, i)
	}

	var minLen int
	if len(serverVerionSubstrings) < len(ver2Substrings) {
		minLen = len(serverVerionSubstrings)
	} else {
		minLen = len(ver2Substrings)
	}
	for i := 0; i < minLen; i++ {
		if serverVerionInts[i] < ver2Ints[i] {
			return -1
		} else if serverVerionInts[i] > ver2Ints[i] {
			return +1
		}
	}
	return 0
}

// MySql命令状态机：
const (
	UNCONNECTED = iota // 未连接到服务器
	CONNECTED          // TCP连接完成
	HANDSHAKED         // 握手完成
)

type MysqlServer struct {
	config               Config
	storage              Storage
	log                  Log
	stream               *MysqlStream
	state                int
	authenticationMethod AuthenticationMethodType
	serverConfig         *ServerConfigType
}

type MysqlErrorCodeType int

const (
	NOT_CONNECTED MysqlErrorCodeType = iota + 1
	CONNECTING_FAILED
	NOT_HANDSHAKED
	MYSQL_ERROR         // Mysql返回了ErrPacket，导致后续无法进行
	NOT_EXPECTED_PACKET // 返回了一个意料之外的回返包，可能是mysql新版不支持等原因
)

type MysqlError struct {
	Code   MysqlErrorCodeType
	server *MysqlServer
	err    error
}

func (this MysqlError) Error() string {
	var ret string
	switch this.Code {
	case NOT_CONNECTED:
		ret = "Not Connected"
	case CONNECTING_FAILED:
		ret = fmt.Sprintf("Connecting failed for %s", this.err.Error())
	case MYSQL_ERROR:
		ret = fmt.Sprintf("Mysql returned an error %s", this.err.Error())
	default:
		ret = fmt.Sprintf("MysqlError:%v", this.Code)
	}
	ret = fmt.Sprintf("Server=%s:%d %s", this.server.config.Host, this.server.config.Port, ret)
	return ret
}

func NewMysqlServer(config Config, storage Storage, log Log) *MysqlServer {
	this := &MysqlServer{}
	this.config = config
	this.storage = storage
	this.log = log
	this.state = UNCONNECTED
	this.serverConfig = NewServerConfig()
	return this
}

// func Test() {
// 	// S->C  4e 00 00 00 0a 35 2e 35 2e 36 32 2d 6c 6f 67 00  47 00 00 00 32 47 52 43 3c 41 73 3e 00 ff f7 08    N....5.5.62-log. G...2GRC<As>....
// 	//       02 00 0f 80 15 00 00 00 00 00 00 00 00 00 00 7a  6d 35 5d 58 7e 34 28 66 4d 2c 36 00 6d 79 73 71    ...............z m5]X~4(fM,6.mysq
// 	//       6c 5f 6e 61 74 69 76 65 5f 70 61 73 73 77 6f 72  64 00                                              l_native_passwor d.
// 	// 32 47 52 43 3c 41 73 3e
// 	// flags = low=ff f7 high=0f 80, CLIENT_PLUGIN_AUTH=1, Length of auth-plugin-data=0x15, CLIENT_SECURE_CONNECTION=1,
// 	// auth-plugin-data-part-1 = 32 47 52 43 3c 41 73 3e
// 	// len of auth-plugin-data-part-2 = max(13, 0x15-8) = 13, auth-plugin-data-part-2 = 7a  6d 35 5d 58 7e 34 28 66 4d 2c 36 00
// 	// C->S  55 00 00 01 85 a6 7f 00 00 00 00 01 21 00 00 00  00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00    U...........!... ................
// 	//       00 00 00 00 74 65 73 74 5f 75 73 65 72 00 14 b1  dc 18 f5 10 f1 c9 9f 79 8f 3e 46 34 74 40 91 bd    ....test_user... .......y.>F4t@..
// 	//       a3 b4 37 6d 79 73 71 6c 5f 6e 61 74 69 76 65 5f  70 61 73 73 77 6f 72 64 00                         ..7mysql_native_ password.
// 	// flags = 85 a6 7f 00, LENENC_CLIENT_DATA = 1, length of auth-response=0x14, auth response=b1  dc 18 f5 10 f1 c9 9f 79 8f 3e 46 34 74 40 91 bd a3 b4 37
// 	buf := []byte{0x32, 0x47, 0x52, 0x43, 0x3c, 0x41, 0x73, 0x3e, 0x7a, 0x6d, 0x35, 0x5d, 0x58, 0x7e, 0x34, 0x28, 0x66, 0x4d, 0x2c, 0x36}
// 	key := string(buf)
// 	fmt.Println("key=", key, " len=", len(key))
// 	r := nativeAuth("123456", key)
// 	x := []byte(r)
// 	for t := range x {
// 		fmt.Println(fmt.Sprintf("%2x", x[t]))
// 	}
// }

func (this *MysqlServer) Log(tag uint32, content string) {
	if tag&this.config.LogTag != 0 {
		this.log.Log(tag, content)
	}
}
func (this *MysqlServer) LogError(err error) error {
	this.Log(LogError, err.Error())
	return err
}
func (this *MysqlServer) Connect() error {
	// 设置超时时间等
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", this.config.Host, this.config.Port))
	if err != nil {
		return MysqlError{CONNECTING_FAILED, this, err}
	}
	this.stream = NewMysqlStream(conn)
	this.stream.log = this.log
	this.state = CONNECTED
	this.stream.serverConfig = this.serverConfig
	return nil
}

type RowHistory struct {
	Before []ColumnValueType
	After  []ColumnValueType
}

func (this RowHistory) String() string {
	buf := bytes.NewBufferString("{Type:RowHistory, Before:[")
	for _, v := range this.Before {
		buf.WriteString(v.String())
		buf.WriteString(",")
	}
	buf.WriteString("], After:[")
	for _, v := range this.After {
		buf.WriteString(v.String())
		buf.WriteString(",")
	}
	buf.WriteString("]}")
	return buf.String()
}

type DataHistory struct {
	// 发生的时间
	Schema string
	Table  string
	Rows   []RowHistory
}

func (this DataHistory) String() string {
	buf := bytes.NewBufferString("{Type:DataHistory, Rows:[")
	for _, v := range this.Rows {
		buf.WriteString(v.String())
		buf.WriteString(",")
	}
	buf.WriteString("]}")
	return buf.String()
}
func NewDataHistory(columnValue RowsEventType, tableMap *TableMapEventType) DataHistory {
	ret := DataHistory{}
	if tableMap != nil {
		ret.Schema = string(tableMap.SchemaName)
		ret.Table = string(tableMap.TableName)
	}
	for _, row := range columnValue.Rows {
		rowHistory := RowHistory{}
		rowHistory.Before = row.Value1
		rowHistory.After = row.Value2
		ret.Rows = append(ret.Rows, rowHistory)
	}
	return ret
}

type CallbackInterface interface {
	// 执行SQL
	OnQuery(sql string)
	// 插入、更新、删除
	OnInsert(row DataHistory)
	OnUpdate(row DataHistory)
	OnDelete(row DataHistory)
	// 当无事件时调用。如果返回true表示结束。如果返回false会继续等新的事件
	OnEnd() bool
	// 当缺少表结构时调用
	OnColumnAttr(schema, table string, colIdx int)
	// OnError()
}

func (this *MysqlServer) Open() error {
	if this.state != CONNECTED {
		return MysqlError{NOT_CONNECTED, this, nil}
	}
	fmt.Println("user=", this.config.User, " pass=", this.config.Pass)
	err := this.handshake(this.config.User, this.config.Pass, "")
	if err != nil {
		return err
	}
	this.state = HANDSHAKED
	return nil
}
func (this *MysqlServer) Close() error {
	return nil
}
func (this *MysqlServer) Replicate(callback CallbackInterface) error {
	if this.state != HANDSHAKED {
		return MysqlError{NOT_HANDSHAKED, this, nil}
	}
	if this.serverConfig.compareVersion("5.6.2") >= 0 {
		// 要导出binlog，需要先关闭binlog checksum
		com := NewComQuery("SET @master_binlog_checksum='NONE'")
		writeResultRet := this.stream.WriteCom(com)
		if writeResultRet.err != nil {
			return writeResultRet.err
		}
		comResponse, err := this.stream.Read()
		if err != nil {
			return err
		}
		if _, ok := comResponse.(OKPacket); ok {
		} else if errPacket, ok := comResponse.(ErrPacket); ok {
			return this.errorByErrPacket(errPacket)
		} else {
			return this.errorNotExpectedPacket(comResponse)
		}
	}

	// 把自己注册成一个Slave
	registerCom := NewComRegisterSlave()
	writeResultRet := this.stream.WriteCom(registerCom)
	if writeResultRet.err != nil {
		return writeResultRet.err
	}
	reisterComResponse, err := this.stream.Read()
	if err != nil {
		return err
	}
	if errPacket, ok := reisterComResponse.(ErrPacket); ok {
		return this.errorByErrPacket(errPacket)
	}
	if _, ok := reisterComResponse.(OKPacket); !ok {
		return this.errorNotExpectedPacket(reisterComResponse)
	}

	// DUMP表
	// Mysql 5.5.62不支持，会报ErrPacket {Type:ErrPacket, Header:255, ErrorCode:1047, SqlStateMarker:, SqlState:, ErrorMessage:#08S01Unknown command,
	//dumpTableCom := NewComTableDump()
	//writeResultRet = this.stream.WriteCom(dumpTableCom)
	//if writeResultRet.err != nil{
	//return writeResultRet.err
	//}

	var filename string
	var binlogPos uint32
	if this.config.Continue {
		if this.storage != nil {
			// 读
			filename = this.serverConfig.BinlogFilename
			binlogPos = this.serverConfig.BinlogPosition
		}
	}
	// var jump uint32
	// 如果读不到，或者continue为false
	if filename == "" || !this.config.Continue {
		switch this.config.DumpFrom {
		case DumpFromBeginning:
			filename = "mysql-bin.000001"
			binlogPos = 4
		case DumpFromLatest:
			// TODO: 再连接，show master status，取得当前位置
			// filename = "xxx"
			// binlogPos = xxx
			fallthrough
		case DumpFromPosition:
			// 后两种情况特殊，不能从指定的位置开始。因为binglog第一条是format_description_event
			// 如果不读这个记录，后面会报错。所以只能从第1条开始读，然后前面的跳过
			filename = this.config.BinlogPosition.Filename
			binlogPos = this.config.BinlogPosition.BinlogPos
		}
		// 这里后两种需要判断一下，如果当前记下来的format_description_event不是指定文件的，需要从头开始读，再跳过
		if filename != this.serverConfig.BinlogFilename {
			// jump = binlogPos
			binlogPos = 4
		}
	}

	dumpBinlogCom := NewComBinlogDump(this.config.ServerId, filename, binlogPos)

	// 非阻塞
	dumpBinlogCom.Flags = 0x01

	writeResultRet = this.stream.WriteCom(dumpBinlogCom)
	if writeResultRet.err != nil {
		return writeResultRet.err
	}

	for {
		pkt, err := this.stream.ReadEvent()
		if err == nil {
			// TOOD: 当前binlog的位置？与jump比较
			//var eventHeader EventHeaderType
			if fullEvent, ok := pkt.(FullEvent); ok {
				pkt = fullEvent.event
			}
			if _, ok := pkt.(EOFPacket); ok {
				if callback.OnEnd() {
					return nil
				}
			} else if formatDescriptionEvent, ok := pkt.(FormatDescriptionEventType); ok {
				this.serverConfig.EventTypeHeaderLength = formatDescriptionEvent.EventTypeHeaderLength
				// this.serverConfig.BinlogFilename = xxx
				// this.serverConfig.BinlogFilename = xxx
			} else if tableMapEvent, ok := pkt.(TableMapEventType); ok {
				this.serverConfig.TableMaps[Uint8(tableMapEvent.TableId)] = tableMapEvent
			} else if rowsEvent, ok := pkt.(RowsEventType); ok {
				var tableMap *TableMapEventType
				if tm, ok := this.serverConfig.TableMaps[Uint8(rowsEvent.TableId)]; ok {
					tableMap = &tm
				}
				row := NewDataHistory(rowsEvent, tableMap)
				//fmt.Println("ROW=", row)
				if _, ok := this.serverConfig.TableMaps[Uint8(rowsEvent.TableId)]; ok {
					// 这里解析成我们需要的数据，包括列名、正负等
				} else {
					// 这个表的结构未知
				}
				switch rowsEvent.Command {
				case RowsEvenCommandInsert:
					callback.OnInsert(row)
				case RowsEvenCommandUpdate:
					callback.OnUpdate(row)
				case RowsEvenCommandDelete:
					callback.OnDelete(row)
				}
			} else if queryEvent, ok := pkt.(QueryEventType); ok {
				//fmt.Println("queryEvent=", queryEvent)
				schema := string(queryEvent.Schema)
				var tableAsts []*Table
				tableAsts, err = parseSql(string(queryEvent.Query))
				if err != nil {
					// sql 解析失败，有可能是一些无法识别的类型导致的。比如 ruiaylin/sqlparser 不支持geometry类型，也可能是合法的语句如BEGIN等。这种情况只能先跳过了
					//fmt.Println("parseSql err=", err)
				} else {
					//fmt.Println("tableAsts=", tableAsts)
					for _, tableAst := range tableAsts {
						// queryEvent.Schema 有时是“”，空字符串。怀疑是mysql的bug(5.5.62)。当这里是空字符串时，用下面sql解析出来的schema
						// Q_UPDATED_DB_NAMES 表示当前DB？
						if schema == "" {
							schema = tableAst.Schema
						}
						if schema != tableAst.Schema && schema == "" && tableAst.Schema != "" {
							// binlog中的schema与解析出来的不相符？
							panic("binlog schema != tableAstSchema, binlog schema=" + schema + " tableAst.schema=" + tableAst.Schema)
						}

						if _, ok := this.serverConfig.Columns[schema]; !ok {
							this.serverConfig.Columns[schema] = make(TableAttrs)
						}
						if _, ok := this.serverConfig.Columns[schema][string(tableAst.Name)]; !ok {
							this.serverConfig.Columns[schema][string(tableAst.Name)] = make(TableAttr, 0)
						}

						//fmt.Println("Action=", tableAst.Action, " Schema=", tableAst.Schema, " Table=", tableAst.Name)
						for _, col := range tableAst.Cols {
							// fmt.Println("col.Name=", col.Name)
							// fmt.Println("col.ColumnType=", col.ColumnType)
							// fmt.Println("col.Unsigned=", col.Unsigned)
							// fmt.Println("col.SetParams=", col.SetParams)
							// fmt.Println("col.Position=", col.Position)
							// fmt.Println("col.AddAfter=", col.AddAfter)
							// fmt.Println("col.Drop=", col.Drop)

							switch tableAst.Action {
							case ActionCreate:
								columnAttr := NewColumnAttr(col)
								this.serverConfig.Columns[schema][tableAst.Name] = append(this.serverConfig.Columns[schema][tableAst.Name], columnAttr)

							case ActionAlter:
								//fmt.Println("ActionAlter=", queryEvent.Query)
								if col.Drop {
									// 删除一列
									colIdx := -1
									for idx, colFound := range this.serverConfig.Columns[schema][tableAst.Name] {
										if colFound.Name == col.Name {
											colIdx = idx
											break
										}
									}
									if colIdx != -1 {
										l := len(this.serverConfig.Columns[schema][tableAst.Name])
										if colIdx == 0 {
											this.serverConfig.Columns[schema][tableAst.Name] = this.serverConfig.Columns[schema][tableAst.Name][1:]
										} else if colIdx == l-1 {
											this.serverConfig.Columns[schema][tableAst.Name] = this.serverConfig.Columns[schema][tableAst.Name][:l-1]
										} else {
											tmp := this.serverConfig.Columns[schema][tableAst.Name][0:colIdx]
											tmp = append(tmp, this.serverConfig.Columns[schema][tableAst.Name][colIdx+1:]...)
											this.serverConfig.Columns[schema][tableAst.Name] = tmp
										}
									}
								} else {
									// 新增一列
									columnAttr := NewColumnAttr(col)
									if col.Position == AddColumnAtTail {
										this.serverConfig.Columns[schema][tableAst.Name] = append(this.serverConfig.Columns[schema][tableAst.Name], columnAttr)
									} else if col.Position == AddColumnAtFirst {
										tmp := make([]ColumnAttr, 0)
										tmp = append(tmp, columnAttr)
										this.serverConfig.Columns[schema][tableAst.Name] = append(tmp, this.serverConfig.Columns[schema][tableAst.Name]...)
									} else {
									}
								}
							}

						}
						fmt.Println("----- TABLE END -----")
					}
				}
				callback.OnQuery(string(queryEvent.Query))
			}
			//if _, ok := pkt.(RowsEventType); ok{
			//	break;
			//}
			// 在这里处理给用户的回调
		} else {
			fmt.Println(err)
			break
		}
	}
	return err
}
func (this *MysqlServer) errorByErrPacket(errPacket ErrPacket) MysqlError {
	err := Error{fmt.Sprintf("ErrPacket Code=%d, Msg=%v", errPacket.ErrorCode, errPacket.ErrorMessage), 0}
	return MysqlError{MYSQL_ERROR, this, err}
}
func (this *MysqlServer) errorNotExpectedPacket(packet interface{}) MysqlError {
	err := Error{fmt.Sprintf("Unexpteced Packet, Type=%v, %v", reflect.TypeOf(packet), packet), 0}
	return MysqlError{NOT_EXPECTED_PACKET, this, err}
}
func (this *MysqlServer) errorMustUse41(packet interface{}) MysqlError {
	err := Error{fmt.Sprintf("CapabilityFlag_CLIENT_PROTOCOL_41 unsetted, %v", packet), 0}
	return MysqlError{NOT_EXPECTED_PACKET, this, err}
}
func (this *MysqlServer) errorUnknownAuthenticationMethodByAuthSwitchRequest(authMethod string) MysqlError {
	err := Error{fmt.Sprintf("errorUnknownAuthenticationMethodByAuthSwitchRequest method= %v", authMethod), 0}
	return MysqlError{NOT_EXPECTED_PACKET, this, err}
}
func (this *MysqlServer) errorUnknownAuthenticationMethod(handshakePacket HandshakeV10) MysqlError {
	err := Error{fmt.Sprintf("Unknown AuthenticationMethod, CLIENT_PROTOCOL_41=%v, CLIENT_SECURE_CONNECTION=%v, CLIENT_PLUGIN_AUTH=%v", CapabilityFlag_CLIENT_PROTOCOL_41.isSet(handshakePacket.CapabilityFlags), CapabilityFlag_CLIENT_SECURE_CONNECTION.isSet(handshakePacket.CapabilityFlags), CapabilityFlag_CLIENT_PLUGIN_AUTH.isSet(handshakePacket.CapabilityFlags)), 0}
	return MysqlError{NOT_EXPECTED_PACKET, this, err}
}
func reserveNumFunc(c rune) bool {
	var ret bool
	switch c {
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		ret = false
	default:
		ret = true
	}
	return ret
}
func (this *MysqlServer) handshake(username string, password string, database string) error {
	// Connected -+--------> HandshakePacket -+----> SSLExchange ------> ClientResponse ------> AuthenticationMethodSwitch ------> Disconnect
	//            |                           |                           ↑        |                     ↓
	//            | 连接满等                   +---------------------------+        +----------> AuthenticationExchangeContinuation ------> OK
	//            +--------> ErrPacket                                             |                     |
	//                                                                             +---------------------+-------------------------------> Err
	packet, err := this.stream.Read()
	// this.printPacket(packet)
	if err != nil {
		return err
	}
	if errPacket, ok := packet.(ErrPacket); ok {
		return this.errorByErrPacket(errPacket)
	}
	if handshakePacket, ok := packet.(HandshakeV10); !ok {
		return this.errorNotExpectedPacket(packet)
	} else {
		// 只支持Client_4.1
		if Uint4(handshakePacket.CapabilityFlags)&Uint4(CapabilityFlag_CLIENT_PROTOCOL_41) == 0 {
			return this.errorMustUse41(packet)
		}
		this.stream.serverConfig.Version = string(handshakePacket.ServerVersion)
		// 从5.6.0后开始支持CRC32校验
		if this.stream.serverConfig.compareVersion("5.6.0") >= 0 {
			this.serverConfig.ServerCrc32CheckFlag = true
			this.serverConfig.CrcSize = 4
		}

		// 判断Authentication Method
		if !CapabilityFlag_CLIENT_PLUGIN_AUTH.isSet(handshakePacket.CapabilityFlags) {
			if CapabilityFlag_CLIENT_PROTOCOL_41.isSet(handshakePacket.CapabilityFlags) && CapabilityFlag_CLIENT_SECURE_CONNECTION.isSet(handshakePacket.CapabilityFlags) {
				this.authenticationMethod = OldPasswordAuthentication
			} else if CapabilityFlag_CLIENT_PROTOCOL_41.isSet(handshakePacket.CapabilityFlags) && CapabilityFlag_CLIENT_SECURE_CONNECTION.isSet(handshakePacket.CapabilityFlags) {
				this.authenticationMethod = SecurePasswordAuthentication
			} else {
				return this.errorUnknownAuthenticationMethod(handshakePacket)
			}
		} else {
			this.authenticationMethod = AuthenticationMethodType(handshakePacket.AuthPluginName)
		}

		if f, ok := authMethod[this.authenticationMethod]; ok && f != nil {
			pluginData := string(handshakePacket.AuthPluginDataPart1 + handshakePacket.AuthPluginDataPart2)
			authPluginResponseStr := f(this.config.Pass, pluginData)
			password = string(authPluginResponseStr)
		} else {
			return this.errorUnknownAuthenticationMethodByAuthSwitchRequest(string(this.authenticationMethod))
		}
		handshakeResponse := NewHandshakeResponse41(&handshakePacket, username, password, database, this.authenticationMethod)

		// 根据文档 dev.mysql.com/doc/internals/en/connection-phase-packets.html ，从5.6.6之后就可以发送变量
		// 如果不设置这些变量，在replication时会报CRC错误
		if this.serverConfig.compareVersion("5.6.6") >= 0 {
			// C->S
			// bf 00 00 01 05 a2 3e 00 00 00 00 40 08 00 00 00  00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00    ......>....@.... ................
			// 00 00 00 00 72 65 70 6c 5f 75 73 65 72 00 14 b3  bf 99 85 6f 97 df 88 14 29 10 91 e8 65 74 cc 25    ....repl_user... ...o....)...et.%
			// cb 80 8a 6d 79 73 71 6c 5f 6e 61 74 69 76 65 5f  70 61 73 73 77 6f 72 64 00 69 03 5f 6f 73 05 4c    ...mysql_native_ password.i._os.L
			// 69 6e 75 78 0c 5f 63 6c 69 65 6e 74 5f 6e 61 6d  65 08 6c 69 62 6d 79 73 71 6c 04 5f 70 69 64 04    inux._client_nam e.libmysql._pid.
			// 34 31 33 35 0f 5f 63 6c 69 65 6e 74 5f 76 65 72  73 69 6f 6e 06 35 2e 36 2e 34 36 09 5f 70 6c 61    4135._client_ver sion.5.6.46._pla
			// 74 66 6f 72 6d 04 69 36 38 36 0c 70 72 6f 67 72  61 6d 5f 6e 61 6d 65 0b 6d 79 73 71 6c 62 69 6e    tform.i686.progr am_name.mysqlbin
			// 6c 6f 67                                                                                            log
			handshakeResponse.AddKeyValue("_os", "Linux")
			handshakeResponse.AddKeyValue("_client_name", "libmysql")
			handshakeResponse.AddKeyValue("_pid", "4135")
			handshakeResponse.AddKeyValue("_client_version", strings.TrimFunc(this.stream.serverConfig.Version, reserveNumFunc))
			handshakeResponse.AddKeyValue("_platform", "i686")
			handshakeResponse.AddKeyValue("program_name", "mysqlbinlog")
		}
		// this.printPacket(handshakeResponse)
		writeResultRet := this.stream.Write(handshakeResponse)
		if writeResultRet.err != nil {
			return writeResultRet.err
		}

		packet, err = this.stream.Read()
		// this.printPacket(packet)
		if errPacket, ok := packet.(ErrPacket); ok {
			return this.errorByErrPacket(errPacket)
		} else if _, ok := packet.(OKPacket); ok {
			goto EXIT
		}

		// 缺少OKPacket
		// 切换认证方法（前面是OK或Err就不需要走这里）
		_, ok1 := packet.(OldAuthSwitchRequest)
		authSwitchRequest, ok2 := packet.(AuthSwitchRequest)
		if !ok1 && !ok2 {
			return this.errorNotExpectedPacket(packet)
		}

		authSwitchResponse := NewAuthSwitchResponse()
		var pluginData string
		if ok1 {
			this.authenticationMethod = OldPasswordAuthentication
			pluginData = string(handshakePacket.AuthPluginDataPart1)
		} else if ok2 {
			this.authenticationMethod = AuthenticationMethodType(authSwitchRequest.PluginName)
			pluginData = string(authSwitchRequest.AuthPluginData)
		}

		if f, ok := authMethod[this.authenticationMethod]; ok && f != nil {
			authPluginResponseStr := f(this.config.Pass, pluginData)
			authSwitchResponse.AuthPluginResponse = StringEof(authPluginResponseStr)
		} else {
			return this.errorUnknownAuthenticationMethodByAuthSwitchRequest(string(this.authenticationMethod))
		}
		// this.printPacket(authSwitchResponse)
		writeResultRet = this.stream.Write(authSwitchResponse)
		if writeResultRet.err != nil {
			return writeResultRet.err
		}

		// 跳过SSL。这里在不支持
		packet, err = this.stream.Read()
		// this.printPacket(packet)
		if err != nil {
			return err
		}
		if errPacket, ok := packet.(ErrPacket); ok {
			return this.errorByErrPacket(errPacket)
		}
		// OK Packet
		// 其它Packet
	}
EXIT:
	this.state = HANDSHAKED
	return nil
}

// func (this *MysqlServer) printPacket(p interface{}) {
// 	if str, ok := p.(fmt.Stringer); ok {
// 		fmt.Println(str)
// 	} else {
// 		fmt.Println(p)
// 	}
// }
