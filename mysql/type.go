package mysql
import "fmt"
import "bytes"
import "sort"
import "time"

// 把Mysql的数据类型转换成字节流
type Decoder interface{
	Decode() []byte
}
// 把定长的字节流转化成对应的Mysql的数据类型
type FixLengthEncoder interface{
	Encode([]byte)
}

// 各个字节位运算的掩码
const (
	byte0 = 0xFF <<(iota*8)
	byte1
	byte2
	byte3
	byte4
	byte5
	byte6
	byte7
)
// 字节数组转成uint64
func byte2uint64(b []byte, maxLen int) uint64{
	n := len(b)
	masks := [8]uint64{byte0, byte1, byte2, byte3, byte4, byte5, byte6, byte7}
	if maxLen > len(masks){
		maxLen = len(masks)
	}
	if maxLen > n{
		maxLen = n
	}
	var ret uint64 = uint64(b[0])
	for i := 0; i < maxLen; i++{
		ret |= ((uint64(b[i]) << (uint(i)*8)) & masks[i])
	}
	return ret
}
// uint64转成字节
func uint642bytebuf(n uint64, maxLen int, buf []byte){
	masks := [8]uint64{byte0, byte1, byte2, byte3, byte4, byte5, byte6, byte7}
	for i := 0; i < maxLen; i++{
		buf[i] = byte((n & masks[i]) >> (uint(i)*8))
	}
}
func uint642byte(n uint64, maxLen int) []byte{
	ret := make([]byte, maxLen)
	uint642bytebuf(n, maxLen, ret)
	return ret
}

const (
	Uint1Length = 1
	Uint2Length = 2
	Uint3Length = 3
	Uint4Length = 4
	Uint6Length = 6
	Uint8Length = 8
	UintLenencLength = 9 // 最长9个字节
)
type Uint1 uint
func (this *Uint1) Encode(b []byte){
	*this = Uint1(b[0])
}
func (this *Uint1) Decode() []byte{
	return uint642byte(uint64(*this), Uint1Length)
}

type Uint2 uint
func (this *Uint2) Encode(b []byte){
	*this = Uint2(byte2uint64(b, Uint2Length))
}
func (this *Uint2) Decode() []byte{
	return uint642byte(uint64(*this), Uint2Length)
}

type Uint3 uint
func (this *Uint3) Encode(b []byte){
	*this = Uint3(byte2uint64(b, Uint3Length))
}
func (this *Uint3) Decode() []byte{
	return uint642byte(uint64(*this), Uint3Length)
}

type Uint4 uint
func (this *Uint4) Encode(b []byte){
	*this = Uint4(byte2uint64(b, Uint4Length))
}
func (this *Uint4) Decode() []byte{
	return uint642byte(uint64(*this), Uint4Length)
}

type Uint6 uint64
func (this *Uint6) Encode(b []byte){
	*this = Uint6(byte2uint64(b, Uint6Length))
}
func (this *Uint6) Decode() []byte{
	return uint642byte(uint64(*this), Uint6Length)
}

type Uint8 uint64
func (this *Uint8) Encode(b []byte){
	*this = Uint8(byte2uint64(b, Uint8Length))
}
func (this *Uint8) Decode() []byte{
	return uint642byte(uint64(*this), Uint8Length)
}

type UintLenenc uint64
func (this UintLenenc) Decode() []byte{
	var l int
	var ret []byte
	var head byte
	if this < 251 {
		l = 1
		head = byte(this)
	}else if this < (1<<16) {
		l = 1 + 2
		head = 0xFC
	}else if this < (1<<24) {
		l = 1 + 3
		head = 0xFD
	}else{
		l = 1 + 8
		head = 0xFE
	}
	ret = make([]byte, l)
	ret[0] = head
	if l > 1{
		uint642bytebuf(uint64(this), l-1, ret[1:])
	}
	return ret
}

type StringFix string
func (this *StringFix) Decode() []byte{
	return []byte(*this)
}
type StringNul string
func (this *StringNul) Decode() []byte{
	return append([]byte(*this), 0x00)
}
type StringLenenc string
func (this *StringLenenc) Decode() []byte{
	return append(UintLenenc(len(*this)).Decode(), []byte(*this)...)
}
type StringEof string
func (this *StringEof) Decode() []byte{
	return []byte(*this)
}

type OKPacket struct{
	Header Uint1
	AffectedRows UintLenenc
	LastInsertId UintLenenc
	StatusFlags  Uint2
	Warnings     Uint2
	Info         StringLenenc
	SessionStateChanges StringLenenc
}
func (this OKPacket) String() string{
	return fmt.Sprintf("{Type:OKPacket, Header:%v, AffectedRows:%v, LastInsertId:%v, StatusFlags:%v, Warnings:%v, Info:%v, SessionStateChanges:%v}", this.Header, this.AffectedRows, this.LastInsertId, this.StatusFlags, this.Warnings, this.Info, this.SessionStateChanges)
}

type ErrPacket struct{
	Header Uint1
	ErrorCode Uint2
	SqlStateMarker StringFix
	SqlState StringFix
	ErrorMessage StringEof
}
func (this ErrPacket) String() string{
	return fmt.Sprintf("{Type:ErrPacket, Header:%v, ErrorCode:%v, SqlStateMarker:%v, SqlState:%v, ErrorMessage:%v,", this.Header, this.ErrorCode, this.SqlStateMarker, this.SqlState, this.ErrorMessage)
}
type EOFPacket struct{
}
func (this EOFPacket) String() string{
	return "EOFPacket"
}
type CapalibilityFlagType int
const (
	CapabilityFlag_CLIENT_LONG_PASSWORD                  CapalibilityFlagType = 0x00000001 // Use the improved version of Old Password Authentication.Assumed to be set since 4.1.1.
	CapabilityFlag_CLIENT_FOUND_ROWS                     CapalibilityFlagType = 0x00000002 // Send found rows instead of affected rows in EOF_Packet.
	CapabilityFlag_CLIENT_LONG_FLAG                      CapalibilityFlagType = 0x00000004 // Longer flags in Protocol::ColumnDefinition320.Server:Supports longer flags. Client:Expects longer flags.
	CapabilityFlag_CLIENT_CONNECT_WITH_DB                CapalibilityFlagType = 0x00000008 // Database (schema) name can be specified on connect in Handshake Response Packet.
	CapabilityFlag_CLIENT_NO_SCHEMA                      CapalibilityFlagType = 0x00000010 // Do not permit database.table.column.
	CapabilityFlag_CLIENT_COMPRESS                       CapalibilityFlagType = 0x00000020 // Compression protocol supported.
	CapabilityFlag_CLIENT_ODBC                           CapalibilityFlagType = 0x00000040 // Special handling of ODBC behavior.No special behavior since 3.22.
	CapabilityFlag_CLIENT_LOCAL_FILES                    CapalibilityFlagType = 0x00000080 // Can use LOAD DATA LOCAL.
	CapabilityFlag_CLIENT_IGNORE_SPACE                   CapalibilityFlagType = 0x00000100 // Server:Parser can ignore spaces before '('. Client:Let the parser ignore spaces before '('.
	CapabilityFlag_CLIENT_PROTOCOL_41                    CapalibilityFlagType = 0x00000200 // Server:Supports the 4.1 protocol. Client:Uses the 4.1 protocol. Note:this value was CLIENT_CHANGE_USER in 3.22, unused in 4.0
	CapabilityFlag_CLIENT_INTERACTIVE                    CapalibilityFlagType = 0x00000400 // Server:Supports interactive and noninteractive clients. Client:Client is interactive.
	CapabilityFlag_CLIENT_SSL                            CapalibilityFlagType = 0x00000800 // Server:Supports SSL. Client:Switch to SSL after sending the capability-flags.
	CapabilityFlag_CLIENT_IGNORE_SIGPIPE                 CapalibilityFlagType = 0x00001000 // Client:Do not issue SIGPIPE if network failures occur (libmysqlclient only).
	CapabilityFlag_CLIENT_TRANSACTIONS                   CapalibilityFlagType = 0x00002000 // Server:Can send status flags in EOF_Packet. Client:Expects status flags in EOF_Packet.
	CapabilityFlag_CLIENT_RESERVED                       CapalibilityFlagType = 0x00004000 // Unused.
	CapabilityFlag_CLIENT_SECURE_CONNECTION              CapalibilityFlagType = 0x00008000 // Server:Supports Authentication::Native41.Client: Supports Authentication::Native41.
	CapabilityFlag_CLIENT_MULTI_STATEMENTS               CapalibilityFlagType = 0x00010000 // Server:Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE. Client:May send multiple statements per COM_QUERY and COM_STMT_PREPARE.
	CapabilityFlag_CLIENT_MULTI_RESULTS                  CapalibilityFlagType = 0x00020000 // Server:Can send multiple resultsets for COM_QUERY. Client:Can handle multiple resultsets for COM_QUERY.
	CapabilityFlag_CLIENT_PS_MULTI_RESULTS               CapalibilityFlagType = 0x00040000 // multiple resultsets for COM_STMT_EXECUTE.
	CapabilityFlag_CLIENT_PLUGIN_AUTH                    CapalibilityFlagType = 0x00080000 // Server:Sends extra data in Initial Handshake Packet and supports the pluggable authentication protocol.Client:Supports authentication plugins.
	CapabilityFlag_CLIENT_CONNECT_ATTRS                  CapalibilityFlagType = 0x00100000 // connection attributes in Protocol::HandshakeResponse41.
	CapabilityFlag_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA CapalibilityFlagType = 0x00200000 // ength-encoded integer for auth response data in Protocol::HandshakeResponse41.
	CapabilityFlag_CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   CapalibilityFlagType = 0x00400000 // support for expired password extension.
	CapabilityFlag_CLIENT_SESSION_TRACK                  CapalibilityFlagType = 0x00800000 // Server:Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and send session-state change data after a OK packet.Client:Expects the server to send sesson-state changes after a OK packet.
	CapabilityFlag_CLIENT_DEPRECATE_EOF                  CapalibilityFlagType = 0x01000000 // Server:Can send OK after a Text Resultset. Client:Expects an OK (instead of EOF) after the resultset rows of a Text Resultset.
)
func (this CapalibilityFlagType)isSet(flags Uint4) bool{
	return Uint4(this) & flags != 0
}
var capabilityFlagDesc map[CapalibilityFlagType] string
func init(){
	capabilityFlagDesc = make(map[CapalibilityFlagType] string, 25)
	capabilityFlagDesc[CapabilityFlag_CLIENT_LONG_PASSWORD] = "CLIENT_LONG_PASSWORD"
	capabilityFlagDesc[CapabilityFlag_CLIENT_FOUND_ROWS] = "CLIENT_FOUND_ROWS"
	capabilityFlagDesc[CapabilityFlag_CLIENT_LONG_FLAG] = "CLIENT_LONG_FLAG"
	capabilityFlagDesc[CapabilityFlag_CLIENT_CONNECT_WITH_DB] = "CLIENT_CONNECT_WITH_DB"
	capabilityFlagDesc[CapabilityFlag_CLIENT_NO_SCHEMA] = "CLIENT_NO_SCHEMA"
	capabilityFlagDesc[CapabilityFlag_CLIENT_COMPRESS] = "CLIENT_COMPRESS"
	capabilityFlagDesc[CapabilityFlag_CLIENT_ODBC] = "CLIENT_ODBC"
	capabilityFlagDesc[CapabilityFlag_CLIENT_LOCAL_FILES] = "CLIENT_LOCAL_FILES"
	capabilityFlagDesc[CapabilityFlag_CLIENT_IGNORE_SPACE] = "CLIENT_IGNORE_SPACE"
	capabilityFlagDesc[CapabilityFlag_CLIENT_PROTOCOL_41] = "CLIENT_PROTOCOL_41"
	capabilityFlagDesc[CapabilityFlag_CLIENT_INTERACTIVE] = "CLIENT_INTERACTIVEagType"
	capabilityFlagDesc[CapabilityFlag_CLIENT_SSL] = "CLIENT_SSL"
	capabilityFlagDesc[CapabilityFlag_CLIENT_IGNORE_SIGPIPE] = "CLIENT_IGNORE_SIGPIPE"
	capabilityFlagDesc[CapabilityFlag_CLIENT_TRANSACTIONS] = "CLIENT_TRANSACTIONS"
	capabilityFlagDesc[CapabilityFlag_CLIENT_RESERVED] = "CLIENT_RESERVED"
	capabilityFlagDesc[CapabilityFlag_CLIENT_SECURE_CONNECTION] = "CLIENT_SECURE_CONNECTION"
	capabilityFlagDesc[CapabilityFlag_CLIENT_MULTI_STATEMENTS] = "CLIENT_MULTI_STATEMENTS"
	capabilityFlagDesc[CapabilityFlag_CLIENT_MULTI_RESULTS] = "CLIENT_MULTI_RESULTS"
	capabilityFlagDesc[CapabilityFlag_CLIENT_PS_MULTI_RESULTS] = "CLIENT_PS_MULTI_RESULTS"
	capabilityFlagDesc[CapabilityFlag_CLIENT_PLUGIN_AUTH] = "CLIENT_PLUGIN_AUTH"
	capabilityFlagDesc[CapabilityFlag_CLIENT_CONNECT_ATTRS] = "CLIENT_CONNECT_ATTRS"
	capabilityFlagDesc[CapabilityFlag_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA] = "CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA"
	capabilityFlagDesc[CapabilityFlag_CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS] = "CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS"
	capabilityFlagDesc[CapabilityFlag_CLIENT_SESSION_TRACK] = "CLIENT_SESSION_TRACK"
	capabilityFlagDesc[CapabilityFlag_CLIENT_DEPRECATE_EOF] = "CLIENT_DEPRECATE_EOF"
}

type ServerStatusType Uint2
const(
	SERVER_STATUS_IN_TRANS             ServerStatusType = 0x0001 //a transaction is active
	SERVER_STATUS_AUTOCOMMIT           ServerStatusType = 0x0002 //auto-commit is enabled
	SERVER_MORE_RESULTS_EXISTS         ServerStatusType = 0x0008	 
	SERVER_STATUS_NO_GOOD_INDEX_USED   ServerStatusType = 0x0010	 
	SERVER_STATUS_NO_INDEX_USED        ServerStatusType = 0x0020	 
	SERVER_STATUS_CURSOR_EXISTS        ServerStatusType = 0x0040 //Used by Binary Protocol Resultset to signal that COM_STMT_FETCH must be used to fetch the row-data.
	SERVER_STATUS_LAST_ROW_SENT        ServerStatusType = 0x0080	 
	SERVER_STATUS_DB_DROPPED           ServerStatusType = 0x0100	 
	SERVER_STATUS_NO_BACKSLASH_ESCAPES ServerStatusType = 0x0200	 
	SERVER_STATUS_METADATA_CHANGED     ServerStatusType = 0x0400	 
	SERVER_QUERY_WAS_SLOW              ServerStatusType = 0x0800	 
	SERVER_PS_OUT_PARAMS               ServerStatusType = 0x1000	 
	SERVER_STATUS_IN_TRANS_READONLY    ServerStatusType = 0x2000 //in a read-only transaction
	SERVER_SESSION_STATE_CHANGED       ServerStatusType = 0x4000// connection state information has changed
)

const (
	Utf8mb4 = 255
)
func capabilityFlagsString(buf *bytes.Buffer, capabilityFlags Uint4){
	keys := make([]int, 0)
	for k := range(capabilityFlagDesc){
		keys = append(keys, int(k))
	}
	ks := sort.IntSlice(keys)
	ks.Sort()
	for i, k := range(keys){
		v := int(capabilityFlags) & int(k)
		var x int
		if v != 0{
			x = 1
		}else{
			x = 0
		}
		buf.WriteString(fmt.Sprintf("%s=%v, ", capabilityFlagDesc[CapalibilityFlagType(k)], x))
		if i != 0 && i % 4 == 0{
			buf.WriteString("\n");
		}
	}
}
type HandshakeV10 struct{
	Version Uint1
	ServerVersion StringNul
	ConnectionId Uint4
	AuthPluginDataPart1 StringFix
	Filler Uint1
	CapabilityFlagsLowerBytes Uint2
	CharacterSet Uint1
	StatusFlags Uint2
	CapabilityFlagsUpperBytes Uint2
	LengthOfAuthPluginData Uint1
	Reserved StringFix
	AuthPluginDataPart2 StringFix
	AuthPluginName StringNul

	CapabilityFlags Uint4
}
func (this HandshakeV10) String() string{
	buf := bytes.NewBufferString(fmt.Sprintf("{Type:HandshakeV10, Version:%v, ServerVersion:%v, ConnectionId:%v, AuthPluginDataPart1:%v, AuthPluginDataPart2:%v, CapabilityFlags:%v(%v|%v), CharacterSet:%v, StatusFlags:%v, LengthOfAuthPluginData:%v, AuthPluginName:%v\n", this.Version, this.ServerVersion, this.ConnectionId, this.AuthPluginDataPart1, this.AuthPluginDataPart2, this.CapabilityFlags, this.CapabilityFlagsUpperBytes, this.CapabilityFlagsLowerBytes, this.CharacterSet, this.StatusFlags, this.LengthOfAuthPluginData, this.AuthPluginName))
	capabilityFlagsString(buf, this.CapabilityFlags)
	buf.WriteString("}")
	return buf.String()
}
// 在 3.21.0 之前才用V9和Response320，所以就不做了
//type HandshakeV9 struct{
//}
// type HandshakeResponse320 struct{
//}
type AuthenticationMethodType string
const (
	OldPasswordAuthentication    AuthenticationMethodType = "mysql_old_password"
	SecurePasswordAuthentication AuthenticationMethodType = "mysql_native_password"
	ClearPasswordAuthentication  AuthenticationMethodType = "mysql_clear_password"
)
type AuthMethodType func(string, string)string
var authMethod map[AuthenticationMethodType] AuthMethodType
func init(){
	authMethod = make(map[AuthenticationMethodType] AuthMethodType, 3)
	authMethod[OldPasswordAuthentication] = oldAuth
	authMethod[SecurePasswordAuthentication] = nativeAuth
	authMethod[ClearPasswordAuthentication] = clearAuth
}
type HandshakeResponse41 struct{
	CapabilityFlags Uint4
	MaxPacketSize   Uint4
	CharacterSet    Uint1
	Username        StringNul
	AuthResponse    string // 这个字段在不同flag下会有不同格式
	Database        StringNul
	AuthPluginName  StringNul
	LengthOfAllKeyValues UintLenenc  // 字节长度
	KeyValues       map[string] string
	authenticationMethod AuthenticationMethodType
}
func (this HandshakeResponse41)String() string{
	buf := bytes.NewBufferString(fmt.Sprintf("{Type:HandshakeResponse41, CapabilityFlags=%v, MaxPacketSize=%v, CharacterSet=%v, Username=%v, AuthResponse=%v, Database=%v, AuthPluginName=%v, LengthOfAllKeyValues=%v, authenticationMethod=%v\n", this.CapabilityFlags, this.MaxPacketSize, this.CharacterSet, this.Username, this.AuthResponse, this.Database, this.AuthPluginName, this.LengthOfAllKeyValues, this.authenticationMethod))
	capabilityFlagsString(buf, this.CapabilityFlags)
	buf.WriteString("}")
	return buf.String()
}
func (this *HandshakeResponse41)clrFlag(flag Uint4){
	this.CapabilityFlags = this.CapabilityFlags & (^flag)
}
func (this *HandshakeResponse41)setFlag(flag Uint4){
	this.CapabilityFlags = Uint4(this.CapabilityFlags) | flag
}
func (this *HandshakeResponse41)tstFlag(flag Uint4) bool{
	return (this.CapabilityFlags & flag) != 0
}
func NewHandshakeResponse41(handshake *HandshakeV10, username string, password string, database string, authenticationMethod AuthenticationMethodType) *HandshakeResponse41{
	ret := &HandshakeResponse41{}
	// PROTOCOL_41强制支持；这里不支持SSL，后面再加上；PLUGIN_AUTH强制支持
	ret.CapabilityFlags = handshake.CapabilityFlags
	ret.setFlag(Uint4(CapabilityFlag_CLIENT_PROTOCOL_41))
	ret.clrFlag(Uint4(CapabilityFlag_CLIENT_SSL))
	ret.clrFlag(Uint4(CapabilityFlag_CLIENT_COMPRESS))
	ret.setFlag(Uint4(CapabilityFlag_CLIENT_PLUGIN_AUTH))
	ret.clrFlag(Uint4(CapabilityFlag_CLIENT_CONNECT_WITH_DB))

	ret.MaxPacketSize = 1 * 1024 * 1024
	ret.CharacterSet = Utf8mb4
	ret.Username = StringNul(username)
	ret.authenticationMethod = SecurePasswordAuthentication// ClearPasswordAuthentication// authenticationMethod

	switch(ret.authenticationMethod){
		case OldPasswordAuthentication:
		ret.AuthResponse = password

		case SecurePasswordAuthentication:
		ret.AuthResponse = password

		case ClearPasswordAuthentication:
		ret.AuthResponse = password
	}
	ret.Database = StringNul(database)
	ret.AuthPluginName = StringNul(ret.authenticationMethod)

	// key-values
	return ret
}
func(this *HandshakeResponse41)AddKeyValue(key, value string){
	if this.KeyValues == nil {
		this.KeyValues = make(map[string] string)
	}
	this.KeyValues[key] = value
}
func(this *HandshakeResponse41)Decode() []byte{
	ret := make([]byte, 0)
	ret = append(ret, this.CapabilityFlags.Decode()...)
	ret = append(ret, this.MaxPacketSize.Decode()...)
	ret = append(ret, this.CharacterSet.Decode()...)
	ret = append(ret, make([]byte, 23)...)
	ret = append(ret, this.Username.Decode()...)
	if CapabilityFlag_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.isSet(this.CapabilityFlags){
		l := UintLenenc(len(this.AuthResponse))
		ret = append(ret, l.Decode()...)
		r := StringFix(this.AuthResponse)
		ret = append(ret, r.Decode()...)		
	} else if CapabilityFlag_CLIENT_SECURE_CONNECTION.isSet(this.CapabilityFlags) {
		l := UintLenenc(1)
		ret = append(ret, l.Decode()...)
		r := StringFix(this.AuthResponse)
		ret = append(ret, r.Decode()...)
	} else {
		r := StringNul(this.AuthResponse)
		ret = append(ret, r.Decode()...)
	}

	if CapabilityFlag_CLIENT_CONNECT_WITH_DB.isSet(this.CapabilityFlags){
		r := StringNul(this.Database)
		ret = append(ret, r.Decode()...)
	}
	if CapabilityFlag_CLIENT_PLUGIN_AUTH.isSet(this.CapabilityFlags){
		r := StringNul(this.AuthPluginName)
		ret = append(ret, r.Decode()...)
	}
	if CapabilityFlag_CLIENT_CONNECT_ATTRS.isSet(this.CapabilityFlags){
		fmt.Println("Output HandshakeResponse41 KeyValues:{")
		buf := make([]byte, 0)
		for key, val := range this.KeyValues {
			fmt.Println(key, ",", val, ",")
			k := StringLenenc(key)
			buf = append(buf, k.Decode()...)
			v := StringLenenc(val)
			buf = append(buf, v.Decode()...)
		}
		fmt.Println("}")
		ret = append(ret, UintLenenc(len(buf)).Decode()...)
		ret = append(ret, buf...)
	}
	return ret
}

type AuthMoreData struct{
	Version    Uint1 // 0x01
	PluginData StringEof
}
func NewAuthMoreData() *AuthMoreData{
	ret := &AuthMoreData{}
	return ret
}
func(this *AuthMoreData)Decode() []byte{
	buf := bytes.NewBuffer(this.Version.Decode())
	if _, err := buf.Write(this.PluginData.Decode()); err != nil{
		return nil
	}
	return buf.Bytes()
}
type AuthSwitchRequest struct{
	Version        Uint1        // 0xFE
	PluginName     StringNul
	AuthPluginData StringEof
}
func NewAuthSwitchRequest() *AuthSwitchRequest{
	ret := &AuthSwitchRequest{}
	return ret
}
func (this AuthSwitchRequest)String() string{
	return fmt.Sprintf("{Type:AuthSwitchRequest, Version:%v, PluginName:%v, AuthPluginData:%v}", this.Version, string(this.PluginName), string(this.AuthPluginData))
}
type OldAuthSwitchRequest struct{
	Version Uint1 // 0xFE
}
func (this OldAuthSwitchRequest)String() string{
	return fmt.Sprintf("{Type:OldAuthSwitchRequest, Version:%v}", this.Version)
}
func NewOldAuthSwitchRequest() *OldAuthSwitchRequest{
	ret := &OldAuthSwitchRequest{}
	return ret
}
type AuthSwitchResponse struct{
	AuthPluginResponse StringEof
}
func NewAuthSwitchResponse() *AuthSwitchResponse{
	ret := &AuthSwitchResponse{}
	return ret
}
func(this *AuthSwitchResponse)Decode() []byte{
	return this.AuthPluginResponse.Decode()
}
func (this AuthSwitchResponse)String() string{
	buf := bytes.NewBufferString(fmt.Sprintf("{Type:AuthSwitchResponse, AuthPluginResponse:["))
	for i := 0; i < len(this.AuthPluginResponse); i++ {
		buf.WriteString(fmt.Sprintf("%2x ", this.AuthPluginResponse[i]))
	}
	buf.WriteString("]}")
	return buf.String()
}

type ComSleep struct{
	Com Uint1 // 0x00
}
type ComQuit struct{
	Com Uint1 // 0x01
}
type ComInitDb struct{
	Com Uint1 // 0x02
	SchemaName StringEof
}
type ComQueryType struct{
	Com Uint1 // 0x03
	Query StringEof
}
func NewComQuery(com string) *ComQueryType{
	ret := &ComQueryType{}
	ret.Com = 0x03
	ret.Query = StringEof(com)
	return ret
}
func(this *ComQueryType)Decode() []byte{
	ret := this.Com.Decode()
	ret = append(ret, this.Query.Decode()...)
	return ret
}
func (this ComQueryType) String() string{
	return "{Type:" + string(this.Query) + "}"
}

// TODO: 暂时先跳过
type ComQueryResponse struct{
}
// deprecated since 5.7.11
//type ComFieldList struct{
//}
type ComCreateDb struct{
	Com Uint1 // 0x05
	SchemaName StringEof
}
type ComDropDb struct{
	Com Uint1 // 0x06
	SchemaName StringEof
}
type ComShutdown struct{
	Com Uint1 // 0x08
	ShutdownType Uint1
}
type ComStatistics struct{
	Com Uint1 // 0x09
}
type ComProcessInfo struct{
	Com Uint1 // 0x0a
}
type ComPing struct{
	Com Uint1 // 0x0e
}
func NewComPing() *ComPing{
	ret := &ComPing{}
	ret.Com = 0x0e
	return ret
}
func(this *ComPing)Decode() []byte{
	return this.Com.Decode()
}
type ComBinlogDump struct{
	Com            Uint1 // 0x12
	BinlogPos      Uint4
	Flags          Uint2
	ServerId       Uint4
	BinlogFilename StringEof
}
func NewComBinlogDump() *ComBinlogDump{
	ret := &ComBinlogDump{}
	ret.Com = 0x12
	ret.BinlogPos = 4
	ret.Flags = 0
	ret.ServerId = 2
	ret.BinlogFilename = StringEof("mysql-bin.000001")
	return ret
}
func(this *ComBinlogDump)Decode() []byte{
	ret := this.Com.Decode()
	ret = append(ret, this.BinlogPos.Decode()...)
	ret = append(ret, this.Flags.Decode()...)
	ret = append(ret, this.ServerId.Decode()...)
	ret = append(ret, this.BinlogFilename.Decode()...)
	return ret;
}
func (this ComBinlogDump) String() string{
	return fmt.Sprintf("{Type:ComBinlogDump, Com:%v, BinlogPos:%v, Flags:%v, ServerId:%v, BinlogFilename:%v}", this.Com, this.BinlogPos, this.Flags, this.ServerId, this.BinlogFilename)
}

type ComBinlogDumpGTID struct{
	Com               Uint1 // 0x1e
	Flags             Uint2
	ServerId          Uint4
	BinlogFilenameLen Uint4
	BinlogFilename    StringFix
	BinlogPos         Uint8
	DataSize          Uint4     // BINLOG_THROUGH_GTID
	Data              StringFix // BINLOG_THROUGH_GTID
}
type ComTableDump struct{
	Com          Uint1 // 0x13
	DatabaseLen  Uint1
	DatabaseName StringFix
	TableLen     Uint1
	TableName    StringFix
}
func NewComTableDump() *ComTableDump{
	ret := &ComTableDump{}
	ret.Com = 0x13
	ret.DatabaseName = StringFix("test")
	ret.TableName = StringFix("aaa")
	ret.DatabaseLen = Uint1(len(ret.DatabaseName))
	ret.TableLen = Uint1(len(ret.TableName))
	return ret
}
func(this *ComTableDump)Decode() []byte{
	ret := this.Com.Decode()
	ret = append(ret, this.DatabaseLen.Decode()...)
	ret = append(ret, this.DatabaseName.Decode()...)
	ret = append(ret, this.TableLen.Decode()...)
	ret = append(ret, this.TableName.Decode()...)
	return ret
}

type ComRegisterSlave struct{
	Comv                 Uint1 // 0x15
	ServerId             Uint4
	SlavesHostnameLength Uint1
	SlavesHostname       StringFix
	SlavesUserLen        Uint1
	SlaveUser            StringFix
	SlavesPasswordLen    Uint1
	SlavesPassword       StringFix
	SlavesMysqlPort      Uint2
	ReplicationRank      Uint4
	MasterId             Uint4
}
func NewComRegisterSlave() *ComRegisterSlave{
	ret := &ComRegisterSlave{}
	ret.Comv = 0x15
	ret.ServerId = 2
	ret.SlavesHostnameLength = 0
	ret.SlavesHostname = ""
	ret.SlavesUserLen = 0
	ret.SlaveUser = ""
	ret.SlavesPasswordLen = 0
	ret.SlavesPassword = ""
	ret.SlavesMysqlPort = 0
	ret.ReplicationRank = 0
	ret.MasterId = 0
	return ret
}
func(this *ComRegisterSlave)Decode() []byte{
	ret := this.Comv.Decode()
	ret = append(ret, this.ServerId.Decode()...)
	ret = append(ret, this.SlavesHostnameLength.Decode()...)
	ret = append(ret, this.SlavesHostname.Decode()...)
	ret = append(ret, this.SlavesUserLen.Decode()...)
	ret = append(ret, this.SlaveUser.Decode()...)
	ret = append(ret, this.SlavesPasswordLen.Decode()...)
	ret = append(ret, this.SlavesPassword.Decode()...)
	ret = append(ret, this.SlavesMysqlPort.Decode()...)
	ret = append(ret, this.ReplicationRank.Decode()...)
	ret = append(ret, this.MasterId.Decode()...)
	return ret
}
func (this ComRegisterSlave) String() string{
	return fmt.Sprintf("{Type:ComRegisterSlave, Comv:%v, ServerId:%v, SlavesHostnameLength:%v, SlavesHostname:%v, SlavesUserLen:%v, SlaveUser:%v, SlavesPasswordLen:%v, SlavesPassword:%v, SlavesMysqlPort:%v, ReplicationRank:%v, MasterId:%v}", this.Comv, this.ServerId, this.SlavesHostnameLength, this.SlavesHostname, this.SlavesUserLen, this.SlaveUser, this.SlavesPasswordLen, this.SlavesPassword, this.SlavesMysqlPort, this.ReplicationRank, this.MasterId)
}

type ColumnType Uint1
const(
	ColumnTypeDecimal    ColumnType = 0x00
	ColumnTypeTiny       ColumnType = 0x01
	ColumnTypeShort      ColumnType = 0x02
	ColumnTypeLong       ColumnType = 0x03
	ColumnTypeFloat      ColumnType = 0x04
	ColumnTypeDouble     ColumnType = 0x05
	ColumnTypeNull       ColumnType = 0x06
	ColumnTypeTimestamp  ColumnType = 0x07
	ColumnTypeLonglong   ColumnType = 0x08
	ColumnTypeInt24      ColumnType = 0x09
	ColumnTypeDate       ColumnType = 0x0a
	ColumnTypeTime       ColumnType = 0x0b
	ColumnTypeDatetime   ColumnType = 0x0c
	ColumnTypeYear       ColumnType = 0x0d
	ColumnTypeNewDate    ColumnType = 0x0e
	ColumnTypeVarchar    ColumnType = 0x0f
	ColumnTypeBit        ColumnType = 0x10
	ColumnTypeTimestamp2 ColumnType = 0x11
	ColumnTypeDatetime2  ColumnType = 0x12
	ColumnTypeTime2      ColumnType = 0x13 // 5.6.46中开始用到
	ColumnTypeNewDecimal ColumnType = 0xf6
	ColumnTypeEnum       ColumnType = 0xf7
	ColumnTypeSet        ColumnType = 0xf8
	ColumnTypeTinyBlob   ColumnType = 0xf9
	ColumnTypeMediumBlod ColumnType = 0xfa
	ColumnTypeLongBlob   ColumnType = 0xfb
	ColumnTypeBlob       ColumnType = 0xfc
	ColumnTypeVarString  ColumnType = 0xfd
	ColumnTypeString     ColumnType = 0xfe
	ColumnTypeGeometry   ColumnType = 0xff	 
)
// 关于meta def，可以参考
// https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html
// 在 https://dev.mysql.com/doc/internals/en/table-map-event.html 中Decimal的长度为2，但在上面的链接里长度为0
var columnMetaDefLength map[ColumnType] int
func init(){
	columnMetaDefLength = make(map[ColumnType] int)
	columnMetaDefLength[ColumnTypeString] = 2
	columnMetaDefLength[ColumnTypeVarString] = 2
	columnMetaDefLength[ColumnTypeVarchar] = 2
	columnMetaDefLength[ColumnTypeBlob] = 1
	columnMetaDefLength[ColumnTypeDecimal] = 2
	columnMetaDefLength[ColumnTypeNewDecimal] = 2
	columnMetaDefLength[ColumnTypeDouble] = 1
	columnMetaDefLength[ColumnTypeFloat] = 1
	columnMetaDefLength[ColumnTypeEnum] = 2
	columnMetaDefLength[ColumnTypeSet] = 2
	columnMetaDefLength[ColumnTypeBit] = 2 // 在文档上写的是0，但实际看起来是2（5.5.62）
	columnMetaDefLength[ColumnTypeTimestamp2] = 0 // 在5.6.46中发现这个
	columnMetaDefLength[ColumnTypeDate] = 0
	columnMetaDefLength[ColumnTypeDatetime] = 0
	columnMetaDefLength[ColumnTypeDatetime2] = 1 // mysql5.6.46中tbl_datetime类型
	columnMetaDefLength[ColumnTypeTimestamp] = 0
	columnMetaDefLength[ColumnTypeTime] = 0 // 文档中就是写了“--”
	columnMetaDefLength[ColumnTypeTime2] = 1
	columnMetaDefLength[ColumnTypeTiny] = 0
	columnMetaDefLength[ColumnTypeShort] = 0
	columnMetaDefLength[ColumnTypeInt24] = 0
	columnMetaDefLength[ColumnTypeLong] = 0
	columnMetaDefLength[ColumnTypeLonglong] = 0
	columnMetaDefLength[ColumnTypeYear] = 0
	columnMetaDefLength[ColumnTypeGeometry] = 1
}
type TableMapEventType struct{
	TableId          Uint6
	Flags            Uint2
	SchemaNameLength Uint1
	SchemaName       StringFix
	Filler1          Uint1 // 0x00
	TableNameLength  Uint1
	TableName        StringFix
	Filler2          Uint1 // 0x00
	ColumnCount      UintLenenc
	ColumnDef        []ColumnType
	ColumnMetaDef    []byte
	NullBitmask      []byte
}
func(this TableMapEventType)String() string{
	return fmt.Sprintf("{Type:TableMapEventType, TableId:%v, Flags:%v, SchemaNameLength:%v, SchemaName:%v, TableNameLength:%v, TableName:%v, ColumnCount:%v, ColumnDef:%v, ColumnMetaDef:%v, NullBitmask:%v}", this.TableId, this.Flags, this.SchemaNameLength, this.SchemaName, this.TableNameLength, this.TableName, this.ColumnCount, this.ColumnDef, this.ColumnMetaDef, this.NullBitmask)
}
func NewTableMapEvent() TableMapEventType{
	return TableMapEventType{}
}
type ColumnValueType struct{
	columnType ColumnType
	isNul      bool
	value      interface{}
}
func NewColumnValue(t ColumnType, isNul bool) ColumnValueType{
	ret := ColumnValueType{}
	ret.columnType = t
	ret.isNul = isNul
	return ret
}
func (this ColumnValueType)String() string{
	return fmt.Sprintf("{Type:ColumnValueType, columnType:%v, isNul:%v, value:%v}", this.columnType, this.isNul, this.value)
}
type RowsEventRowType struct{
	NulBitmap1 []byte
	Value1     []ColumnValueType
	NulBitmap2 []byte
	Value2     []ColumnValueType	
}
func NewRowsEventRow() RowsEventRowType{
	return RowsEventRowType{}
}
func (this RowsEventRowType)String() string{
	buf := bytes.NewBufferString("{Type:RowsEventRowType, NulBitmap1:[")
	for i := range this.NulBitmap1{
		buf.WriteString(fmt.Sprintf("%02x ", this.NulBitmap1[i]))
	}
	buf.WriteString("], Value1:[")
	for i := range this.Value1{
		buf.WriteString(fmt.Sprintf("%v, ", this.Value1[i]))
	}
	buf.WriteString("], NulBitmap2:[")
	for i := range this.NulBitmap2{
		buf.WriteString(fmt.Sprintf("%02x ", this.NulBitmap2[i]))
	}
	buf.WriteString("], Value2:[")
	for i := range this.Value2{
		buf.WriteString(fmt.Sprintf("%v, ", this.Value2[i]))
	}
	buf.WriteString("]}")
	return buf.String()
}
type RowsEvenCommand byte
const(
	RowsEvenCommandInsert RowsEvenCommand = 1
	RowsEvenCommandDelete RowsEvenCommand = 2
	RowsEvenCommandUpdate RowsEvenCommand = 3
)
type RowsEventType struct{
	Version               Uint1
	TableId               Uint6
	Flags                 Uint2
	ExtraDataLength       Uint2
	ExtraData             []byte
	NumberOfColumns       UintLenenc
	ColumnsPresentBitmap1 []byte
	ColumnsPresentBitmap2 []byte
	Rows                  []RowsEventRowType
	Command               RowsEvenCommand
	
}
func NewRowsEvent(ver Uint1, eventType Uint1) RowsEventType{
	ret := RowsEventType{}
	ret.Version = ver
	if eventType == EventTypeWriteRowsEventv0 || eventType == EventTypeWriteRowsEventv1 || eventType == EventTypeWriteRowsEventv2 {
		ret.Command = RowsEvenCommandInsert
	} else if eventType == EventTypeUpdateRowsEventv0 || eventType == EventTypeUpdateRowsEventv1 || eventType == EventTypeUpdateRowsEventv2 {
		ret.Command = RowsEvenCommandUpdate
	} else if eventType == EventTypeDeleteRowsEventv0 || eventType == EventTypeDeleteRowsEventv1 || eventType == EventTypeDeleteRowsEventv2 {
		ret.Command = RowsEvenCommandDelete
	}
	return ret
}
func (this RowsEventType)String() string{
	buf := bytes.NewBufferString(fmt.Sprintf("{Type:RowsEventType, Version:%v, Command:%v, TableId:%v, Flags:%v, ExtraDataLength:%v, ExtraData:%v, NumberOfColumns:%v, ", this.Version, this.Command, this.TableId, this.Flags, this.ExtraDataLength, this.ExtraData, this.NumberOfColumns))
	buf.WriteString("ColumnsPresentBitmap1:[")
	for i := range this.ColumnsPresentBitmap1 {
		buf.WriteString(fmt.Sprintf("%02x ", this.ColumnsPresentBitmap1[i]))
	}
	buf.WriteString("], ColumnsPresentBitmap2:[")
	for i := range this.ColumnsPresentBitmap2 {
		buf.WriteString(fmt.Sprintf("%02x ", this.ColumnsPresentBitmap2[i]))
	}
	buf.WriteString("], Rows:[")
	for i := range this.Rows {
		buf.WriteString(fmt.Sprintf("%v, ", this.Rows[i]))
	}
	buf.WriteString("]}")
	return buf.String()
}
type RowsQueryEventType struct{
	Length    Uint1
	QueryText StringEof
}
func NewRowsQueryEvent() RowsQueryEventType{
	return RowsQueryEventType{}
}

const MaxPacketPayloadSize = (1<<24)-1
type Packet struct{
	PayloadLength Uint3
	SequenceId Uint1
	Payload []byte
}
func (this *Packet) Decode() []byte{
	ret := append(this.PayloadLength.Decode(), this.SequenceId.Decode()...)
	return append(ret, this.Payload...)
}

const (
	EventTypeUnknownEvent           Uint1 = 0x00
	EventTypeStartEventV3           Uint1 = 0x01
	EventTypeQueryEvent             Uint1 = 0x02
	EventTypeStopEvent              Uint1 = 0x03
	EventTypeRotateEvent            Uint1 = 0x04
	EventTypeIntvarEvent            Uint1 = 0x05
	EventTypeLoadEvent              Uint1 = 0x06
	EventTypeSlaveEvent             Uint1 = 0x07
	EventTypeCreateFileEvent        Uint1 = 0x08
	EventTypeAppendBlockEvent       Uint1 = 0x09
	EventTypeExecLoadEvent          Uint1 = 0x0A
	EventTypeDeleteFileEvent        Uint1 = 0x0B
	EventTypeNewLoadEvent           Uint1 = 0x0C
	EventTypeRandEvent              Uint1 = 0x0D
	EventTypeUserVarEvent           Uint1 = 0x0E
	EventTypeFormatDescriptionEvent Uint1 = 0X0F
	EventTypeXidEvent               Uint1 = 0x10
	EventTypeBeginLoadQueryEvent    Uint1 = 0x11
	EventTypeExecuteLoadQueryEvent  Uint1 = 0x12
	EventTypeTableMapEvent          Uint1 = 0x13
	EventTypeWriteRowsEventv0       Uint1 = 0x14
	EventTypeUpdateRowsEventv0      Uint1 = 0x15
	EventTypeDeleteRowsEventv0      Uint1 = 0x16
	EventTypeWriteRowsEventv1       Uint1 = 0x17
	EventTypeUpdateRowsEventv1      Uint1 = 0x18
	EventTypeDeleteRowsEventv1      Uint1 = 0x19
	EventTypeIncidentEvent          Uint1 = 0x1A
	EventTypeHeartBeatEvent         Uint1 = 0x1B
	EventTypeIgnorableEvent         Uint1 = 0x1C
	EventTypeRowsQueryEvent         Uint1 = 0x1D
	EventTypeWriteRowsEventv2       Uint1 = 0x1E
	EventTypeUpdateRowsEventv2      Uint1 = 0x1F
	EventTypeDeleteRowsEventv2      Uint1 = 0x20
	EventTypeGtidEvent              Uint1 = 0x21
	EventTypeAnonymousGtidEvent     Uint1 = 0x22
	EventTypePreviousGtidsEvent     Uint1 = 0x23
)
type EventHeaderType struct{
	Timestamp Uint4
	EventType Uint1
	ServerId  Uint4
	EventSize Uint4
	LogPos    Uint4
	Flags     Uint2
}
type UnknownEventTypeError struct{
	EventType Uint1
}
func NewUnknownEventTypeError(eventType Uint1) UnknownEventTypeError{
	ret := UnknownEventTypeError{}
	ret.EventType = eventType
	return ret
}
func (this UnknownEventTypeError)Error() string{
	return fmt.Sprintf("Unknown EventType:%v", this.EventType)
}

type StartEventV3Type struct{
	BinlogVersion      Uint2
	MysqlServerVersion StringFix
	CreateTimestamp    Uint4
}
// 每个binlog的第一个事件。用于描述这个binlog文件
type FormatDescriptionEventType struct{
	BinlogVersion         Uint2
	MysqlServerVersion    StringFix
	CreateTimestamp       Uint4
	EventHeaderLength     Uint1
	EventTypeHeaderLength []byte
}
func NewFormatDescriptionEventType() FormatDescriptionEventType{
	return FormatDescriptionEventType{}
}
func (this FormatDescriptionEventType)String() string{
	buf := bytes.NewBufferString("{Type:FormatDescriptionEventType, BinlogVersion:%v, MysqlServerVersion:%v, CreateTimestamp:%v, EventHeaderLength:%v, EventTypeHeaderLength:[")
	params := make([]interface{}, 0)
	params = append(params, this.BinlogVersion, this.MysqlServerVersion, this.CreateTimestamp, this.EventHeaderLength)
	if this.EventTypeHeaderLength != nil{
		for _, v := range(this.EventTypeHeaderLength) {
			buf.WriteString("%v ")
			params = append(params, v)
		}
	}
	buf.WriteString("]}")
	return fmt.Sprintf(buf.String(), params...)
}
type RotateEventType struct{
	Position Uint8
	Name     StringEof
}
func NewRotateEventType() RotateEventType{
	return RotateEventType{}
}
func (this RotateEventType)String() string{
	return fmt.Sprintf("{Type:RotateEventType, Position:%v, Name:%v}", this.Position, this.Name)
}
type StopEventType struct{
}
func NewStopEventType() StopEventType{
	return StopEventType{}
}
const(
	Q_FLAGS2_CODE               Uint1 = 0x00
	Q_SQL_MODE_CODE             Uint1 = 0x01
	Q_CATALOG                   Uint1 = 0x02
	Q_AUTO_INCREMENT            Uint1 = 0x03
	Q_CHARSET_CODE              Uint1 = 0x04
	Q_TIME_ZONE_CODE            Uint1 = 0x05
	Q_CATALOG_NZ_CODE           Uint1 = 0x06
	Q_LC_TIME_NAMES_CODE        Uint1 = 0x07
	Q_CHARSET_DATABASE_CODE     Uint1 = 0x08
	Q_TABLE_MAP_FOR_UPDATE_CODE Uint1 = 0x09
	Q_MASTER_DATA_WRITTEN_CODE  Uint1 = 0x0a
	Q_INVOKERS                  Uint1 = 0x0b
	Q_UPDATED_DB_NAMES          Uint1 = 0x0c
	Q_MICROSECONDS              Uint1 = 0x0d
)

type StatusVarType struct{
	uint4Val       Uint4
	uint8Val       Uint8
	bytesVal       []byte
	uint2Val1      Uint2
	uint2Val2      Uint2
	uint2Val3      Uint2
	stringFixVal1  StringFix
	stringFixVal2  StringFix
	updatedDbNames []StringNul
	uint3Val       Uint3
	
}
func NewStatusVarType() StatusVarType{
	return StatusVarType{}
}
type StatusVarsType map[Uint1] StatusVarType
type QueryEventType struct{
	SlaveProxyId     Uint4
	ExecutionTime    Uint4
	SchemaLength     Uint1
	ErrorCode        Uint2
	StatusVarsLength Uint2
	StatusVars       StatusVarsType
	Schema           StringFix
	Reserved         Uint1
	Query            StringEof
}
func(this QueryEventType)String() string{
	buf := bytes.NewBufferString("{Type:QueryEventType, SlaveProxyId:%v, ExecutionTime:%v, SchemaLength:%v, ErrorCode:%v, StatusVarsLength:%v, StatusVars:{")
	params := make([]interface{}, 0)
	params = append(params, this.SlaveProxyId, this.ExecutionTime, this.SchemaLength, this.ErrorCode, this.StatusVarsLength)
	keys := make([]int, 0)
	for k := range this.StatusVars{
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	for i := range keys{
		k := keys[i]
		buf.WriteString("%x:%v, ")
		params = append(params, k)
		v := this.StatusVars[Uint1(k)]
		switch(Uint1(k)){
			case Q_FLAGS2_CODE:
			params = append(params, v.uint4Val)

			case Q_SQL_MODE_CODE:
			params = append(params, v.uint8Val)

			case Q_CATALOG:
			params = append(params, v.bytesVal)

			case Q_AUTO_INCREMENT:
			params = append(params, [2]Uint2{v.uint2Val1, v.uint2Val2})

			case Q_CHARSET_CODE:
			params = append(params, [3]Uint2{v.uint2Val1, v.uint2Val2, v.uint2Val3})

			case Q_TIME_ZONE_CODE:
			params = append(params, v.stringFixVal1)

			case Q_CATALOG_NZ_CODE:
			params = append(params, v.stringFixVal1)

			case Q_LC_TIME_NAMES_CODE:
			params = append(params, v.uint2Val1)

			case Q_CHARSET_DATABASE_CODE:
			params = append(params, v.uint2Val1)

			case Q_TABLE_MAP_FOR_UPDATE_CODE:
			params = append(params, v.uint8Val)

			case Q_MASTER_DATA_WRITTEN_CODE:
			params = append(params, v.uint4Val)

			case Q_INVOKERS:
			params = append(params, [2]StringFix{v.stringFixVal1, v.stringFixVal2})

			case Q_UPDATED_DB_NAMES:
			params = append(params, v.updatedDbNames)

			case Q_MICROSECONDS:
			params = append(params, v.uint3Val)
		}
	}
	buf.WriteString("}, Schema:%v, Query:%v}")
	params = append(params, this.Schema, this.Query)
	return fmt.Sprintf(buf.String(), params...)
}

type QueryEventStatusVarError struct{
	msg string
}
func NewQueryEventStatusVarError(msg string) QueryEventStatusVarError{
	ret := QueryEventStatusVarError{}
	ret.msg = msg
	return ret
}
func (this QueryEventStatusVarError)Error() string{
	return this.msg
}
type LoadEventType struct{
	SlaveProxyId     Uint4
	ExecTime         Uint4
	SkipLines        Uint4
	TableNameLen     Uint1
	SchemaLen        Uint1
	NumFields        Uint4
	FieldTerm        Uint1
	EnclosedBy       Uint1
	LineTerm         Uint1
	LintStart        Uint1
	EscapedBy        Uint1
	OptFlags         Uint1
	EmptyFlags       Uint1
	FieldNameLengths []Uint1
	FieldNames       []StringNul
	TableName        StringNul
	SchemaName       StringNul
	FileName         StringNul
}
func NewLoadEvent() LoadEventType{
	return LoadEventType{}
}
type NewLoadEventType struct{
	SlaveProxyId     Uint4
	ExecutionTime    Uint4
	SkipLines        Uint4
	TableNameLen     Uint1
	SchemaLen        Uint1
	NumFields        Uint4
	FieldTermLen     Uint1
	FieldTerm        []byte
	EnclosedByLen    Uint1
	EnclosedBy       []byte
	LineTermLen      Uint1
	LineTerm         []byte
	LineStartLen     Uint1
	LineStart        []byte
	EscapedByLen     Uint1
	EscapedBy        []byte
	OptFlags         Uint1
	FieldNameLengths []byte
	FieldNames       []StringNul
	TableName        StringNul
	SchemaName       StringNul
	FileName         StringNul
}
func NewNewLoadEventType() NewLoadEventType{
	return NewLoadEventType{}
}
type CreateFileEventType struct{
	FileId    Uint4
	BlockData StringEof
}
func NewCreateFileEvent() CreateFileEventType{
	return CreateFileEventType{}
}
type AppendBlockEventType struct{
	FileId    Uint4
	BlockData StringEof
}
func NewAppendBlockEvent() AppendBlockEventType{
	return AppendBlockEventType{}
}
type ExecLoadEventType struct{
	FileId Uint4
}
func NewExecLoadEvent() ExecLoadEventType{
	return ExecLoadEventType{}
}
type BeginLoadQueryEventType struct{
	FileId    Uint4
	BlockData StringEof
}
func NewBeginLoadQueryEvent() BeginLoadQueryEventType{
	return BeginLoadQueryEventType{}
}
type ExecuteLoadQueryEventType struct{
	SlaveProxyId     Uint4
	ExecutionTime    Uint4
	SchemaLength     Uint1
	ErrorCode        Uint2
	StatusVarsLength Uint2
	FileId           Uint4
	StartPos         Uint4
	EndPos           Uint4
	DupHandlingFlags Uint1
}
func NewExecuteLoadQueryEvent() ExecuteLoadQueryEventType{
	return ExecuteLoadQueryEventType{}
}
type DeleteFileEventType struct{
	FileId Uint4
}
func NewDeleteFileEvent() DeleteFileEventType{
	return DeleteFileEventType{}
}
type RandEventType struct{
	Seed1 Uint8
	Seed2 Uint8
}
func NewRandEvent() RandEventType{
	return RandEventType{}
}
type XIDEventType struct{
	Xid Uint8
}
func(this XIDEventType)String() string{
	return fmt.Sprintf("{Type: XIDEventType, Xid:%v}", this.Xid)
}
func NewXIDEvent() XIDEventType{
	return XIDEventType{}
}
type IntvarEventType struct{
	Type  Uint1
	Value Uint8
}
func NewIntvarEvent() IntvarEventType{
	return IntvarEventType{}
}
type UserVarEventType struct{
	NameLength  Uint4
	Name        StringFix
	IsNull      Uint1
	Type        Uint1
	Charset     Uint4
	ValueLength Uint4
	Value       []byte
	Flags       Uint1
}
func NewUserVarEvent() UserVarEventType{
	return UserVarEventType{}
}
type IncidentEventType struct{
	Type          Uint2
	MessageLength Uint1
	Message       StringFix
}
func NewIncidentEvent() IncidentEventType{
	return IncidentEventType{}
}
type ColumnValueTimeFromatError struct{
	length int
}
func NewColumnValueTimeFromatError(l int) ColumnValueTimeFromatError{
	ret := ColumnValueTimeFromatError{}
	ret.length = l
	return ret
}
func(this ColumnValueTimeFromatError)Error() string{
	return fmt.Sprintf("ColumnValueTimeFromatError: length(%v) is invalid", this.length)
}
// 因为Mysql中有些数据是没法转成time.Time的，比如"0000-00-00"这样无效的日期，所以这里新做一个类型。
const (
	ZeroDateFormat byte = 0
	DateOnlyFormat byte = 4
	DateTimeFormat byte = 7
	DateTimeNanoFormat byte = 11
)
type ColumnValueTimeType struct {
	// 格式。参考readColumnValueTypeDatetime
	format byte
	year Uint2
	month, day, hour, minute, second Uint1
	microSecond Uint4
}
func (this ColumnValueTimeType)String() string{
	var ret string
	switch(this.format){
		case ZeroDateFormat, DateTimeFormat:
		ret = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", this.year, this.month, this.day, this.hour, this.minute, this.second)
		case DateTimeNanoFormat:
		ret = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%09d", this.year, this.month, this.day, this.hour, this.minute, this.second, this.microSecond * 1000)
		case DateOnlyFormat:
		ret = fmt.Sprintf("%04d-%02d-%02d", this.year, this.month, this.day)
	}
	return ret
}
func (this *ColumnValueTimeType)Time() (time.Time, error){
	return time.Parse("2006-01-02 15:04:05.999999999", this.String())
}
func NewColumnValueTime(format byte, year Uint2, month, day, hour, minute, second Uint1, microSecond Uint4) ColumnValueTimeType{
	ret := ColumnValueTimeType{}
	ret.format = format
	ret.year   = year
	ret.month  = month
	ret.day    = day
	ret.hour   = hour
	ret.minute = minute
	ret.second = second
	ret.microSecond = microSecond
	return ret
}

type ColumnValueSetType struct{
	Index int; // 第几个值
	Value string;
}
func NewColumnValueSet() ColumnValueSetType{
	return ColumnValueSetType{}
}
