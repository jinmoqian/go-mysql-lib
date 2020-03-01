package mysql

//import "fmt"

type DumpFromFlag int

const (
	DumpFromBeginning DumpFromFlag = 1 // 启动时从binlog的最开始开始
	DumpFromPosition               = 2 // 启动时从某特定位置开始读
	DumpFromLatest                 = 3 // 启动时从当前位置开始读
)

// binlog文件的位置
type BinglogType struct {
	Filename  string
	BinlogPos uint32
}
type Config struct {
	Host           string
	Port           string
	User           string
	Pass           string
	ServerId       int
	DumpFrom       DumpFromFlag
	BinlogPosition BinglogType
	Continue       bool // 启动时是否从上次记录的位置开始。如果为true，并且有记录，则DumpFrom无效；否则以DumpFrom为准
	LogTag         uint32
	//buf []byte
}

//func (this *Config) clearBuf(){
//	this.buf = make([]byte, 0)
//}
// func (this *Config) appendBuf(b []byte){
// 	this.buf = append(this.buf, b...)
// }
func NewConfig() *Config {
	ret := &Config{}
	//	ret.buf = make([]byte, 0)
	return ret
}

// 保存当前服务器状态的接口
type Storage interface {
	Save()
}

// 输出log用的
const (
	LogError     uint32 = 0x00000001
	LogWarning          = 0x00000002
	LogPacket           = 0x00000004
	LogTCPStream        = 0x00000008
	LogVariable         = 0x00000010
)

type Log interface {
	Log(logTag uint32, content string)
}
