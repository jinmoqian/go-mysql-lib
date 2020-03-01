package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

type byteStream struct {
	bytes []byte // 读入的字节流
	n     int64  // 读入的字节长度
	err   error  // 读入时产生的error
}
type writeResult struct {
	n   int   // 写入的字节数
	err error // 写入时得到的错误
}
type Stream struct {
	controlChannel     chan int64
	readChannel        chan byteStream
	writeChannel       chan []byte
	writeResultChannel chan writeResult
	byteReadCounter    int64                                                       // 用于统计读入的字节数
	back               []byte                                                      // 推进来的字符的堆栈
	backPos            int                                                         // 个数
	buildPayload       func(Stream, byte, int, bool, chan interface{}, chan error) // 生成Payload的函数
	config             *Config

	SequenceId   Uint1 // 当前packet
	serverConfig *ServerConfigType
	log          Log
	buf          []byte
}

func NewStream() Stream {
	s := Stream{}
	s.initStream()
	s.initSequenceId()
	return s
}
func (this *Stream) initSequenceId() {
	this.SequenceId = 255
}
func (this *Stream) nextSequenceId() Uint1 {
	return (this.SequenceId + 1) % 256
}
func (this *Stream) initStream() {
	this.controlChannel = make(chan int64)
	this.readChannel = make(chan byteStream)
	this.writeChannel = make(chan []byte)
	this.writeResultChannel = make(chan writeResult)
	this.back = make([]byte, 8)
	this.reset()
	this.config = NewConfig()
	this.serverConfig = &ServerConfigType{}
	if this.log != nil && this.config.LogTag&LogTCPStream != 0 {
		this.buf = make([]byte, 0)
	}

	this.buildPayload = func(subStream Stream, payloadType byte, length int, isEvent bool, payloadChannel chan interface{}, payloadErrorChannel chan error) {
		var packet interface{}
		var err error
		// 区分OK与EOF
		// OK: header=0 并且 length > 7
		// EOF: header=0xFE 并且 length < 9
		switch {
		case payloadType == 0x00:
			if isEvent {
				event, e := subStream.readEventPacket(length)
				packet = event
				err = e
			} else {
				okPacket, e := subStream.readOKPacket(length)
				packet = okPacket
				err = e
			}

		case payloadType == 0x0A:
			handShakePacket, e := subStream.readHandshakeV10Packet(length)
			packet = handShakePacket
			err = e

		case payloadType == 0xFF:
			errPacket, e := subStream.readErrPacket(length)
			packet = errPacket
			err = e

		case payloadType == 0xFE:
			if length == 1 {
				packet, err = subStream.readOldAuthSwitchRequestPacket()
			} else if length == 5 {
				packet, err = subStream.readEOFPacket(length)
			} else {
				packet, err = subStream.readAuthSwitchRequestPacket(length)
			}
		}
		close(subStream.controlChannel)
		payloadChannel <- packet
		payloadErrorChannel <- err
	}
}
func (this *Stream) close() {
	if this.controlChannel != nil {
		close(this.controlChannel)
		this.controlChannel = nil
	}
}
func (this *Stream) reset() {
	this.byteReadCounter = 0

}
func (this *Stream) moreDataInPayload(length int) bool {
	crcLength := int64(0)
	if this.crc32Checked() {
		crcLength = 4
	}
	// fmt.Println("moreDataInPayload, crcLength=", crcLength, " length=", length, " crcLength=", crcLength)
	return this.byteReadCounter < (int64(length) - crcLength)
}
func (this *Stream) crc32Checked() bool {
	// 从5.6版开始支持CRC、row_image只记部分等
	// https://docs.oracle.com/cd/E17952_01/mysql-5.6-en/replication-compatibility.html
	if this.serverConfig.compareVersion("5.6.0") >= 0 {
		return this.serverConfig.ServerCrc32CheckFlag
	} else {
		return false
	}
}

// 把b重新推回字节流。下一次重新读出来
// 注意，推进去的顺序与读出来的顺序是相反的。如pushBack(10)，pushBack(20)，pushBack(30)，读出来的顺序是30、20、10
func (this *Stream) pushBack(b byte) {
	if this.backPos == len(this.back) {
		this.back = append(this.back, b)
	} else {
		this.back[this.backPos] = b
	}
	this.backPos++
}

type Error struct {
	err   string
	Value int
}

func (this Error) Error() string {
	return this.err
}

type EOFError struct {
}

func (this EOFError) Error() string {
	return "EOF of data provider"
}

const ReadAllPayload = int64(-1) // 读出所有的payload。哪怕是跨packet的
// 当n>0时，表示读入n个字节。其中返回值中len(bytes)一定等于int，但注意返回的int与n可能不等。这种情况意味首[]byte中可能只有部分数据。并且会有error。
func (this *Stream) readNBytes(n int64) ([]byte, int, error) {
	var channelBytes int64
	var backBytes int64
	if n == ReadAllPayload {
		backBytes = int64(this.backPos)
		channelBytes = ReadAllPayload
	} else {
		// 实际从channel读的字节数=需要的字节数-缓存的字节数
		channelBytes = n - int64(this.backPos)
		if channelBytes < 0 {
			channelBytes = 0
		}
		// 从缓存中读的字节数
		backBytes = n - channelBytes
	}

	// 从channel读
	var r byteStream
	ok := true
	if channelBytes >= 0 || n == ReadAllPayload {
		this.controlChannel <- channelBytes // 表示需要读channelBytes个字节，或读所有
		r, ok = <-this.readChannel          // 返回数据
		if ok && (channelBytes >= 0 && channelBytes != r.n) {
			this.log.Log(LogWarning, fmt.Sprintf("Buffer read, but size=%v not exptected channelBytes=%v", r.n, channelBytes))
			// 有数据返回，但返回的数据量不等于期望的数据量，需要裁剪返回的slice。之后应该返回一个报错
			buf := r.bytes[0:r.n]
			r.bytes = buf
			channelBytes = r.n
			r.err = EOFError{}
		}
		// if ok && this.config != nil {
		// 	this.config.appendBuf(r.bytes)
		// }
	}

	// 从back缓存里读
	var buf []byte
	if backBytes > 0 {
		buf = make([]byte, backBytes)
		for i := int64(0); i < backBytes; i++ {
			buf[i] = this.back[this.backPos-1-int(i)]
		}
		this.backPos -= int(backBytes)
	}

	// 如果需要合并
	if (channelBytes > 0 || n == ReadAllPayload) && ok {
		if backBytes > 0 {
			buf = append(buf, r.bytes...)
		} else {
			buf = r.bytes
		}
	}

	if !ok {
		// channel被数据提供方关闭。如果数据提供方提供了错误原因（如超时关闭等），用具体的原因；如果没有，用简单的EOFError。
		if r.err == nil {
			// fmt.Println("channel被数据提供方关闭。如果数据提供方提供了错误原因（如超时关闭等），用具体的原因；如果没有，用简单的EOFError。")
			r.err = EOFError{}
		}

	}
	//if r.err != nil{
	//	panic(r.err)
	//}
	this.byteReadCounter += (channelBytes + backBytes)
	return buf, int(channelBytes + backBytes), r.err
}
func (this *Stream) readFixLengthObject(n int, obj FixLengthEncoder) error {
	buf, _, err := this.readNBytes(int64(n))
	if err == nil {
		obj.Encode(buf)
	}
	return err
}
func (this *Stream) ReadUint1() (Uint1, error) {
	var ret Uint1
	err := this.readFixLengthObject(Uint1Length, &ret)
	return ret, err
}
func (this *Stream) ReadUint2() (Uint2, error) {
	var ret Uint2
	err := this.readFixLengthObject(Uint2Length, &ret)
	return ret, err
}
func (this *Stream) ReadUint3() (Uint3, error) {
	var ret Uint3
	err := this.readFixLengthObject(Uint3Length, &ret)
	return ret, err
}
func (this *Stream) ReadUint4() (Uint4, error) {
	var ret Uint4
	err := this.readFixLengthObject(Uint4Length, &ret)
	return ret, err
}
func (this *Stream) ReadUint6() (Uint6, error) {
	var ret Uint6
	err := this.readFixLengthObject(Uint6Length, &ret)
	return ret, err
}
func (this *Stream) ReadUint8() (Uint8, error) {
	var ret Uint8
	err := this.readFixLengthObject(Uint8Length, &ret)
	return ret, err
}

const (
	NotUintLenenc = "0xFF/0xFB2 In UintLenenc"
)

func (this *Stream) ReadUintLenenc() (UintLenenc, error) {
	var ret UintLenenc
	buf, _, err := this.readNBytes(1)
	if err == nil {
		var need int
		if buf[0] < 251 {
			ret = UintLenenc(buf[0]) // If the value is < 251, it is stored as a 1-byte integer.
		} else {
			if buf[0] == 0xFC {
				// 后面跟2字节
				need = 2 // If the value is ≥ 251 and < (2^16), it is stored as fc + 2-byte integer.
			} else if buf[0] == 0xFD {
				// 后面跟3字节
				need = 3 // If the value is ≥ (2^16) and < (2^24), it is stored as fd + 3-byte integer.
			} else if buf[0] == 0xFE {
				// 后面跟8字节
				// If the value is ≥ (2^24) and < (2^64) it is stored as fe + 8-byte integer.
				// Note :Up to MySQL 3.22, 0xfe was followed by a 4-byte integer.
				// 这里不考虑Version=3.22以下
				need = 8
			} else if buf[0] == 0xFF || buf[0] == 0xFB {
				// 特殊含义
				// If it is 0xfb, it is represents a NULL in a ProtocolText::ResultsetRow.
				// If it is 0xff and is the first byte of an ERR_Packet
				err := Error{}
				err.Value = int(buf[0])
				err.err = NotUintLenenc
				return ret, err
			}
			bytes, _, err := this.readNBytes(int64(need))
			if err == nil {
				ret = UintLenenc(byte2uint64(bytes, need))
			}
		}
	}
	return ret, nil
}

func (this *Stream) ReadStringFix(len int) (StringFix, error) {
	var ret StringFix
	bytes, _, err := this.readNBytes(int64(len))
	if err == nil {
		ret = StringFix(bytes)
	}
	return ret, err
}
func (this *Stream) ReadStringNul() (StringNul, error) {
	var result []byte
	const bufLen = 128 // 128个字符合并一次
	buf := make([]byte, bufLen)
	var err error
	for {
		nulIndex := bufLen
		for i := 0; i < bufLen; i++ {
			bytes, _, err := this.readNBytes(1)
			if err != nil {
				break
			}

			// 查找0x00
			if bytes[0] == 0x00 {
				nulIndex = i
				break
			}
			buf[i] = bytes[0]
		}
		result = append(result, buf[0:nulIndex]...)
		if nulIndex != bufLen {
			break
		}
	}
	return StringNul(result), err
}

// 一直读到整个Packet结尾
func (this *Stream) ReadStringEof(packetLength int) (StringEof, error) {
	if bytes, _, err := this.readNBytes(int64(packetLength) - this.byteReadCounter - int64(this.serverConfig.CrcSize)); err == nil {
		return StringEof(bytes), nil
	} else {
		return StringEof(""), err
	}
}

func (this *Stream) ReadStringLenenc() (StringLenenc, error) {
	len, err := this.ReadUintLenenc()
	if err == nil {
		bytes, _, err := this.readNBytes(int64(len))
		if err == nil {
			return StringLenenc(bytes), err
		}
	}
	return "", err
}

func (this *Stream) ReadPacketHeader() (Packet, error) {
	ret := Packet{}
	var err error
	if ret.PayloadLength, err = this.ReadUint3(); err == nil {
		ret.SequenceId, err = this.ReadUint1()
	}
	if err == nil {
		this.SequenceId = ret.SequenceId
	}
	return ret, err
}
func (this *Stream) tstFlag(flag CapalibilityFlagType) bool {
	return (this.serverConfig.CapabilityFlags & Uint4(flag)) != 0
}
func (this *Stream) tstStatus(status ServerStatusType) bool {
	return this.serverConfig.StatusFlags&Uint2(status) != 0
}
func (this *Stream) readEventHeader() (EventHeaderType, error) {
	ret := EventHeaderType{}
	var err error
	if ret.Timestamp, err = this.ReadUint4(); err == nil {
		if ret.EventType, err = this.ReadUint1(); err == nil {
			if ret.ServerId, err = this.ReadUint4(); err == nil {
				if ret.EventSize, err = this.ReadUint4(); err == nil {
					if this.serverConfig.BinlogVersion > 1 {
						if ret.LogPos, err = this.ReadUint4(); err == nil {
							ret.Flags, err = this.ReadUint2()
						}
					}
				}
			}
		}
	}
	return ret, err
}

var readColumnValueFunc map[ColumnType]func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error)

func readColumnValueTypeBytesLessOrEqual255Bytes(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
	// 只用一个字节表示长度
	var err error
	var n Uint1
	var buf []byte
	if n, err = stream.ReadUint1(); err == nil {
		buf, _, err = stream.readNBytes(int64(n))
	}
	return interface{}(buf), err
}
func readColumnValueTypeBytes(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
	var err error
	var n UintLenenc
	var buf []byte
	if n, err = stream.ReadUintLenenc(); err == nil {
		buf, _, err = stream.readNBytes(int64(n))
	}
	return interface{}(buf), err
}
func readColumnValueTypeString(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
	var s string
	ret, err := readColumnValueTypeBytes(schema, table, column, metaDef, stream)
	if ret != nil {
		if buf, ok := ret.([]byte); ok {
			s = string(buf)
		} else {
			err = Error{fmt.Sprintf("ColumnTypeString returned no []byte"), 0}
		}
	}
	return s, err
}
func readColumnValueTypeInt32(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
	var err error
	var buf []byte
	var ret int32
	if buf, _, err = stream.readNBytes(4); err == nil {
		ret = int32(binary.LittleEndian.Uint32(buf))
	}
	return ret, err
}
func readColumnValueTypeInt16(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
	var err error
	var buf []byte
	var ret int16
	if buf, _, err = stream.readNBytes(2); err == nil {
		ret = int16(binary.LittleEndian.Uint16(buf))
	}
	return ret, err
}
func unsigned(schema, table, column string, stream *Stream) bool {
	columnAttr := stream.serverConfig.getColumn(schema, table, column)
	if columnAttr != nil {
		return columnAttr.Signed
	}
	return false
}
func readColumnValueTypeInt8(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
	var err error
	var buf []byte
	var ret int8
	var uret uint8
	if buf, _, err = stream.readNBytes(1); err == nil {
		if unsigned(schema, table, column, stream) {
			uret = uint8(buf[0])
			return uret, err
		} else {
			ret = int8(buf[0])
			return ret, err
		}
	}
	return nil, err
}
func readColumnValueTypeFloat64(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
	var err error
	var buf []byte
	var ret float64
	if buf, _, err = stream.readNBytes(8); err == nil {
		// TODO: 这里需要处理有无符号
		ret = math.Float64frombits(binary.LittleEndian.Uint64(buf))
	}
	return ret, err
}
func readColumnValueTypeDatetime(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
	var ret ColumnValueTimeType
	var l Uint1
	var err error
	var year Uint2
	var month, day, hour, minute, second Uint1
	var microSecond Uint4
	var format byte
	if l, err = stream.ReadUint1(); err == nil {
		switch l {
		case 0:
			format = 0
		case 4, 7, 11:
			format = byte(l)
			if year, err = stream.ReadUint2(); err == nil {
				if month, err = stream.ReadUint1(); err == nil {
					if day, err = stream.ReadUint1(); err == nil && (l == 7 || l == 11) {
						if hour, err = stream.ReadUint1(); err == nil {
							if minute, err = stream.ReadUint1(); err == nil {
								if second, err = stream.ReadUint1(); err == nil && l == 11 {
									microSecond, err = stream.ReadUint4()
								}
							}
						}
					}
				}
			}
		default:
			err = NewColumnValueTimeFromatError(int(l))
		}
	}
	if err == nil {
		ret = NewColumnValueTime(format, year, month, day, hour, minute, second, microSecond)
	}
	return ret, err
}
func init() {
	readColumnValueFunc = make(map[ColumnType]func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error))
	readColumnValueFunc[ColumnTypeDecimal] = readColumnValueTypeBytes
	readColumnValueFunc[ColumnTypeTiny] = readColumnValueTypeInt8
	readColumnValueFunc[ColumnTypeShort] = readColumnValueTypeInt16
	readColumnValueFunc[ColumnTypeLong] = readColumnValueTypeInt32
	readColumnValueFunc[ColumnTypeFloat] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		var err error
		var buf []byte
		var ret float32
		// TODO: 需要增加无符号处理
		if buf, _, err = stream.readNBytes(4); err == nil {
			ret = math.Float32frombits(binary.LittleEndian.Uint32(buf))
		}
		return ret, err
	}
	readColumnValueFunc[ColumnTypeDouble] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		var err error
		var buf []byte
		var ret float64
		// TODO: 需要增加无符号处理
		if buf, _, err = stream.readNBytes(8); err == nil {
			ret = math.Float64frombits(binary.LittleEndian.Uint64(buf))
		}
		return ret, err
	}
	readColumnValueFunc[ColumnTypeNull] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		return nil, nil
	}
	readColumnValueFunc[ColumnTypeTimestamp] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		//readColumnValueTypeDatetime
		// 5.5.62下是4个int32，表示0000-00-00 00:00:00、1970-01-01 08:00:01～2038-01-19 11:14:07
		var ret ColumnValueTimeType
		val, err := readColumnValueTypeInt32(schema, table, column, metaDef, stream)
		if int32Val, ok := val.(int32); ok {
			// TODO 这里要考虑数据库中设定的时区，而不是程序的时区
			if int32Val == 0 {
				ret = NewColumnValueTime(ZeroDateFormat, 0, 0, 0, 0, 0, 0, 0)
			} else {
				t := time.Unix(int64(int32Val), 0)
				ret = NewColumnValueTime(DateTimeFormat, Uint2(t.Year()), Uint1(t.Month()), Uint1(t.Day()), Uint1(t.Hour()), Uint1(t.Minute()), Uint1(t.Second()), 0)
			}
		} else {
			err = Error{"ColumnTypeTimestamp not int32", 0}
		}
		return ret, err
	}
	readColumnValueFunc[ColumnTypeLonglong] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		var err error
		var buf []byte
		var ret int64
		if buf, _, err = stream.readNBytes(8); err == nil {
			ret = int64(binary.LittleEndian.Uint64(buf))
		}
		return ret, err
	}
	readColumnValueFunc[ColumnTypeInt24] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		var err error
		var buf []byte
		var ret int32
		if buf, _, err = stream.readNBytes(3); err == nil {
			buf = append([]byte{0x00}, buf...)
			nb := bytes.NewBuffer(buf)
			binary.Read(nb, binary.LittleEndian, &ret)
			ret = ret >> 8
		}
		return ret, err
	}
	readCompactDate := func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// 实际看来这里与网上说的不相符。实际上当只有年-月-日时，是把年月日压缩在3个字节内，格式不明确
		// 用例
		// 9999 = 10 0111 0000 1111(2) 12 - 1100(2) 31 - 11111(2)
		// 9F1F4E = 1001 1111 0001 1111 0100 1110
		// 2020 = 00 0111 1110 0100(2) 02 - 0010(2) 07 - 00111(2)
		// 47c80f = 0100 0111 1100 1000 0000 1111
		// 2020 = 00 0111 1110 0100(2) 02 - 0010(2) 06 - 00110(2)
		// 46c80f = 0100 0110 1100 1000 0000 1111
		// 2020 = 00 0111 1110 0100(2) 03 - 0011(2) 06 - 00110(2)
		// 66c80f = 0110 0110 1100 1000 0000 1111
		//          MMM  DDDD
		// 2020 = 00 0111 1110 0100(2) 12 - 1100(2) 06 - 00110(2)
		// 86c90f = 1000 0110 1100 1001 0000 1111
		//          MMM  DDDD         M
		// 2021 = 00 0111 1110 0101(2) 12 - 1100(2) 06 - 00110(2)
		// 86cB0f = 1000 0110 1100 1011 0000 1111
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		// 4069 = 00 1111 1110 0101(2) 12 - 1100(2) 06 - 00110(2)
		// 86cB1f = 1000 0110 1100 1011 0001 1111
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		// 8165 = 01 1111 1110 0101(2) 12 - 1100(2) 06 - 00110(2)
		// 86cB3f = 1000 0110 1100 1011 0011 1111
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		// 8192 = 10 0000 0000 0000(2) 12 - 1100(2) 06 - 00110(2)
		// 860140 = 1000 0110 0000 0001 0100 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		// 8192 = 10 0000 0000 0000(2) 03 - 0011(2) 06 - 00110(2)
		// 660040 = 0110 0110 0000 0000 0100 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 997   = 00 0011 1110 0101(2) 03 - 0011(2) 06 - 00110(2)
		// 66CA07 = 0110 0110 1100 1010 0000 0111
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 485   = 00 0001 1110 0101(2) 03 - 0011(2) 06 - 00110(2)
		// 66CA03 = 0110 0110 1100 1010 0000 0011
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 229   = 00 0000 1110 0101(2) 03 - 0011(2) 06 - 00110(2)
		// 66CA01 = 0110 0110 1100 1010 0000 0001
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 101   = 00 0000 0110 0101(2) 03 - 0011(2) 06 - 00110(2)
		// 66CA00 = 0110 0110 1100 1010 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 2064  = 00 1000 0001 0000(2) 03 - 0011(2) 06 - 00110(2)
		// 662010 = 0110 0110 0010 0000 0001 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 0064  = 00 0000 0100 0000(2) 03 - 0011(2) 06 - 00110(2)
		// 668000 = 0110 0110 1000 0000 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 0032  = 00 0000 0010 0000(2) 03 - 0011(2) 06 - 00110(2)
		// 664000 = 0110 0110 0100 0000 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 0016  = 00 0000 0001 0000(2) 03 - 0011(2) 06 - 00110(2)
		// 664000 = 0110 0110 0010 0000 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 0008  = 00 0000 0000 1000(2) 03 - 0011(2) 06 - 00110(2)
		// 661000 = 0110 0110 0001 0000 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 0004  = 00 0000 0000 0100(2) 03 - 0011(2) 06 - 00110(2)
		// 660800 = 0110 0110 0000 1000 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 0002  = 00 0000 0000 0100(2) 03 - 0011(2) 06 - 00110(2)
		// 660400 = 0110 0110 0000 0100 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 0001  = 00 0000 0000 0010(2) 03 - 0011(2) 06 - 00110(2)
		// 660200 = 0110 0110 0000 0010 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 0001  = 00 0000 0000 0010(2) 03 - 0011(2) 06 - 00110(2)
		// 660200 = 0110 0110 0000 0010 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 0001  = 00 0000 0000 0010(2) 04 - 0100(2) 06 - 00110(2)
		// 660200 = 0110 0110 0000 0010 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 860200 = 1000 0110 0000 0010 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 060300 = 0000 0110 0000 0011 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 260300 = 0010 0110 0000 0011 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 2C0300 = 0010 1100 0000 0011 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		// 380300 = 0011 1000 0000 0011 0000 0000
		//          MMMD DDDD YYYY YYYM NYYY YYYY
		//          2341 2345 89AB CDE1  123 4567
		var ret ColumnValueTimeType
		var err error
		var buf []byte
		var year Uint2
		var month, day Uint1
		if buf, _, err = stream.readNBytes(3); err == nil {
			year = (Uint2(buf[2])&0x7F)<<8 | Uint2(buf[1])
			year = year >> 1
			month = (Uint1(buf[1])&0x01)<<3 | (Uint1(buf[0]) >> 5)
			day = Uint1(buf[0]) & Uint1(0x1F)
			var format byte
			if year == 0 && month == 0 && day == 0 {
				format = 0
			} else {
				format = 4
			}
			ret = NewColumnValueTime(format, year, month, day, 0, 0, 0, 0)
		}
		return ret, nil
	}
	readColumnValueFunc[ColumnTypeDate] = readCompactDate
	readColumnValueFunc[ColumnTypeTime] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// 实际上看和下面文档中说的不一样，可能是版本差异？在5.5.62上返回的格式是8385959表示838小时59分59秒，或负数
		var ret time.Duration
		var err error
		var buf []byte
		if buf, _, err = stream.readNBytes(3); err == nil {
			var val int32
			buf = append([]byte{0x00}, buf...)
			binary.Read(bytes.NewBuffer(buf), binary.LittleEndian, &val)
			val = val >> 8
			ret, err = time.ParseDuration(fmt.Sprintf("%vh%vm%vs", (val / 10000), int32(math.Abs(float64(val/100%100))), int32(math.Abs(float64(val%100)))))
		}
		return ret, err
	}
	func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		if metaDef == nil {
			return nil, nil
		}
		// 文档：https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_TIME
		var err error
		var l Uint8
		var days Uint4
		var isNegative, hour, minute, second Uint1
		var microSecond Uint4
		var ret time.Duration
		if l, err = stream.ReadUint8(); err == nil {
			switch l {
			case 0:
			case 8, 12:
				if isNegative, err = stream.ReadUint1(); err == nil {
					if days, err = stream.ReadUint4(); err == nil {
						if hour, err = stream.ReadUint1(); err == nil {
							if minute, err = stream.ReadUint1(); err == nil {
								second, err = stream.ReadUint1()
							}
						}
					}
				}
				if err == nil && l == 12 {
					microSecond, err = stream.ReadUint4()
				}
				allMicroSeconds := int64(microSecond) + int64(second)*1000000 + int64(minute)*60*1000000 + int64(hour)*60*60*1000000 + int64(days)*24*60*60*1000000
				if isNegative != 0 {
					allMicroSeconds = allMicroSeconds * -1
				}
				ret, err = time.ParseDuration(fmt.Sprintf("%vus", allMicroSeconds))
			default:
				err = NewColumnValueTimeFromatError(int(l))
			}
		}
		return ret, err
	}("", "", "", nil, nil)
	readColumnValueFunc[ColumnTypeDatetime] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		//  0  0 0 0 0 0 0 0 => 000000 =       0 => 0000-00-00 00:00:00
		// 40 42 f 0 0 0 0 0 => 0F4240 = 1000000 => 0000-00-01 00:00:00
		// 80 84 1e 0 0 0 0 0 =>1E8480 = 2000000 => 0000-00-01 00:00:00
		// 40 4 fb b 0 0 0 0  => 0FFB0440 = 201000000 => 0000-02-01 00:00:00
		// 40 e8 6 60 2 0 0 0 => 026006e840 = 10201000000 => 0001-02-01 00:00:00
		// 80 cb 70 a9 89 29 0 0 => 2989a970cb80 = 45671230000000 => 4567-12-30 00:00:00
		// 77 87 d1 5 f1 5a 0 0 => 0x5AF105D18777 = 99991231235959 => 9999-12-31 23:59:59
		var ret ColumnValueTimeType
		val, err := stream.ReadUint8()
		if err == nil {
			ret = NewColumnValueTime(DateTimeFormat, Uint2(val/Uint8(10000000000)%Uint8(10000)), Uint1(val/Uint8(100000000)%Uint8(100)), Uint1(val/Uint8(1000000)%Uint8(100)), Uint1(val/Uint8(10000)%Uint8(100)), Uint1(val/Uint8(100)%Uint8(100)), Uint1(val%Uint8(100)), 0)
		}
		return ret, err
	}

	readColumnValueFunc[ColumnTypeYear] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// YEAR只允许从0～255 表示0000，1901～2155(5.5.62)
		var ret int16
		var err error
		var val Uint1
		if val, err = stream.ReadUint1(); err == nil {
			if val == 0 {
				ret = 0
			} else {
				ret = 1900 + int16(val)
			}
		}
		return ret, err
	}
	readColumnValueFunc[ColumnTypeNewDate] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		return nil, nil
	}
	readBytesInMaxBytes := func(maxBytes uint32, stream *Stream) ([]byte, error) {
		var len int
		var err error
		if maxBytes > 16777215 {
			var val Uint4
			val, err = stream.ReadUint4()
			len = int(val)
		} else if maxBytes > 65535 {
			var val Uint3
			val, err = stream.ReadUint3()
			len = int(val)
		} else if maxBytes > 255 {
			var val Uint2
			val, err = stream.ReadUint2()
			len = int(val)
		} else {
			var val Uint1
			val, err = stream.ReadUint1()
			len = int(val)
		}
		var buf []byte
		if err == nil {
			buf, _, err = stream.readNBytes(int64(len))
		}
		return buf, err
	}
	readStringInMaxBytes := func(maxBytes uint32, stream *Stream) (interface{}, error) {
		ret, err := readBytesInMaxBytes(maxBytes, stream)
		return string(ret), err
	}
	readColumnValueFunc[ColumnTypeVarchar] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// Varchar的meta表示最大字节数
		// meta中头2个字节是最大长度，决定用几个字节表示数据长度
		maxBytes := uint16(metaDef[0]) | (uint16(metaDef[1]) << 8)
		//fmt.Println("maxBytes in ColumnTypeVarchar=", maxBytes, uint16(metaDef[0]), metaDef[1], uint16(metaDef[1]), (uint16(metaDef[1]) << 8))
		return readStringInMaxBytes(uint32(maxBytes), stream)
	}
	//readColumnValueTypeString
	readColumnValueFunc[ColumnTypeBit] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// create table tbl_bit(val bit(50));   // ColumnDef:[16], ColumnMetaDef:[2 6], NullBitmask:[1]
		// create table tbl_bit2(val bit(49));  // ColumnDef:[16], ColumnMetaDef:[1 6], NullBitmask:[1]}
		// create table tbl_bit3(val bit(48));  // ColumnDef:[16], ColumnMetaDef:[0 6], NullBitmask:[1]}
		// bit的meta有2个字节，高字节表示有几个字节；高字节表示除了高字节外剩下的1个字节里占了多少位
		//fmt.Println(metaDef)
		len := metaDef[1] + (metaDef[0]+7)/8
		buf, _, err := stream.readNBytes(int64(len))
		return buf, err
	}
	//readColumnValueTypeBytes
	readColumnValueFunc[ColumnTypeTimestamp2] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// 在5.6.46中确认使用，4个字节。但是高位在前。比如1971-01-01 00:00:00对应的是31507200（0x1E0C300），取得的数据流是1 e0 c3 0
		var ret ColumnValueTimeType
		var err error
		if buf, _, err := stream.readNBytes(int64(4)); err == nil {
			int32Val := (Uint4(buf[0]) << 24) | (Uint4(buf[1]) << 16) | (Uint4(buf[2]) << 8) | Uint4(buf[3])
			if int32Val == 0 {
				ret = NewColumnValueTime(ZeroDateFormat, 0, 0, 0, 0, 0, 0, 0)
			} else {
				t := time.Unix(int64(int32Val), 0)
				ret = NewColumnValueTime(DateTimeFormat, Uint2(t.Year()), Uint1(t.Month()), Uint1(t.Day()), Uint1(t.Hour()), Uint1(t.Minute()), Uint1(t.Second()), 0)
			}
		}
		return ret, err
		//return nil, nil
	}
	readColumnValueFunc[ColumnTypeDatetime2] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// 年14位；月4位；日5位；时间9位
		// 80 0 0 0 0 = 0000-00-00 00:00:00
		// 80 0 2 0 0 = 0000-00-01 00:00:00
		// 80 0 4 0 0 = 0000-00-02 00:00:00
		// 80 0 82 0 0 = 0000-02-01 00:00:00   1000 0000 0000 0000 10   00 001   0 0000 0000
		// 80 3 c2 0 0 = 0001-02-01 00:00:00   1000 0000 0000 0011 11   00 001   0 0000 0000 + 13
		// b9 fd fc 0 0 = 4567-12-30 00:00:00  1011 1001 1111 1101 11   11 110   0 0000 0000
		// 80 0 0 0 0 = 0000-00-00 00:00:00
		// 80 7 2 0 0 = 0002-02-01 00:00:00    1000 0000 0000 0111 00   00 001   0 0000 0000
		// 80 d 82 0 0 = 0004-02-01 00:00:00   1000 0000 0000 1101 10   00 001   0 0000 0000
		// 80 1a 82 0 0 = 0008-02-01 00:00:00  1000 0000 0001 1010 10   00 001   0 0000 0000
		// 80 0 c2 0 0 = 0000-03-01 00:00:00   1000 0000 0000 0000 11   00 001   0 0000 0000
		// 80 1 2 0 0 =  0000-04-01 00:00:00   1000 0000 0000 0001 00   00 001   0 0000 0000
		// 80 2 2 0 0 =  0000-08-01 00:00:00   1000 0000 0000 0010 00   00 001   0 0000 0000
		// 80 0 0 0 0 = 0000
		// 80 3 2 0 0 = 0000-12-01 00:00:00    1000 0000 0000 0011 00   00 001   0 0000 0000
		// 80 3 82 0 0 = 0001-01-01 00:00:00   1000 0000 0000 0011 10   00 001   0 0000 0000
		// 80 0 42 0 0 = 0000-01-01 00:00:00   1000 0000 0000 0000 01   00 001   0 0000 0000
		// 8c b3 82 0 0 = 1000-06-01 00:00:00                 10 0011 0010 1100 1110   00 001   0 0000 0000
		// 8c b3 82 c7 ed = 1000-06-01 12:31:45       1100 0111 1110 1101
		// 1000 0000 0000 0000 0000 0000 0000 0000 0000 0000
		//   <- year and month -><day-><-      second     ->
		//                             <- H-> <- M -><- S ->
		// year / 13 = 真实的年；year%13=月（包含0）
		var err error
		var buf []byte
		var ret ColumnValueTimeType
		if buf, _, err = stream.readNBytes(int64(5)); err == nil {
			second := buf[4] & 0x3f
			minute := (buf[4] >> 6) | ((buf[3] & 0x0f) << 2)
			hour := ((buf[3] & 0xf0) >> 4) | ((buf[2] & 0x01) << 4)
			day := (buf[2] & 0x3e) >> 1
			yearAndMonth := (int32(buf[0]&0x3f) << 10) | (int32(buf[1]) << 2) | int32((buf[2]&0xc0)>>6)
			year := yearAndMonth / 13
			month := yearAndMonth % 13

			ret = NewColumnValueTime(DateTimeNanoFormat, Uint2(year), Uint1(month), Uint1(day), Uint1(hour), Uint1(minute), Uint1(second), 0)
		}
		return ret, err
	}
	readColumnValueFunc[ColumnTypeTime2] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// 80 0 0  = 00:00:00
		// 80 0 1  = 00:00:01
		// b4 6e fb = 838:59:59
		// 80 0 40 = 00:01:00
		// 80 10 0  = 01:00:00
		// a0 0 0 = 512:00:00
		// 4b 91 5 = -838:59:59
		// 正数最高位1，原码表示；负数最高位是0补码表示
		// buf[0] ~ buf[1]高4位：小时
		// buf[1]低4位～buf[2]高2位：分
		// buf[2]低6位：秒
		var buf []byte
		var ret time.Duration
		var err error
		if buf, _, err = stream.readNBytes(int64(3)); err == nil {
			plus := -1
			if buf[0]&0x80 != 0 {
				plus = +1
			}
			var val int32
			if plus == +1 {
				val = (int32(buf[0]&0x7F) << 16) | (int32(buf[1]) << 8) | int32(buf[2])
			} else {
				val2 := (uint32(0xFF) << 24) | (uint32(buf[0]|0x80) << 16) | (uint32(buf[1]) << 8) | uint32(buf[2])
				val = int32(val2)
				val = -val
			}
			fmt.Println("step1, val=", val)
			hour := val >> 12
			minute := (val & 0x00000FC0) >> 6
			second := val & 0x0000003F
			fmt.Println("hour=", plus*int(hour), " minute=", minute, " second=", second)
			ret, err = time.ParseDuration(fmt.Sprintf("%vh%vm%vs", plus*int(hour), minute, second))
		}
		return ret, err

	}
	//func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
	//buf, _, err := stream.readNBytes(int64(3));
	//return buf, err
	//}
	readColumnValueFunc[ColumnTypeNewDecimal] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// https://www.mysqltutorial.org/mysql-decimal/
		// 整数和小数部分分开
		// 整数部分从右向左9个十进制字符打包成4字节，最后剩下的几位打包（0～4字节），最高位符号？
		// 小数部分从左向右9个十进制字符打包成4字节，最后剩下的几位打包（0～4字节）
		// decimal的meta包含2个字节，对应Decimal(M, D)中的M与D
		// 先取得多少个整数位，多少个小数位

		// 10进制数的位数对应的2进制的字节数
		digitsRequireBytes := [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
		// 整数部分和小数部分分别需要读多少段
		mDigits := ((metaDef[0] - metaDef[1]) + 8) / 9
		dDigits := (metaDef[1] + 8) / 9
		var err error
		var mBuf = bytes.NewBufferString("")
		var dBuf = bytes.NewBufferString("")
		digits := [2]byte{mDigits, dDigits}
		buffers := [2]*bytes.Buffer{mBuf, dBuf}
		var mAllZero = true
		var dAllZero = true
		var sign bool // true表示正数、false表示负数

		for i := range digits {
			buffer := buffers[i]
			for j := byte(0); j < digits[i]; j++ {
				var n int
				// 转成十进制后的位数
				decLength := 9
				if i == 0 && j == 0 {
					// 整数部分第一段
					decLength = int((metaDef[0] - metaDef[1]) % 9)
					n = digitsRequireBytes[decLength]
				} else if i == 1 && j == dDigits-1 {
					// 小数部分最后一段
					decLength = int(metaDef[1] % 9)
					n = digitsRequireBytes[int(decLength)]
					//lastFraction = true
				} else {
					n = 4
				}
				if decLength == 0 {
					decLength = 9
				}
				if n == 0 {
					n = 4
				}
				var buf []byte
				if buf, _, err = stream.readNBytes(int64(n)); err == nil {
					if i == 0 && j == 0 {
						// bytes[0] & 0x80 最高位是符号位？
						sign = (buf[0] & 0x80) != byte(0)
					}
					if !sign {
						for k := range buf {
							buf[k] = ^buf[k]
						}
					}
					buf[0] = buf[0] & 0x7F
					if n != 4 {
						// 在前面补0
						newBytes := make([]byte, 4)
						for k := 0; k < n; k++ {
							newBytes[4-n+k] = buf[k]
						}
						buf = newBytes
					}
					// 这里有点特殊，变成了高位在前
					var xx int32
					if err = binary.Read(bytes.NewBuffer(buf), binary.BigEndian, &xx); err != nil {
						break
					}
					if i == 0 {
						mAllZero = mAllZero && (xx == int32(0))
					} else {
						dAllZero = dAllZero && (xx == int32(0))
					}
					// 小数部分最后一段不需要补0
					format := fmt.Sprintf("%%0%dd", decLength)
					//if lastFraction {
					//format = "%0d"
					//}
					buffer.WriteString(fmt.Sprintf(format, xx))
				} else {
					// 出错
				}
			}
		}
		var ret string
		if mAllZero && dAllZero {
			ret = "0"
		} else {
			if dAllZero {
				ret = strings.TrimLeft(mBuf.String(), "0")
			} else if mAllZero {
				ret = "0." + strings.TrimRight(dBuf.String(), "0")
			} else {
				ret = mBuf.String() + "." + dBuf.String()
				ret = strings.Trim(ret, "0")
			}
			if !sign {
				ret = "-" + ret
			}
		}
		return ret, err
	}
	readColumnValueFunc[ColumnTypeEnum] = readColumnValueTypeBytes
	readColumnValueFunc[ColumnTypeSet] = readColumnValueTypeBytes
	readColumnValueFunc[ColumnTypeTinyBlob] = readColumnValueTypeBytes
	readColumnValueFunc[ColumnTypeMediumBlob] = readColumnValueTypeBytes
	readColumnValueFunc[ColumnTypeLongBlob] = readColumnValueTypeBytes
	readColumnValueFunc[ColumnTypeBlob] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// blob的meta表示几个字节表示长度
		maxBytes := uint32((uint64(1) << (8 * metaDef[0])) - 1)
		return readBytesInMaxBytes(maxBytes, stream)
	}
	readColumnValueFunc[ColumnTypeVarString] = readColumnValueTypeBytes
	readColumnValueFunc[ColumnTypeString] = func(schema, table, column string, metaDef []byte, stream *Stream) (interface{}, error) {
		// 根据meta取得最大长度。如果长度>255，则2位字节表示数据长度；如果<=255，则用一位表示字节
		// 但char型有个bug
		// https://bugs.mysql.com/bug.php?id=37426
		// 编码：https://lists.mysql.com/commits/48757
		// SET类型也会用到这个函数(5.5.62)，但它的格式又和char不一样。char会在meta中标明最大字节数，反推用几个字节表示长度，再读长度，再读数据。
		// SET型直接根据meta判断最长占几个字节
		// 先区分这是个char，还是个SET。（也可以通过metaDef[0]来判断。String=254, Set=248, enum=247
		//if _, ok := stream.serverConfig.ColumnSetTypeValues[schema][table][column]; ok{
		if metaDef[0] == byte(ColumnTypeSet) || metaDef[0] == byte(ColumnTypeEnum) {
			// 是个set或enum。metaDef[1]表示实际的字节数
			buf, _, err := stream.readNBytes(int64(metaDef[1]))
			var ret []ColumnValueSetType
			if err == nil {
				ret = make([]ColumnValueSetType, 0)
				for i := range buf {
					for j := 0; j < 8; j++ {
						index := i*8 + j
						m := 1 << j
						if buf[i]&byte(m) != 0 {
							val := NewColumnValueSet()
							val.Index = index
							ret = append(ret, val)
						}
					}
				}
			}
			return ret, err
		} else {
			// 是个string
			if stream.serverConfig.compareVersion("5.6.0") >= 0 {
				// 5.6.46时，STRING要maxByte决定byte长度占几个字节，后面是把字节数+长度byte的方式
				// 总长170(utf8)时，总字节数510=       01 1111 1110，meta = EE FE  1110 1110 1111 1110
				// 总长255(utf8)时，总字节数765=       10 1111 1101，meta = DE FD  1101 1110 1111 1101
				// 总长60(utf8_bing)时，总字节数180=   00 1011 0100，meta = FE B4  1111 1110 1011 0100
				// 总长255(utf8mb4)时，最大字节数1020= 11 1111 1100，meta = CE FC  1100 1110 1111 1100
				// 结论：用meta[0]的低5、6位求反+meta[1]表示最大长度
				maxBytes := (uint16(^(metaDef[0])&0x30) << 4) | uint16(metaDef[1])
				lengthBytes := 1
				if maxBytes > 255 {
					lengthBytes = 2
				}
				var length int
				var err error
				if lengthBytes == 1 {
					var n Uint1
					if n, err = stream.ReadUint1(); err == nil {
						length = int(n)
					}
				} else if lengthBytes == 2 {
					var n Uint2
					if n, err = stream.ReadUint2(); err == nil {
						length = int(n)
					}
				}
				if err == nil {
					var str StringFix
					str, err = stream.ReadStringFix(length)
					//fmt.Println("Read String From stream>=5.6.46, length=", length, " str=", str)
					return string(str), err
				} else {
					return nil, err
				}
			} else {
				maxBytes := ((uint16(metaDef[0]&0x30 ^ 0x30)) << 8) | uint16(metaDef[1])
				return readStringInMaxBytes(uint32(maxBytes), stream)
			}
		}
	}
	readColumnValueFunc[ColumnTypeGeometry] = readColumnValueTypeBytes
}

func (this *Stream) readColumnValue(schema, table, column string, columnType ColumnType, metaDef []byte, isNul bool) (ColumnValueType, error) {
	ret := NewColumnValue(columnType, isNul)
	var err error
	if !isNul {
		if f, ok := readColumnValueFunc[columnType]; ok {
			ret.value, err = f(schema, table, column, metaDef, this)
		} else {
			// TODO 没有处理这个类型
			err = Error{fmt.Sprintf("Cannot process Schema=%v Table=%v Column=%v Type=%v", schema, table, column, columnType), 0}
		}
	}
	return ret, err
}
func countMask(bytes []byte, n int) int {
	c := 0
	b := (n + 7) / 8
	for i := 0; i < b; i++ {
		m := 0x01
		for j := 0; (j < 8 && i < b-1) || j < (n-8*(b-1)); j++ {
			if (bytes[i] & byte(m<<j)) != 0 {
				c++
			}
		}
	}
	return c
}

type createEventFuncType func(int, EventHeaderType, *Stream) (interface{}, error)

var createEventFuncs map[Uint1]createEventFuncType

func init() {
	createEventFuncs = make(map[Uint1]createEventFuncType, 30)
	// 0x00 UNKNOWN_EVENT（忽略）
	createEventFuncs[EventTypeUnknownEvent] = func(int, EventHeaderType, *Stream) (interface{}, error) {
		panic("UNKNOWN_EVENT")
		return nil, nil
	}
	// START_EVENT_V3（只适用于binlog-vertion=1~3，所以这里忽略）
	createEventFuncs[EventTypeStartEventV3] = func(int, EventHeaderType, *Stream) (interface{}, error) {
		panic("START_EVENT_V3")
		return nil, nil
	}
	// QUERY_EVENT
	createEventFuncs[EventTypeQueryEvent] = func(length int, eventHeader EventHeaderType, stream *Stream) (interface{}, error) {
		ret := QueryEventType{}
		var err error
		if ret.SlaveProxyId, err = stream.ReadUint4(); err == nil {
			if ret.ExecutionTime, err = stream.ReadUint4(); err == nil {
				if ret.SchemaLength, err = stream.ReadUint1(); err == nil {
					if ret.ErrorCode, err = stream.ReadUint2(); err == nil {
						if stream.serverConfig.BinlogVersion >= 4 {
							if ret.StatusVarsLength, err = stream.ReadUint2(); err == nil {
								// 读StatusVars
								ret.StatusVars = make(StatusVarsType, 0)
								for end := stream.byteReadCounter + int64(ret.StatusVarsLength); stream.byteReadCounter < end; {
									if key, err := stream.ReadUint1(); err == nil {
										statusVar := NewStatusVarType()
										var n Uint1
										switch key {
										case Q_FLAGS2_CODE:
											statusVar.uint4Val, err = stream.ReadUint4()

										case Q_SQL_MODE_CODE:
											statusVar.uint8Val, err = stream.ReadUint8()

										case Q_CATALOG:
											if n, err = stream.ReadUint1(); err == nil {
												if statusVar.bytesVal, _, err = stream.readNBytes(int64(n)); err == nil {
													if n, err = stream.ReadUint1(); err == nil {
														if n != 0 {
															err = NewQueryEventStatusVarError("Q_CATALOG format error")
														}
													}
												}
											}

										case Q_AUTO_INCREMENT:
											// 未验证
											statusVar.uint2Val1, err = stream.ReadUint2()
											statusVar.uint2Val2, err = stream.ReadUint2()

										case Q_CHARSET_CODE:
											statusVar.uint2Val1, err = stream.ReadUint2()
											statusVar.uint2Val2, err = stream.ReadUint2()
											statusVar.uint2Val3, err = stream.ReadUint2()

										case Q_TIME_ZONE_CODE:
											// 在创建timestamp时，会查询时区
											if n, err = stream.ReadUint1(); err == nil {
												statusVar.stringFixVal1, err = stream.ReadStringFix(int(n))
											}

										case Q_CATALOG_NZ_CODE:
											if n, err = stream.ReadUint1(); err == nil {
												statusVar.stringFixVal1, err = stream.ReadStringFix(int(n))
											}

										case Q_LC_TIME_NAMES_CODE:
											// 未验证
											statusVar.uint2Val1, err = stream.ReadUint2()

										case Q_CHARSET_DATABASE_CODE:
											// 未验证
											statusVar.uint2Val1, err = stream.ReadUint2()

										case Q_TABLE_MAP_FOR_UPDATE_CODE:
											// 未验证
											statusVar.uint8Val, err = stream.ReadUint8()

										case Q_MASTER_DATA_WRITTEN_CODE:
											// 未验证
											statusVar.uint4Val, err = stream.ReadUint4()

										case Q_INVOKERS:
											if n, err = stream.ReadUint1(); err == nil {
												if statusVar.stringFixVal1, err = stream.ReadStringFix(int(n)); err == nil {
													if n, err = stream.ReadUint1(); err == nil {
														statusVar.stringFixVal2, err = stream.ReadStringFix(int(n))
													}
												}
											}

										case Q_UPDATED_DB_NAMES:
											// 在5.6.46中CREATE TABLE IF NOT EXIS&……中调用。在初始化DB时会产生
											if n, err = stream.ReadUint1(); err == nil {
												statusVar.updatedDbNames = make([]StringNul, 0)
												for i := 0; i < int(n); i++ {
													if updatedDbName, updatedDbNameErr := stream.ReadStringNul(); updatedDbNameErr != nil {
														err = updatedDbNameErr
														break

													} else {
														statusVar.updatedDbNames = append(statusVar.updatedDbNames, updatedDbName)
													}
												}
											}

										case Q_MICROSECONDS:
											// 未验证
											statusVar.uint3Val, err = stream.ReadUint3()
										}
										// 判断在处理这个value时有没有产生错误
										if err != nil {
											break
										}
										ret.StatusVars[key] = statusVar
									} else {
										// 读key时出了error
										break
									}
								}
							}
						}
						if err == nil {
							// 读Schema
							if ret.Schema, err = stream.ReadStringFix(int(ret.SchemaLength)); err == nil {
								if _, err = stream.ReadUint1(); err == nil {
									ret.Query, err = stream.ReadStringEof(length)
								}
							}
						}
					}
				}
			}
		}
		return ret, err
	}
	// STOP_EVENT。发生在master shutdown，或者RESET slave时
	createEventFuncs[EventTypeStopEvent] = func(int, EventHeaderType, *Stream) (interface{}, error) {
		return NewStopEventType(), nil
	}
	// ROTATE_EVENT。binlog中的最后一个event
	createEventFuncs[EventTypeRotateEvent] = func(packetLength int, eventHeader EventHeaderType, stream *Stream) (interface{}, error) {
		ret := NewRotateEventType()
		var err error
		if stream.serverConfig.BinlogVersion > 1 {
			ret.Position, err = stream.ReadUint8()
		}
		if err == nil {
			ret.Name, err = stream.ReadStringEof(packetLength)
		}
		return ret, err
	}
	// INTVAR_EVENT
	createEventFuncs[EventTypeIntvarEvent] = func(_ int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewIntvarEvent()
		var err error
		if ret.Type, err = stream.ReadUint1(); err == nil {
			ret.Value, err = stream.ReadUint8()
		}
		return ret, err
	}
	// LOAD_EVENT
	createEventFuncs[EventTypeLoadEvent] = func(_ int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewLoadEvent()
		var err error
		if ret.SlaveProxyId, err = stream.ReadUint4(); err == nil {
			if ret.ExecTime, err = stream.ReadUint4(); err == nil {
				if ret.SkipLines, err = stream.ReadUint4(); err == nil {
					if ret.TableNameLen, err = stream.ReadUint1(); err == nil {
						if ret.SchemaLen, err = stream.ReadUint1(); err == nil {
							if ret.NumFields, err = stream.ReadUint4(); err == nil {
								if ret.FieldTerm, err = stream.ReadUint1(); err == nil {
									if ret.EnclosedBy, err = stream.ReadUint1(); err == nil {
										if ret.LineTerm, err = stream.ReadUint1(); err == nil {
											if ret.LintStart, err = stream.ReadUint1(); err == nil {
												if ret.EscapedBy, err = stream.ReadUint1(); err == nil {
													if ret.OptFlags, err = stream.ReadUint1(); err == nil {
														if ret.EmptyFlags, err = stream.ReadUint1(); err == nil {
															ret.FieldNameLengths = make([]Uint1, 0)
															var l Uint1
															for i := 0; i < int(ret.NumFields); i++ {
																if l, err = stream.ReadUint1(); err != nil {
																	break
																}
																ret.FieldNameLengths = append(ret.FieldNameLengths, l)
															}

															ret.FieldNames = make([]StringNul, 0)
															var fieldName StringNul
															for i := 0; i < int(ret.NumFields); i++ {
																if fieldName, err = stream.ReadStringNul(); err != nil {
																	break
																}
																ret.FieldNames = append(ret.FieldNames, fieldName)
															}

															if err == nil {
																if ret.TableName, err = stream.ReadStringNul(); err == nil {
																	if ret.SchemaName, err = stream.ReadStringNul(); err == nil {
																		ret.FileName, err = stream.ReadStringNul()
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return ret, err
	}
	// SLAVE_EVENT 忽略
	createEventFuncs[EventTypeSlaveEvent] = func(int, EventHeaderType, *Stream) (interface{}, error) {
		// 未验证
		return nil, nil
	}
	// CREATE_FILE_EVENT
	createEventFuncs[EventTypeCreateFileEvent] = func(packetLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewCreateFileEvent()
		var err error
		if ret.FileId, err = stream.ReadUint4(); err == nil {
			ret.BlockData, err = stream.ReadStringEof(packetLength)
		}
		return ret, err
	}
	// APPEND_BLOCK_EVENT
	createEventFuncs[EventTypeAppendBlockEvent] = func(packetLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewAppendBlockEvent()
		var err error
		if ret.FileId, err = stream.ReadUint4(); err == nil {
			ret.BlockData, err = stream.ReadStringEof(packetLength)
		}
		return ret, err
	}
	// EXEC_LOAD_EVENT
	createEventFuncs[EventTypeExecLoadEvent] = func(_ int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewExecLoadEvent()
		var err error
		ret.FileId, err = stream.ReadUint4()
		return ret, err
	}
	// DELETE_FILE_EVENT
	createEventFuncs[EventTypeDeleteFileEvent] = func(_ int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewDeleteFileEvent()
		var err error
		ret.FileId, err = stream.ReadUint4()
		return ret, err
	}
	// NEW_LOAD_EVENT
	createEventFuncs[EventTypeNewLoadEvent] = func(_ int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewNewLoadEventType()
		var err error
		if ret.SlaveProxyId, err = stream.ReadUint4(); err != nil {
			goto EXIT
		}
		if ret.ExecutionTime, err = stream.ReadUint4(); err != nil {
			goto EXIT
		}
		if ret.SkipLines, err = stream.ReadUint4(); err != nil {
			goto EXIT
		}
		if ret.TableNameLen, err = stream.ReadUint1(); err != nil {
			goto EXIT
		}
		if ret.SchemaLen, err = stream.ReadUint1(); err != nil {
			goto EXIT
		}
		if ret.NumFields, err = stream.ReadUint4(); err != nil {
			goto EXIT
		}
		if ret.FieldTermLen, err = stream.ReadUint1(); err != nil {
			goto EXIT
		}
		if ret.FieldTerm, _, err = stream.readNBytes(int64(ret.FieldTermLen)); err != nil {
			goto EXIT
		}
		if ret.EnclosedByLen, err = stream.ReadUint1(); err != nil {
			goto EXIT
		}
		if ret.EnclosedBy, _, err = stream.readNBytes(int64(ret.EnclosedByLen)); err != nil {
			goto EXIT
		}
		if ret.LineTermLen, err = stream.ReadUint1(); err != nil {
			goto EXIT
		}
		if ret.LineTerm, _, err = stream.readNBytes(int64(ret.LineTermLen)); err != nil {
			goto EXIT
		}
		if ret.LineStartLen, err = stream.ReadUint1(); err != nil {
			goto EXIT
		}
		if ret.LineStart, _, err = stream.readNBytes(int64(ret.LineStartLen)); err != nil {
			goto EXIT
		}
		if ret.EscapedByLen, err = stream.ReadUint1(); err != nil {
			goto EXIT
		}
		if ret.EscapedBy, _, err = stream.readNBytes(int64(ret.EscapedByLen)); err != nil {
			goto EXIT
		}
		if ret.OptFlags, err = stream.ReadUint1(); err != nil {
			goto EXIT
		}
		if ret.FieldNameLengths, _, err = stream.readNBytes(int64(ret.NumFields)); err != nil {
			goto EXIT
		}
		ret.FieldNames = make([]StringNul, 0)
		for i := 0; i < int(ret.NumFields); i++ {
			var fieleName StringNul
			if fieleName, err = stream.ReadStringNul(); err != nil {
				goto EXIT
			}
			ret.FieldNames = append(ret.FieldNames, fieleName)
		}
		if ret.TableName, err = stream.ReadStringNul(); err != nil {
			goto EXIT
		}
		if ret.SchemaName, err = stream.ReadStringNul(); err != nil {
			goto EXIT
		}
		ret.FileName, err = stream.ReadStringNul()
	EXIT:
		return ret, err
	}
	// RAND_EVENT
	createEventFuncs[EventTypeRandEvent] = func(_ int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewRandEvent()
		var err error
		if ret.Seed1, err = stream.ReadUint8(); err == nil {
			ret.Seed2, err = stream.ReadUint8()
		}
		return ret, err
	}
	// USER_VAR_EVENT
	createEventFuncs[EventTypeUserVarEvent] = func(packetLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewUserVarEvent()
		var err error
		if ret.NameLength, err = stream.ReadUint4(); err == nil {
			if ret.Name, err = stream.ReadStringFix(int(ret.NameLength)); err == nil {
				if ret.IsNull, err = stream.ReadUint1(); err == nil {
					if ret.IsNull == Uint1(0) {
						if ret.Type, err = stream.ReadUint1(); err == nil {
							if ret.Charset, err = stream.ReadUint4(); err == nil {
								if ret.ValueLength, err = stream.ReadUint4(); err == nil {
									if ret.Value, _, err = stream.readNBytes(int64(ret.ValueLength)); err == nil {
										if stream.byteReadCounter < int64(packetLength) {
											ret.Flags, err = stream.ReadUint1()
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return ret, err
	}
	// 0x0F FORMAT_DESCRIPTION_EVENT
	createEventFuncs[EventTypeFormatDescriptionEvent] = func(eventLength int, eventHeader EventHeaderType, stream *Stream) (interface{}, error) {
		ret := NewFormatDescriptionEventType()
		var err error
		if ret.BinlogVersion, err = stream.ReadUint2(); err == nil {
			if ret.MysqlServerVersion, err = stream.ReadStringFix(50); err == nil {
				if ret.CreateTimestamp, err = stream.ReadUint4(); err == nil {
					if ret.EventHeaderLength, err = stream.ReadUint1(); err == nil {
						//fmt.Println("eventLength=%v  byteReadCounter=%v", eventLength, stream.byteReadCounter)
						if bytes, _, err := stream.readNBytes(int64(eventLength) - stream.byteReadCounter); err == nil {
							//fmt.Println("FORMAT_DESCRIPTION_EVENT bytes=", len(bytes))
							// 注意，这里从服务器返回的header length和服务器版本有关。所以在低版本的mysql上有可能部分event的header length是没有的
							// 如在5.5.62上就没有DELETE_ROWS_EVENTv2/UPDATE_ROWS_EVENTv2/WRITE_ROWS_EVENTv2
							ret.EventTypeHeaderLength = bytes
						}
					}
				}
			}
		}
		return ret, err
	}
	// XID_EVENT. Written whenever a COMMIT expected
	createEventFuncs[EventTypeXidEvent] = func(_ int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		ret := NewXIDEvent()
		var err error
		ret.Xid, err = stream.ReadUint8()
		return ret, err
	}
	// BEGIN_LOAD_QUERY_EVENT
	createEventFuncs[EventTypeBeginLoadQueryEvent] = func(packetLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewBeginLoadQueryEvent()
		var err error
		if ret.FileId, err = stream.ReadUint4(); err == nil {
			ret.BlockData, err = stream.ReadStringEof(packetLength)
		}
		return ret, err
	}
	// EXECUTE_LOAD_QUERY_EVENT
	createEventFuncs[EventTypeExecuteLoadQueryEvent] = func(_ int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewExecuteLoadQueryEvent()
		var err error
		if ret.SlaveProxyId, err = stream.ReadUint4(); err == nil {
			if ret.ExecutionTime, err = stream.ReadUint4(); err == nil {
				if ret.SchemaLength, err = stream.ReadUint1(); err == nil {
					if ret.ErrorCode, err = stream.ReadUint2(); err == nil {
						if ret.StatusVarsLength, err = stream.ReadUint2(); err == nil {
							if ret.FileId, err = stream.ReadUint4(); err == nil {
								if ret.StartPos, err = stream.ReadUint4(); err == nil {
									if ret.EndPos, err = stream.ReadUint4(); err == nil {
										ret.DupHandlingFlags, err = stream.ReadUint1()
									}
								}
							}
						}
					}
				}
			}
		}
		return ret, err
	}
	// TABLE_MAP_EVENT。用于Row based replication中描述表的
	createEventFuncs[EventTypeTableMapEvent] = func(_ int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		ret := NewTableMapEvent()
		var err error
		if stream.serverConfig.EventTypeHeaderLength[EventTypeTableMapEvent-1] == 6 {
			var tableId Uint4
			tableId, err = stream.ReadUint4()
			ret.TableId = Uint6(tableId)
		} else {
			ret.TableId, err = stream.ReadUint6()
		}
		if err == nil {
			if ret.Flags, err = stream.ReadUint2(); err == nil {
				if ret.SchemaNameLength, err = stream.ReadUint1(); err == nil {
					if ret.SchemaName, err = stream.ReadStringFix(int(ret.SchemaNameLength)); err == nil {
						if ret.Filler1, err = stream.ReadUint1(); err == nil {
							if ret.TableNameLength, err = stream.ReadUint1(); err == nil {
								if ret.TableName, err = stream.ReadStringFix(int(ret.TableNameLength)); err == nil {
									if ret.Filler2, err = stream.ReadUint1(); err == nil {
										if ret.ColumnCount, err = stream.ReadUintLenenc(); err == nil {
											var bytes []byte
											if bytes, _, err = stream.readNBytes(int64(ret.ColumnCount)); err == nil {
												ret.ColumnDef = make([]ColumnType, 0)
												for i := range bytes {
													ret.ColumnDef = append(ret.ColumnDef, ColumnType(bytes[i]))
												}
												var metaDefLength UintLenenc
												if metaDefLength, err = stream.ReadUintLenenc(); err == nil {
													if metaDefLength > 0 {
														// 先不处理。见columnMetaDefLength的定义
														ret.ColumnMetaDef, _, err = stream.readNBytes(int64(metaDefLength))
													}
													if err == nil {
														ret.NullBitmask, _, err = stream.readNBytes(int64(ret.ColumnCount+7) / 8) // 这里文档写错了。文档上写的是“+7)/8”
														// 在5.6.46中，查看log_event.h，这里多一个字段，叫m_meta_memory，但意义不明。这里应该消耗掉
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return ret, err
	}
	readRowColumnValues := func(schema, table, column string, nulBitMap []byte, tableMapEvent TableMapEventType, stream *Stream) ([]ColumnValueType, error) {
		var err error
		var val ColumnValueType
		ret := make([]ColumnValueType, 0)
		columnTypes := tableMapEvent.ColumnDef // 这个ColumnType，是无法区分CHAR与SET的……
		columnMetaDef := tableMapEvent.ColumnMetaDef
		metaDefPosition := 0
		for i := range columnTypes {
			isNul := (nulBitMap[i/8] & (byte(0x01) << (i % 8))) != 0
			if metaDefLength, ok := columnMetaDefLength[columnTypes[i]]; ok {
				metaDef := columnMetaDef[metaDefPosition : metaDefPosition+metaDefLength]
				if val, err = stream.readColumnValue(schema, table, column, columnTypes[i], metaDef, isNul); err == nil {
					ret = append(ret, val)
					metaDefPosition = metaDefPosition + metaDefLength
				} else {
					break
				}
			} else {
				// TODO 这个长度没指定
				err = Error{fmt.Sprintf("Donot konw metaDefLength for %v", columnTypes[i]), 0}
				break
			}
		}
		return ret, err
	}
	createRowEvent := func(eventType Uint1, rowEventVersion Uint1, payloadLength int, stream *Stream) (interface{}, error) {
		ret := NewRowsEvent(rowEventVersion, eventType)
		var err error
		if stream.serverConfig.EventTypeHeaderLength[eventType-1] == 6 {
			var tableId Uint4
			tableId, err = stream.ReadUint4()
			ret.TableId = Uint6(tableId)
		} else {
			ret.TableId, err = stream.ReadUint6()
		}
		if err == nil {
			if ret.Flags, err = stream.ReadUint2(); err == nil {
				if rowEventVersion == Uint1(2) {
					if ret.ExtraDataLength, err = stream.ReadUint2(); err == nil {
						if ret.ExtraDataLength-2 > 0 {
							ret.ExtraData, _, err = stream.readNBytes(int64(ret.ExtraDataLength) - 2) // 根据文档这里要减2
						}
					}
				}
				if err == nil {
					if ret.NumberOfColumns, err = stream.ReadUintLenenc(); err == nil {
						ret.ColumnsPresentBitmap1, _, err = stream.readNBytes(int64((ret.NumberOfColumns)+7) / 8)
						if err == nil && (eventType == EventTypeUpdateRowsEventv1 || eventType == EventTypeUpdateRowsEventv2) {
							ret.ColumnsPresentBitmap2, _, err = stream.readNBytes(int64((ret.NumberOfColumns)+7) / 8)
						}
					}
					if err == nil {
						// 根据tableId取得table
						var schema string
						var table string
						if tableMap, ok := stream.serverConfig.TableMaps[Uint8(ret.TableId)]; !ok {
							// 这个表未定义？
							dummy, _ := stream.ReadStringEof(payloadLength)
							fmt.Println("Dummy data=", dummy)
							return ret, nil
						} else {
							schema = string(tableMap.SchemaName)
							table = string(tableMap.TableName)
							// 检查tableMap各列。如果有不支持的列，直接把剩下的数据读完
							for _, columnType := range tableMap.ColumnDef {
								if columnType == ColumnTypeGeometry {
									//fmt.Println("Unsupported Column=", columnType, " in table ", tableMap.SchemaName, ".", tableMap.TableName)
									dummy, _ := stream.ReadStringEof(payloadLength)
									fmt.Println("Dummy data=", dummy)
									return ret, nil
								}
							}
						}

						columnCount := 0
						ret.Rows = make([]RowsEventRowType, 0)
						for {
							if !stream.moreDataInPayload(payloadLength) {
								break
							}
							rowsEventRow := NewRowsEventRow()
							// TODO:这里要改的地方很多
							columnAttr := stream.serverConfig.getColumnAt(schema, table, columnCount)
							var column string
							if columnAttr != nil {
								column = columnAttr.Name
							}
							// 这里这么麻烦，是因为sqlparser遇到无法识别的列（如geometry）会无法解析整个sql
							//if tables, ok := stream.serverConfig.ColumnNames[schema]; ok{
							//	if columns, ok := tables[table]; ok{
							//		column = columns[columnCount]
							//	}
							//}
							//column = stream.serverConfig.ColumnNames[schema][table][columnCount]

							// 计算在columns-present-bitmap1中有几位
							bitCount := countMask(ret.ColumnsPresentBitmap1, int(ret.NumberOfColumns))
							//fmt.Println("bitCount=", bitCount)
							if rowsEventRow.NulBitmap1, _, err = stream.readNBytes(int64((bitCount + 7) / 8)); err == nil {
								//fmt.Println("rowsEventRow.NulBitmap1=", rowsEventRow.NulBitmap1)
								// 读入各字段的value
								var tableMapEvent TableMapEventType
								var ok bool
								if tableMapEvent, ok = stream.serverConfig.TableMaps[Uint8(ret.TableId)]; ok {
									rowsEventRow.Value1, err = readRowColumnValues(schema, table, column, rowsEventRow.NulBitmap1, tableMapEvent, stream)
								} else {
									// TODO 这个表的结构未知
								}

								if err == nil && (eventType == EventTypeUpdateRowsEventv1 || eventType == EventTypeUpdateRowsEventv2) {
									bitCount = countMask(ret.ColumnsPresentBitmap2, int(ret.NumberOfColumns))
									if rowsEventRow.NulBitmap2, _, err = stream.readNBytes(int64((bitCount + 7) / 8)); err == nil {
										// 读入各字段的value
										rowsEventRow.Value2, err = readRowColumnValues(schema, table, column, rowsEventRow.NulBitmap2, tableMapEvent, stream)
									} else {
										// TODO 这个表的结构未知
									}
								}
							}
							if err != nil {
								break
							}
							ret.Rows = append(ret.Rows, rowsEventRow)

						}
					}
				}
			}
		}
		return ret, err
	}
	// WRITE_ROWS_EVENTv0
	createEventFuncs[EventTypeWriteRowsEventv0] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		return createRowEvent(EventTypeWriteRowsEventv0, 0, payloadLength, stream)
	}
	// UPDATE_ROWS_EVENTv0
	createEventFuncs[EventTypeUpdateRowsEventv0] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		return createRowEvent(EventTypeUpdateRowsEventv0, 0, payloadLength, stream)
	}
	// DELETE_ROWS_EVENTv0
	createEventFuncs[EventTypeDeleteRowsEventv0] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		return createRowEvent(EventTypeDeleteRowsEventv0, 0, payloadLength, stream)
	}
	// WRITE_ROWS_EVENTv1
	createEventFuncs[EventTypeWriteRowsEventv1] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		return createRowEvent(EventTypeWriteRowsEventv1, 1, payloadLength, stream)
	}
	// UPDATE_ROWS_EVENTv1
	createEventFuncs[EventTypeUpdateRowsEventv1] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		return createRowEvent(EventTypeUpdateRowsEventv1, 1, payloadLength, stream)
	}
	// DELETE_ROWS_EVENTv1
	createEventFuncs[EventTypeDeleteRowsEventv1] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		return createRowEvent(EventTypeDeleteRowsEventv1, 1, payloadLength, stream)
	}
	// INCIDENT_EVENT
	createEventFuncs[EventTypeIncidentEvent] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewIncidentEvent()
		var err error
		if ret.Type, err = stream.ReadUint2(); err == nil {
			if ret.MessageLength, err = stream.ReadUint1(); err == nil {
				ret.Message, err = stream.ReadStringFix(int(ret.MessageLength))
			}
		}
		return ret, err
	}
	// HEARTBEAT_EVENT。在replication一定空闲时间后会产生这个event
	createEventFuncs[EventTypeHeartBeatEvent] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		return nil, nil
	}
	// 忽略
	createEventFuncs[EventTypeIgnorableEvent] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		return nil, nil
	}
	// ROW_QUERY_EVENT
	createEventFuncs[EventTypeRowsQueryEvent] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		ret := NewRowsQueryEvent()
		var err error
		if ret.Length, err = stream.ReadUint1(); err == nil {
			ret.QueryText, err = stream.ReadStringEof(payloadLength)
		}
		return ret, err
	}
	// WRITE_ROWS_EVENTv2
	createEventFuncs[EventTypeWriteRowsEventv2] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		return createRowEvent(EventTypeWriteRowsEventv2, 2, payloadLength, stream)
	}
	// UPDATE_ROWS_EVENTv2
	createEventFuncs[EventTypeUpdateRowsEventv2] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		return createRowEvent(EventTypeUpdateRowsEventv2, 2, payloadLength, stream)
	}
	// DELETE_ROWS_EVENTv2
	createEventFuncs[EventTypeDeleteRowsEventv2] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		return createRowEvent(EventTypeDeleteRowsEventv2, 2, payloadLength, stream)
	}
	createEventFuncs[EventTypeGtidEvent] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		return nil, nil
	}
	createEventFuncs[EventTypeAnonymousGtidEvent] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		return nil, nil
	}
	createEventFuncs[EventTypePreviousGtidsEvent] = func(payloadLength int, _ EventHeaderType, stream *Stream) (interface{}, error) {
		// 未验证
		return nil, nil
	}

}

type FullEvent struct {
	eventHeader EventHeaderType
	event       interface{}
}

func (this *Stream) readEventPacket(packetLength int) (ret FullEvent, err error) {
	this.reset()
	if _, err = this.ReadUint1(); err == nil { // 最开始装成OK包的00字节
		var eventHeader EventHeaderType
		if eventHeader, err = this.readEventHeader(); err == nil {
			// 根据类型生成具体的event
			if f, ok := createEventFuncs[eventHeader.EventType]; ok {
				var eventBody interface{}
				eventBody, err = f(packetLength, eventHeader, this)
				ret = FullEvent{}
				ret.eventHeader = eventHeader
				ret.event = eventBody
				// packet是以5.5.62为准的，但后续版本可能会增加一些字段。这些增加的字段不影响处理binglog。所以这里都直接消耗掉
				if this.byteReadCounter != int64(packetLength) {
					var buf []byte
					buf, _, err = this.readNBytes(int64(packetLength) - this.byteReadCounter)
					this.log.Log(LogWarning, fmt.Sprintf("discard bytes=%v", buf))
				}
			} else {
				// 未实现这个event对应的创建功能
				err = NewUnknownEventTypeError(eventHeader.EventType)
			}
		}
	}
	return
}
func (this *Stream) readEOFPacket(packetLength int) (ret EOFPacket, err error) {
	this.reset()
	ret = EOFPacket{}
	if ret.Header, err = this.ReadUint1(); err == nil {
		// 这里只考虑Protocol41
		if ret.NumberOfWarnings, err = this.ReadUint2(); err == nil {
			ret.StatusFlags, err = this.ReadUint2()
		}
	}
	return ret, err
}
func (this *Stream) readOKPacket(packetLength int) (ret OKPacket, err error) {
	this.reset()
	ret = OKPacket{}
	if ret.Header, err = this.ReadUint1(); err == nil {
		if ret.AffectedRows, err = this.ReadUintLenenc(); err == nil {
			if ret.LastInsertId, err = this.ReadUintLenenc(); err == nil {
				if ret.StatusFlags, err = this.ReadUint2(); err == nil {
					if this.tstFlag(CapabilityFlag_CLIENT_PROTOCOL_41) {
						if ret.Warnings, err = this.ReadUint2(); err != nil {
							return
						}
					}
				}

				if this.tstFlag(CapabilityFlag_CLIENT_SESSION_TRACK) {
					if ret.Info, err = this.ReadStringLenenc(); err == nil {
						if this.tstStatus(SERVER_SESSION_STATE_CHANGED) {
							ret.SessionStateChanges, err = this.ReadStringLenenc()
						}
					}
				} else {
					tmp, e := this.ReadStringEof(packetLength)
					ret.Info = StringLenenc(tmp)
					err = e
				}
			}
		}
	}
	return
}
func (this *Stream) readErrPacket(packetLength int) (ret ErrPacket, err error) {
	this.reset()
	ret = ErrPacket{}
	if ret.Header, err = this.ReadUint1(); err == nil {
		if ret.ErrorCode, err = this.ReadUint2(); err == nil {
			if this.serverConfig.CapabilityFlags&Uint4(CapabilityFlag_CLIENT_PROTOCOL_41) != 0 {
				if ret.SqlStateMarker, err = this.ReadStringFix(1); err == nil {
					if ret.SqlState, err = this.ReadStringFix(5); err != nil {
						return
					}
				}
			}
			ret.ErrorMessage, err = this.ReadStringEof(packetLength)
		}
	}
	return
}
func (this *Stream) readHandshakeV10Packet(length int) (ret HandshakeV10, err error) {
	this.reset()
	ret = HandshakeV10{}
	if ret.Version, err = this.ReadUint1(); err == nil {
		if ret.ServerVersion, err = this.ReadStringNul(); err == nil {
			// 所以这里直接写死版本4
			// [3.23, 4.0.0)  => BinlogVersion = 1
			// [4.0.0, 4.0.1) => BinlogVersion = 2
			// [4.0.2, 5.0.0) => BinlogVersion = 3
			// [5.0.0, +∞)    => BinlogVersion = 4
			this.serverConfig.BinlogVersion = 4
			if ret.ConnectionId, err = this.ReadUint4(); err == nil {
				if ret.AuthPluginDataPart1, err = this.ReadStringFix(8); err == nil {
					if ret.Filler, err = this.ReadUint1(); err == nil {
						if ret.CapabilityFlagsLowerBytes, err = this.ReadUint2(); err == nil {
							//ret.CapabilityFlagsLowerBytes = 0xF700 | 0xFF
							ret.CapabilityFlags = Uint4(ret.CapabilityFlagsLowerBytes)
							if this.moreDataInPayload(length) {
								// if more data in the packet:
								if ret.CharacterSet, err = this.ReadUint1(); err == nil {
									if ret.StatusFlags, err = this.ReadUint2(); err == nil {
										this.serverConfig.StatusFlags = ret.StatusFlags
										if ret.CapabilityFlagsUpperBytes, err = this.ReadUint2(); err == nil {
											//ret.CapabilityFlagsUpperBytes = 0
											ret.CapabilityFlags = ret.CapabilityFlags | (Uint4(ret.CapabilityFlagsUpperBytes) << 16)
											this.serverConfig.CapabilityFlags = ret.CapabilityFlags
											useClientAuthData := 0 != (this.serverConfig.CapabilityFlags & Uint4(CapabilityFlag_CLIENT_PLUGIN_AUTH))
											if ret.LengthOfAuthPluginData, err = this.ReadUint1(); err == nil {
												if !useClientAuthData {
													ret.LengthOfAuthPluginData = Uint1(0)
												}
												if ret.Reserved, err = this.ReadStringFix(10); err == nil {
													AuthPluginDataPart2Len := 13
													if int(ret.LengthOfAuthPluginData)-int(8) > int(13) {
														AuthPluginDataPart2Len = int(ret.LengthOfAuthPluginData) - int(8)
													}
													if ret.AuthPluginDataPart2, err = this.ReadStringFix(AuthPluginDataPart2Len); err == nil {
														if useClientAuthData {
															// Due to Bug#59453 the auth-plugin-name is missing the terminating NUL-char in versions prior to 5.5.10 and 5.6.2.
															// TODO: 这里要分版本
															ret.AuthPluginName, err = this.ReadStringNul()
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	return ret, err
}

func (this *Stream) readOldAuthSwitchRequestPacket() (ret OldAuthSwitchRequest, err error) {
	this.reset()
	ret = OldAuthSwitchRequest{}
	ret.Version, err = this.ReadUint1()
	return ret, err
}
func (this *Stream) readAuthSwitchRequestPacket(length int) (ret AuthSwitchRequest, err error) {
	this.reset()
	ret = AuthSwitchRequest{}
	if ret.Version, err = this.ReadUint1(); err == nil {
		if ret.PluginName, err = this.ReadStringNul(); err == nil {
			ret.AuthPluginData, err = this.ReadStringEof(length)
		}
	}
	return
}

func (this *Stream) WritePayload(buf []byte) writeResult {
	this.writeChannel <- buf
	writeResult := <-this.writeResultChannel
	return writeResult
}

// 读出length个字节长度的payload。如果这个payload的长度是跨packet的，此时length=0xFFFFFF
func (this *Stream) ReadPayload(length int, isEvent bool) (ret interface{}, err error) {
	defer func() {
		if info := recover(); info != nil {
			ret = nil
			if e, ok := info.(error); ok {
				err = e
			}
		}
	}()
	// 先读一个字节，这个字节表示这个payload的类型。放在payloadType中
	bytes, _, err := this.readNBytes(1)
	if err != nil {
		return nil, err
	}
	payloadType := bytes[0]
	//fmt.Println("payloadType=", payloadType)

	// 生成具体对象的子数据流
	subStream := Stream{}
	subStream.log = this.log
	subStream.serverConfig = this.serverConfig
	subStream.controlChannel = make(chan int64)
	subStream.readChannel = make(chan byteStream)

	// 生成的结果对象
	payloadChannel := make(chan interface{})
	payloadErrorChannel := make(chan error)

	// 根据packet类型，生成不同的packet
	go this.buildPayload(subStream, payloadType, length, isEvent, payloadChannel, payloadErrorChannel)

	// 为了跨packet，把字节流展平。并且先把之前读出来的个字节再放回去
	this.pushBack(payloadType)

	byteRead := 0         // 所有读进来的字节数。即整个payLoad的字节数。如果这个payLoad太长，是跨packet的，这个值表示总字节数
	byteReadInPacket := 0 // 在生每个packet内读到字节数。packet到下一个时，这个值清0
	for {
		// subStream在读当前字段时，期望的字节数
		//fmt.Println("before <-subStream.controlChannel")
		subStreamNeed, ok := <-subStream.controlChannel
		//fmt.Println("subStreamNeed=", subStreamNeed, "ok=", ok)
		if !ok {
			break // channel关闭，表示整个结构已经读完
		}
		bs := byteStream{}
		if subStreamNeed > 0 {
			readPacket := 0      // 实际需要读的字节数
			allRead := true      // 是否在本packet内可以全读出来，即本字段是否需要跨packet
			leftByte := int64(0) // 如果不能全部读出来，剩下的字节数
			if int64(byteReadInPacket)+subStreamNeed > MaxPacketPayloadSize {
				// 本packet内读不完
				readPacket = MaxPacketPayloadSize - byteReadInPacket
				leftByte = subStreamNeed - int64(readPacket)
				allRead = false
			} else {
				// 本packet内可以读完
				readPacket = int(subStreamNeed)
			}
			//fmt.Println("readPacket=", readPacket)
			bytes, n, err := this.readNBytes(int64(readPacket))
			if err != nil {
				return nil, err
			}
			if !allRead {
				// 读不完时，需要在缓存里合并数据
				buf := bytes
				for {
					if leftByte <= 0 {
						break
					}
					// 再读下一个packet
					nextPacket, err := this.ReadPacketHeader()
					if err != nil {
						return nil, err
					}
					byteReadInPacket = 0
					// TODO：这里需要查sequenceId
					if nextPacket.PayloadLength == 0 {
						byteReadInPacket = 0
						break
					}
					thisRead := leftByte
					if leftByte > int64(nextPacket.PayloadLength) {
						thisRead = int64(nextPacket.PayloadLength)
					}
					bytes, n, err := this.readNBytes(int64(thisRead))
					if err != nil {
						return nil, err
					}
					byteReadInPacket += n
					buf = append(buf, bytes...)
					leftByte -= int64(n)
				}
				bs.bytes = buf
				bs.n = subStreamNeed
			} else {
				bs.bytes = bytes
				bs.n = int64(n)
				byteReadInPacket += n
			}
		} else if subStreamNeed == ReadAllPayload {
			// 读到本个packet的结尾
			//fmt.Println("Read Whole Packet:", MaxPacketPayloadSize - byteReadInPacket, "byteReadInPacket=", byteReadInPacket , "subStreamNeed=", subStreamNeed, "byteRead=", byteRead )
			bytes, n, err := this.readNBytes(int64(MaxPacketPayloadSize - byteReadInPacket))
			//fmt.Println("Read Whole Packet(result):", bytes[:10], n, err)
			bs.err = err
			if err == nil {
				for {
					if length != MaxPacketPayloadSize {
						break
					}
					// 循环读下一个，直到读到一个不再占满整个packet的payload为止，或者直到某个包出错
					nextPacket, err := this.ReadPacketHeader()
					buf, _, err := this.readNBytes(int64(nextPacket.PayloadLength))
					if err != nil {
						bs.err = err
						break
					}
					bytes = append(bytes, buf...)
					length = int(nextPacket.PayloadLength)
				}
			}
			bs.bytes = bytes
			bs.n = int64(n)
			//fmt.Println(bs)
		}
		byteRead += int(bs.n)
		subStream.readChannel <- bs
	}
	select {
	case ret = <-payloadChannel:
	case err = <-payloadErrorChannel:
	}
	return
}

func (this *Stream) ReadEvent() (ret interface{}, err error) {
	return this.readImpl(true)
}
func (this *Stream) Read() (ret interface{}, err error) {
	return this.readImpl(false)
}
func (this *Stream) readImpl(isEvent bool) (ret interface{}, err error) {
	if this.log != nil && this.config.LogTag&LogTCPStream != 0 {
		this.buf = make([]byte, 0)
	}
	packet, e := this.ReadPacketHeader()
	if e == nil {
		ret, e = this.ReadPayload(int(packet.PayloadLength), isEvent)
	}
	if this.log != nil && this.config.LogTag&LogTCPStream != 0 {
		dumpStream(this.log, "S->C", packet.PayloadLength, this.buf)
	}
	err = e
	return ret, err
}

func (this *Stream) Write(payload Decoder) writeResult {
	bytes := payload.Decode()
	if this.log != nil && this.config.LogTag&LogTCPStream != 0 {
		dumpStream(this.log, "C->S", Uint3(len(bytes)), bytes)
	}
	header := &Packet{}
	header.PayloadLength = Uint3(len(bytes))
	header.SequenceId = this.nextSequenceId()
	header.Payload = make([]byte, 0)
	return this.WritePayload(append(header.Decode(), bytes...))
}
func (this *Stream) WriteCom(com Decoder) writeResult {
	this.initSequenceId()
	return this.Write(com)
}
func dumpStream(log Log, dir string, PayloadLength Uint3, buf []byte) {
	if buf != nil {
		str := bytes.NewBufferString(fmt.Sprintf("%v PayloadLength=%d Len= %d [", dir, PayloadLength, len(buf)))
		for _, b := range buf {
			str.WriteString(fmt.Sprintf("% 2x", b))
		}
		str.WriteString("]")
		log.Log(LogTCPStream, str.String())
		// fmt.Println(str)
	} else {
		log.Log(LogTCPStream, fmt.Sprintf("%v PayloadLength=%d Nil []byte", dir, PayloadLength))
		// fmt.Println("%v PayloadLength=%d Nil []byte", dir, PayloadLength)
	}
}
