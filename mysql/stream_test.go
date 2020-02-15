package mysql
import "fmt"
import "testing"

type DummyStream struct{
	Stream
	data []byte // 预先准备的用于测试的数据
	pos int     // 读时当前的位置
}
type DummyError struct{
	err string
}
func (this DummyError)Error() string{
	return this.err
}

// 把整个packet当成字节数组读出来。实际上mysql可能没有这些个packet类型，为了测试加上去的
const BytesPacketTag = 0xFF
type BytesPacket struct{
	bytes []byte
}
func (this *Stream) ReadBytesPacket() (BytesPacket, error){
	var ret BytesPacket
	bytes, _, err := this.readNBytes(ReadAllPayload)
	ret.bytes = bytes
	return ret, err
}

const Uint4BytesPacketTag = 0xFD
type Uint4BytesPacket struct{
	val Uint4
	bytes []byte
}
func (this *Stream) ReadUint4BytesPacket() (Uint4BytesPacket, error){
	var ret Uint4BytesPacket
	tag, err := this.ReadUint1()
	if tag != Uint4BytesPacketTag || err != nil{
		return ret, nil
	}
	v, err := this.ReadUint4()
	if err != nil{
		return ret, err
	}
	ret.val = v
	bytes, _, err := this.readNBytes(ReadAllPayload)
	ret.bytes = bytes
	return ret, err
}

func NewDummyStream(data []byte, err error) *DummyStream{
	ret := &DummyStream{}
	ret.initStream()
	ret.data = data
	ret.pos = 0
	go func(){
		for{
			if need, ok := <-ret.controlChannel; ok{
				if need > len(ret.data) - ret.pos{
					need = len(ret.data) - ret.pos
				}
				buf := make([]byte, need)
				copy(buf, ret.data[ret.pos:ret.pos+need])
				bs := byteStream{}
				bs.bytes = buf
				bs.n = need
				bs.err = err
				ret.pos += need
				ret.readChannel <- bs
			}else{
				break
			}
		}
	}()

	oldBuildPayload := ret.buildPayload
	ret.buildPayload = func(subStream Stream, payloadType byte, length int, payloadChannel chan interface{}, payloadErrorChannel chan error){
		var packet interface{}
		var err error
		// 测试专用的packet
		if payloadType == BytesPacketTag {
			packet, err = subStream.ReadBytesPacket()
			close(subStream.controlChannel)
			payloadChannel <- packet
			payloadErrorChannel <- err
		} else if payloadType == Uint4BytesPacketTag {
			packet, err = subStream.ReadUint4BytesPacket()
			close(subStream.controlChannel)
			payloadChannel <- packet
			payloadErrorChannel <- err
		}else{
			oldBuildPayload(subStream, payloadType, length, payloadChannel, payloadErrorChannel)
		}
	}

	return ret
}

// 测试最基础的读字节功能
func Test_readNBytes(t *testing.T){
	t1 := []byte{1, 2, 3}
	s1 := NewDummyStream(t1, nil)
	bytes, n, err := s1.readNBytes(1)
	if n != 1 || len(bytes) != 1 || bytes[0] != 1 || err != nil {
		t.Error("Test_readNBytes error1:", bytes, n, err)
	}

	bytes, n, err = s1.readNBytes(2)
	if n != 2 || len(bytes) != 2 || bytes[0] != 2 || bytes[1] != 3 || err != nil {
		t.Error("Test_readNBytes error2:", bytes, n, err)
	}

	func(){
		defer func(){
			var info interface{}
			if info = recover(); info != nil{
				if val, ok := info.(error); ok{
					if val.Error() == "readNBytes panic" {
						return
					}
				}
			}
			t.Error("Test_readNBytes error4(panic error):", info)
		}()
		dummyErr := DummyError{}
		dummyErr.err = "readNBytes panic"
		s2 := NewDummyStream(t1, dummyErr)
		bytes, n, err := s2.readNBytes(1)
		t.Error("Test_readNBytes error3(should panic):", bytes, n, err)
	}()


	s1 = NewDummyStream([]byte{1, 2, 3}, nil)
	s1.pushBack(10)
	bytes, n, err = s1.readNBytes(1)
	if n != 1 || len(bytes) != 1 || bytes[0] != 10 || err != nil{
		t.Error("Test_readNBytes error5:", bytes, n, err)
	}
	bytes, n, err = s1.readNBytes(1)
	if n != 1 || len(bytes) != 1 || bytes[0] != 1 || err != nil{
		t.Error("Test_readNBytes error6:", bytes, n, err)
	}
	s1.pushBack(20)
	bytes, n, err = s1.readNBytes(1)
	if n != 1 || len(bytes) != 1 || bytes[0] != 20 || err != nil{
		t.Error("Test_readNBytes error7:", bytes, n, err)
	}
	bytes, n, err = s1.readNBytes(1)
	if n != 1 || len(bytes) != 1 || bytes[0] != 2 || err != nil{
		t.Error("Test_readNBytes error8:", bytes, n, err)
	}
	s1.pushBack(30)
	s1.pushBack(40)
	bytes, n, err = s1.readNBytes(1)
	if n != 1 || len(bytes) != 1 || bytes[0] != 40 || err != nil{
		t.Error("Test_readNBytes error9:", bytes, n, err)
	}
	bytes, n, err = s1.readNBytes(1)
	if n != 1 || len(bytes) != 1 || bytes[0] != 30 || err != nil{
		t.Error("Test_readNBytes error10:", bytes, n, err)
	}
	bytes, n, err = s1.readNBytes(1)
	if n != 1 || len(bytes) != 1 || bytes[0] != 3 || err != nil{
		t.Error("Test_readNBytes error11:", bytes, n, err)
	}

	s1 = NewDummyStream([]byte{1, 2, 3}, nil)
	s1.pushBack(10)
	bytes, n, err = s1.readNBytes(2)
	if n != 2 || len(bytes) != 2 || bytes[0] != 10 || bytes[1] != 1 || err != nil{
		t.Error("Test_readNBytes error12:", bytes, n, err)
	}
	bytes, n, err = s1.readNBytes(2)
	if n != 2 || len(bytes) != 2 || bytes[0] != 2 || bytes[1] != 3 || err != nil{
		t.Error("Test_readNBytes error13:", bytes, n, err)
	}

	s1 = NewDummyStream([]byte{1, 2, 3}, nil)
	s1.pushBack(10)
	s1.pushBack(20)
	bytes, n, err = s1.readNBytes(2)
	if n != 2 || len(bytes) != 2 || bytes[0] != 20 || bytes[1] != 10 || err != nil{
		t.Error("Test_readNBytes error14:", bytes, n, err)
	}
	s1.pushBack(30)
	s1.pushBack(40)
	bytes, n, err = s1.readNBytes(4)
	if n != 4 || len(bytes) != 4 || bytes[0] != 40 || bytes[1] != 30 || bytes[2] == 2 || bytes[3] == 3 || err != nil{
		t.Error("Test_readNBytes error15:", bytes, n, err)
	}

	s1 = NewDummyStream([]byte{1, 2, 3}, nil)
	s1.pushBack(10)
	s1.pushBack(20)
	s1.pushBack(30)
	s1.pushBack(40)
	s1.pushBack(50)
	s1.pushBack(60)
	s1.pushBack(70)
	s1.pushBack(80)
	bytes, n, err = s1.readNBytes(8)
	if n != 8 || len(bytes) != 8 || bytes[0] != 80 || bytes[1] != 70 || bytes[2] != 60 || bytes[3] != 50 || bytes[4] != 40 || bytes[5] != 30 || bytes[6] != 20 || bytes[7] != 10 || err != nil{
		t.Error("Test_readNBytes error16:", bytes, n, err)
	}

	s1 = NewDummyStream([]byte{1, 2, 3}, nil)
	s1.pushBack(10)
	s1.pushBack(20)
	s1.pushBack(30)
	s1.pushBack(40)
	s1.pushBack(50)
	s1.pushBack(60)
	s1.pushBack(70)
	s1.pushBack(80)
	s1.pushBack(90)
	bytes, n, err = s1.readNBytes(8)
	if n != 8 || len(bytes) != 8 || bytes[0] != 90 || bytes[1] != 80 || bytes[2] != 70 || bytes[3] != 60 || bytes[4] != 50 || bytes[5] != 40 || bytes[6] != 30 || bytes[7] != 20 || err != nil{
		t.Error("Test_readNBytes error17:", bytes, n, err)
	}
	s1.pushBack(100)
	s1.pushBack(110)
	bytes, n, err = s1.readNBytes(3)
	if n != 3 || len(bytes) != 3 || bytes[0] != 110 || bytes[1] != 100 || bytes[2] != 10 || err != nil{
		t.Error("Test_readNBytes error18:", bytes, n, err)
	}
	s1.pushBack(120)
	s1.pushBack(130)
	bytes, n, err = s1.readNBytes(3)
	if n != 3 || len(bytes) != 3 || bytes[0] != 130 || bytes[1] != 120 || bytes[2] != 1 || err != nil{
		t.Error("Test_readNBytes error19:", bytes, n, err)
	}
}

func Test_ReadUint1(t *testing.T){
	s := NewDummyStream([]byte{1, 2, 3}, nil)
	n, err := s.ReadUint1()
	if n != 1 || err != nil {
		t.Error("Test_ReadUint1 error1:", n, err)
	}
	n, err = s.ReadUint1()
	if n != 2 || err != nil {
		t.Error("Test_ReadUint1 error2:", n, err)
	}
	n, err = s.ReadUint1()
	if n != 3 || err != nil {
		t.Error("Test_ReadUint1 error3:", n, err)
	}
}

func Test_ReadUint2(t *testing.T){
	s := NewDummyStream([]byte{1, 2, 3, 4}, nil)
	n, err := s.ReadUint2()
	if n != (2*256 + 1) || err != nil {
		t.Error("Test_ReadUint2 error1:", n, err)
	}
	n, err = s.ReadUint2()
	if n != (4*256 + 3) || err != nil {
		t.Error("Test_ReadUint2 error2:", n, err)
	}
}

func Test_ReadUintLenenc(t *testing.T){
	s := NewDummyStream(
		[]byte{
			250,
			252, 5, 6,
			253, 7, 8, 9,
			254, 10, 11, 12, 13, 14, 15, 16, 17,
			251,
			255,
		}, nil)
	n, err := s.ReadUintLenenc()
	if n != 250 || err != nil {
		t.Error("Test_ReadUintLenenc error1:", n, err)
	}
	n, err = s.ReadUintLenenc()
	if n != 6*256+5 || err != nil {
		t.Error("Test_ReadUintLenenc error2:", n, err)
	}
	n, err = s.ReadUintLenenc()
	if n != 9*256*256 + 8*256 + 7 || err != nil {
		t.Error("Test_ReadUintLenenc error3:", n, err)
	}
	n, err = s.ReadUintLenenc()
	if n != 10 +
		11*256+
		12*256*256+
		13*256*256*256+
		14*256*256*256*256+
		15*256*256*256*256*256+
		16*256*256*256*256*256*256+
		17*256*256*256*256*256*256*256 || err != nil {
		t.Error("Test_ReadUintLenenc error4:", n, err)
	}
	func(){
		n, err = s.ReadUintLenenc()
		if err != nil {
			if e, ok := err.(Error); ok && e.Value == 251 && e.Error() == NotUintLenenc {
				return
			}
		}
		t.Error("Test_ReadUintLenenc error5:", n, err)
	}()
	func(){
		n, err = s.ReadUintLenenc()
		if err != nil {
			if e, ok := err.(Error); ok && e.Value == 255 && e.Error() == NotUintLenenc {
				return
			}
		}
		t.Error("Test_ReadUintLenenc error6:", n, err)
	}()
}

func Test_ReadStringFix(t *testing.T){
	s := NewDummyStream(
		[]byte{
			72, 101, 108, 108, 111,
			87, 79, 82, 76, 68,
		}, nil)
	r, err := s.ReadStringFix(5)
	if r != "Hello" || err != nil{
		t.Error("Test_ReadStringFix error1:", r, err)
	}
	r, err = s.ReadStringFix(5)
	if r != "WORLD" || err != nil{
		t.Error("Test_ReadStringFix error2:", r, err)
	}
}

func Test_ReadStringNul(t *testing.T){
	s := NewDummyStream(
		[]byte{
			72, 101, 108, 108, 111, 0, // Hello
			0,                         // 空字符串
			64, 0,                     // @
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 0, // Hello..! * 16
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 62, 0, // Hello..! * 16 + >
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 62, 61, 0, // Hello..! * 16 + >=
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33,
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 0, // Hello..! * 32
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33,
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 
			87, 79, 82, 76, 68, 46, 46, 33, 87, 79, 82, 76, 68, 46, 46, 33, 60, 0, // Hello..! * 32 + <
		}, nil)
	r, err := s.ReadStringNul()
	if r != "Hello" || err != nil{
		t.Error("Test_ReadStringNul error1:", r, err)
	}
	r, err = s.ReadStringNul()
	if r != "" || err != nil{
		t.Error("Test_ReadStringNul error2:", r, err)
	}
	r, err = s.ReadStringNul()
	if r != "@" || err != nil{
		t.Error("Test_ReadStringNul error3:", r, err)
	}
	r, err = s.ReadStringNul()
	if r != "WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!" || err != nil{
		t.Error("Test_ReadStringNul error4:", r, err)
	}
	r, err = s.ReadStringNul()
	if r != "WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!>" || err != nil{
		t.Error("Test_ReadStringNul error5:", r, err)
	}
	r, err = s.ReadStringNul()
	if r != "WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!>=" || err != nil{
		t.Error("Test_ReadStringNul error6:", r, err)
	}
	r, err = s.ReadStringNul()
	if r != "WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!" || err != nil{
		t.Error("Test_ReadStringNul error7:", r, err)
	}
	r, err = s.ReadStringNul()
	if r != "WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!WORLD..!<" || err != nil{
		t.Error("Test_ReadStringNul error8:", r, err)
	}
}

func Test_ReadStringLenenc(t *testing.T){
	s := NewDummyStream(
		[]byte{
			0,
			5, 72, 101, 108, 108, 111,
			250,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			0xFC, 252, 0, 
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 72, 101, 108, 108, 111,
			72, 101, 108, 108, 111, 72, 101, 108, 108, 111, 63, 63,
		}, nil)
	r, err := s.ReadStringLenenc()
	if r != "" || err != nil{
		t.Error("Test_ReadStringLenenc error1:", r, err)
	}
	r, err = s.ReadStringLenenc()
	if r != "Hello" || err != nil{
		t.Error("Test_ReadStringLenenc error2:", r, err)
	}
	r, err = s.ReadStringLenenc()
	if r != "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello" || err != nil{
		t.Error("Test_ReadStringLenenc error3:", r, err)
	}
	r, err = s.ReadStringLenenc()
	if r != "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello??" || err != nil{
		t.Error("Test_ReadStringLenenc error4:", r, err)
	}
}

func Test_ReadPacketHeader(t *testing.T){
	s := NewDummyStream(
		[]byte{
			56, 0, 0, 0,
			0, 0, 0, 10,
		}, nil)
	p, err := s.ReadPacketHeader()
	if p.PayloadLength != 56 || p.SequenceId != 0 {
		t.Error("Test_ReadPacketHeader error1:", p, err)
	}
	p, err = s.ReadPacketHeader()
	if p.PayloadLength != 0 || p.SequenceId != 10 {
		t.Error("Test_ReadPacketHeader error2:", p, err)
	}
}

func Test_ReadPayload(t *testing.T){
	s := NewDummyStream(
		[]byte{
			BytesPacketTag,
		}, nil)
	p, err := s.ReadPayload(1)
	if val, ok := p.(BytesPacket); !ok || len(val.bytes) != 1 || val.bytes[0] != 0xFF{
		t.Error("Test_ReadPayload error1:", val, err)
	}
	{
		// 1个字节，全读
		prepareData := func() []byte{
			return []byte{
				BytesPacketTag, 1, 
			}
		}
		check := func(p interface{}, err error) bool{
			val, ok := p.(BytesPacket);
			return !(!ok || len(val.bytes) != 2 || val.bytes[0] != 0xFF ||  val.bytes[1] != 1)
		}
		s := NewDummyStream(prepareData(), nil)
		p, err := s.ReadPayload(2)
		if !check(p,err){
			t.Error("Test_ReadPayload error2:", p, err)
		}
	}
	func (){
		// MaxPacketPayloadSize-1个字节，全读
		buf := make([]byte, MaxPacketPayloadSize-1)
		buf[0] = BytesPacketTag
		for i:=1; i<MaxPacketPayloadSize-1; i++ {
			buf[i] = byte(i % 199)
		}
		s := NewDummyStream(buf, nil)
		p, err := s.ReadPayload(MaxPacketPayloadSize-1)
		var val BytesPacket
		var ok bool
		if val, ok = p.(BytesPacket); ok && len(val.bytes) == MaxPacketPayloadSize-1 && val.bytes[0] == BytesPacketTag && err == nil{
			for i:=1; i<MaxPacketPayloadSize-1; i++{
				if val.bytes[i] != byte(i%199){
					ok = false
					break
				}
			}
			if ok{
				return
			}else{
				t.Error("Test_ReadPayload error4: ok=", val.bytes[:50], err)
			}
		}
		t.Error("Test_ReadPayload error3: ok=", ok, err)
	}()

	func(){
		// MaxPacketPayloadSize个字节，全读
		buf := make([]byte, MaxPacketPayloadSize + 4)
		buf[0] = BytesPacketTag
		for i:=1; i<MaxPacketPayloadSize; i++ {
			buf[i] = byte(i % 199)
		}
		// 下个packet的长度及序号,0字节
		buf[MaxPacketPayloadSize] = 0
		buf[MaxPacketPayloadSize+1] = 0
		buf[MaxPacketPayloadSize+2] = 0
		buf[MaxPacketPayloadSize+3] = 1
		s := NewDummyStream(buf, nil)
		p, err := s.ReadPayload(MaxPacketPayloadSize)
		var val BytesPacket
		var ok bool
		if val, ok = p.(BytesPacket); ok && len(val.bytes) == MaxPacketPayloadSize && val.bytes[0] == BytesPacketTag && err == nil{
			for i:=1; i<MaxPacketPayloadSize; i++{
				if val.bytes[i] != byte(i%199){
					ok = false
					break
				}
			}
			if ok{
				return
			}else{
				t.Error("Test_ReadPayload error6: ok=", val.bytes[:50], err)
			}
		}
		t.Error("Test_ReadPayload error5: ok=", ok, err)
	}()

	func(){
		// MaxPacketPayloadSize+1个字节，全读
		buf := make([]byte, MaxPacketPayloadSize + 5)
		buf[0] = BytesPacketTag
		for i:=1; i<MaxPacketPayloadSize; i++ {
			buf[i] = byte(i % 199)
		}
		// 下个packet的长度及序号，1字节
		buf[MaxPacketPayloadSize] = 1
		buf[MaxPacketPayloadSize+1] = 0
		buf[MaxPacketPayloadSize+2] = 0
		buf[MaxPacketPayloadSize+3] = 1
		buf[MaxPacketPayloadSize+4] = 99
		s := NewDummyStream(buf, nil)
		p, err := s.ReadPayload(MaxPacketPayloadSize)
		var val BytesPacket
		var ok bool
		if val, ok = p.(BytesPacket); ok && len(val.bytes) == MaxPacketPayloadSize+1 && val.bytes[0] == BytesPacketTag && err == nil{
			for i:=1; i<MaxPacketPayloadSize; i++{
				if val.bytes[i] != byte(i%199){
					ok = false
					break
				}
			}
			if ok && val.bytes[MaxPacketPayloadSize] == 99 {
				return
			}else{
				t.Error("Test_ReadPayload error8 ok=", val.bytes[:50], err)
			}
		}
		t.Error("Test_ReadPayload error7: ok=", ok, err)
	}()

	func(){
		// MaxPacketPayloadSize+3个字节，全读
		buf := make([]byte, MaxPacketPayloadSize + 8)
		buf[0] = BytesPacketTag
		for i:=1; i<MaxPacketPayloadSize; i++ {
			buf[i] = byte(i % 199)
		}
		// 下个packet的长度及序号，1字节
		buf[MaxPacketPayloadSize] = 3
		buf[MaxPacketPayloadSize+1] = 0
		buf[MaxPacketPayloadSize+2] = 0
		buf[MaxPacketPayloadSize+3] = 1
		buf[MaxPacketPayloadSize+4] = 99
		buf[MaxPacketPayloadSize+5] = 199
		buf[MaxPacketPayloadSize+6] = 9
		s := NewDummyStream(buf, nil)
		p, err := s.ReadPayload(MaxPacketPayloadSize)
		var val BytesPacket
		var ok bool
		if val, ok = p.(BytesPacket); ok && len(val.bytes) == MaxPacketPayloadSize+3 && val.bytes[0] == BytesPacketTag && err == nil{
			for i:=1; i<MaxPacketPayloadSize; i++{
				if val.bytes[i] != byte(i%199){
					ok = false
					break
				}
			}
			if ok && val.bytes[MaxPacketPayloadSize] == 99 && val.bytes[MaxPacketPayloadSize+1] == 199 && val.bytes[MaxPacketPayloadSize+2] == 9{
				return
			}else{
				t.Error("Test_ReadPayload error10 ok=", val.bytes[:50], err)
			}
		}
		t.Error("Test_ReadPayload error9: ok=", ok, err)
	}()

	func(){
		// MaxPacketPayloadSize*2-1个字节，全读
		buf := make([]byte, MaxPacketPayloadSize + 4 + MaxPacketPayloadSize - 1)
		buf[0] = BytesPacketTag
		for i:=1; i<MaxPacketPayloadSize; i++ {
			buf[i] = byte(i % 199)
		}
		// 下个packet的长度及序号，0xFFFFFE字节
		buf[MaxPacketPayloadSize] = 0xFE
		buf[MaxPacketPayloadSize+1] = 0xFF
		buf[MaxPacketPayloadSize+2] = 0xFF
		buf[MaxPacketPayloadSize+3] = 1
		for i:=MaxPacketPayloadSize+3+1; i<MaxPacketPayloadSize*2-1+4; i++ {
			buf[i] = byte(i % 198)
		}
		s := NewDummyStream(buf, nil)
		p, err := s.ReadPayload(MaxPacketPayloadSize)
		var val BytesPacket
		var ok bool
		if val, ok = p.(BytesPacket); ok && len(val.bytes) == MaxPacketPayloadSize*2-1 && val.bytes[0] == BytesPacketTag && err == nil{
			for i:=1; i<MaxPacketPayloadSize; i++{
				if val.bytes[i] != byte(i%199){
					ok = false
					break
				}
			}
			for i:=MaxPacketPayloadSize; i<2*MaxPacketPayloadSize-1; i++{
				if val.bytes[i] != byte((i+4)%198){
					ok = false
					break
				}
			}
			if ok {
				return
			}else{
				t.Error("Test_ReadPayload error12 ok=", val.bytes[:50], err)
			}
		}
		t.Error("Test_ReadPayload error11: ok=", ok, err)
	}()

	func(){
		// MaxPacketPayloadSize*2个字节，全读
		buf := make([]byte, MaxPacketPayloadSize + 4 + MaxPacketPayloadSize + 4)
		buf[0] = BytesPacketTag
		for i:=1; i<MaxPacketPayloadSize; i++ {
			buf[i] = byte(i % 199)
		}
		// 下个packet的长度及序号，0xFFFFFE字节
		buf[MaxPacketPayloadSize] = 0xFF
		buf[MaxPacketPayloadSize+1] = 0xFF
		buf[MaxPacketPayloadSize+2] = 0xFF
		buf[MaxPacketPayloadSize+3] = 1
		for i:=MaxPacketPayloadSize+4; i<MaxPacketPayloadSize*2+4; i++ {
			buf[i] = byte(i % 198)
		}
		// 下个packet的长度及序号，0x0字节
		buf[MaxPacketPayloadSize*2+4] = 0
		buf[MaxPacketPayloadSize*2+4+1] = 0
		buf[MaxPacketPayloadSize*2+4+2] = 0
		buf[MaxPacketPayloadSize*2+4+3] = 2
		s := NewDummyStream(buf, nil)
		p, err := s.ReadPayload(MaxPacketPayloadSize)
		var val BytesPacket
		var ok bool
		if val, ok = p.(BytesPacket); ok && len(val.bytes) == MaxPacketPayloadSize*2 && val.bytes[0] == BytesPacketTag && err == nil{
			//fmt.Println("...", buf[MaxPacketPayloadSize+4], ",", val.bytes[MaxPacketPayloadSize])
			//fmt.Println("...", buf[MaxPacketPayloadSize+5], ",", val.bytes[MaxPacketPayloadSize+1])
			//fmt.Println("...", buf[MaxPacketPayloadSize+6], ",", val.bytes[MaxPacketPayloadSize+2])
			for i:=1; i<MaxPacketPayloadSize; i++{
				if val.bytes[i] != byte(i%199){
					ok = false
					break
				}
			}
			for i:=MaxPacketPayloadSize; i<2*MaxPacketPayloadSize; i++{
				if val.bytes[i] != byte((i+4)%198){
					ok = false
					break
				}
			}
			if !ok {
				t.Error("Test_ReadPayload error14 ok=", val.bytes[:50], err)
			}
			return
		}
		t.Error("Test_ReadPayload error13: ok=", ok, err)
	}()

	func(){
		// MaxPacketPayloadSize*2+1个字节，全读
		buf := make([]byte, MaxPacketPayloadSize + 4 + MaxPacketPayloadSize + 4 + 1)
		buf[0] = BytesPacketTag
		for i:=1; i<MaxPacketPayloadSize; i++ {
			buf[i] = byte(i % 199)
		}
		// 下个packet的长度及序号，0xFFFFFE字节
		buf[MaxPacketPayloadSize] = 0xFF
		buf[MaxPacketPayloadSize+1] = 0xFF
		buf[MaxPacketPayloadSize+2] = 0xFF
		buf[MaxPacketPayloadSize+3] = 1
		for i:=MaxPacketPayloadSize+4; i<MaxPacketPayloadSize*2+4; i++ {
			buf[i] = byte(i % 198)
		}
		// 下个packet的长度及序号，0x0字节
		buf[MaxPacketPayloadSize*2+4] = 1
		buf[MaxPacketPayloadSize*2+4+1] = 0
		buf[MaxPacketPayloadSize*2+4+2] = 0
		buf[MaxPacketPayloadSize*2+4+3] = 2
		buf[MaxPacketPayloadSize*2+4+4] = 108
		s := NewDummyStream(buf, nil)
		p, err := s.ReadPayload(MaxPacketPayloadSize)
		var val BytesPacket
		var ok bool
		if val, ok = p.(BytesPacket); ok && len(val.bytes) == MaxPacketPayloadSize*2+1 && val.bytes[0] == BytesPacketTag && err == nil{
			//fmt.Println("...", buf[MaxPacketPayloadSize+4], ",", val.bytes[MaxPacketPayloadSize])
			//fmt.Println("...", buf[MaxPacketPayloadSize+5], ",", val.bytes[MaxPacketPayloadSize+1])
			//fmt.Println("...", buf[MaxPacketPayloadSize+6], ",", val.bytes[MaxPacketPayloadSize+2])
			for i:=1; i<MaxPacketPayloadSize; i++{
				if val.bytes[i] != byte(i%199){
					ok = false
					break
				}
			}
			for i:=MaxPacketPayloadSize; i<2*MaxPacketPayloadSize; i++{
				if val.bytes[i] != byte((i+4)%198){
					ok = false
					break
				}
			}
			if val.bytes[2*MaxPacketPayloadSize] != 108{
				ok = false
			}
			if !ok {
				t.Error("Test_ReadPayload error16 ok=", val.bytes[:50], err)
			}
			return
		}
		t.Error("Test_ReadPayload error15: ok=", ok, err)
	}()

	{
		// 4个字节，全读
		s := NewDummyStream(
			[]byte{
				Uint4BytesPacketTag, 1, 2, 3, 4,
			}, nil)
		p, err := s.ReadPayload(5)
		if v, ok := p.(Uint4BytesPacket); ok && err != nil || v.val != 0x04030201 ||v.bytes != nil{
			t.Error("Test_ReadPayload error16:", p, err)
		}
	}
	{
		// 4个字节+1字节
		s := NewDummyStream(
			[]byte{
				Uint4BytesPacketTag, 1, 2, 3, 4, 5,
			}, nil)
		p, err := s.ReadPayload(5)
		if v, ok := p.(Uint4BytesPacket); ok && err != nil || v.val != 0x04030201 ||v.bytes == nil || v.bytes[0]!=5 {
			t.Error("Test_ReadPayload error17:", p, err)
		}
	}

	func (){
		// MaxPacketPayloadSize-1-4个字节，全读
		buf := make([]byte, MaxPacketPayloadSize-1)
		for i:=1; i<MaxPacketPayloadSize-1; i++ {
			buf[i] = byte(i % 199)
		}
		buf[0] = Uint4BytesPacketTag
		buf[1] = 4
		buf[2] = 3
		buf[3] = 2
		buf[4] = 1
		s := NewDummyStream(buf, nil)
		p, err := s.ReadPayload(MaxPacketPayloadSize-1)
		var val Uint4BytesPacket
		var ok bool
		if val, ok = p.(Uint4BytesPacket); ok && err == nil{
			//fmt.Println(val.val, len(val.bytes), val.bytes[:50])
			if val.val == 0x01020304 && len(val.bytes) == MaxPacketPayloadSize-1-1-4 { // -1是打头的标志位、-1是长度MaxPacketPayloadSize-1，-4是前面有个Uint4
				for i:=0; i<MaxPacketPayloadSize-1-1-4; i++{
					if val.bytes[i] != byte((i+5)%199){
						ok = false
						break
					}
				}
			}
			if !ok {
				t.Error("Test_ReadPayload error19: ok=", val.bytes[:50], err)
			}
			return
		}
		t.Error("Test_ReadPayload error18: ok=", ok, err)
	}()
	
	func (){
		// MaxPacketPayloadSize-4个字节，全读
		buf := make([]byte, MaxPacketPayloadSize+4)
		for i:=0; i<MaxPacketPayloadSize; i++ {
			buf[i] = byte(i % 199)
		}
		buf[0] = Uint4BytesPacketTag
		buf[1] = 4
		buf[2] = 3
		buf[3] = 2
		buf[4] = 1
		buf[MaxPacketPayloadSize+0] = 0
		buf[MaxPacketPayloadSize+1] = 0
		buf[MaxPacketPayloadSize+2] = 0
		buf[MaxPacketPayloadSize+3] = 1
		s := NewDummyStream(buf, nil)
		//fmt.Println("________")
		p, err := s.ReadPayload(MaxPacketPayloadSize)
		var val Uint4BytesPacket
		var ok bool
		if val, ok= p.(Uint4BytesPacket); ok && err == nil{
			//fmt.Println(val.val, len(val.bytes), val.bytes[:50])
			if val.val == 0x01020304 && len(val.bytes) == MaxPacketPayloadSize-1-4 {
				for i:=0; i<MaxPacketPayloadSize-1-4; i++{
					if val.bytes[i] != byte((i+5)%199){
						ok = false
						break
					}
				}
			}
			if !ok {
				t.Error("Test_ReadPayload error21: ok=", val.bytes[:50], err)
			}
			return
		}
		t.Error("Test_ReadPayload error20: ok=", ok, err)
	}()

	func (){
		// MaxPacketPayloadSize-4+1个字节，全读
		buf := make([]byte, MaxPacketPayloadSize+5)
		for i:=0; i<MaxPacketPayloadSize; i++ {
			buf[i] = byte(i % 199)
		}
		buf[0] = Uint4BytesPacketTag
		buf[1] = 4
		buf[2] = 3
		buf[3] = 2
		buf[4] = 1
		buf[MaxPacketPayloadSize+0] = 0
		buf[MaxPacketPayloadSize+1] = 0
		buf[MaxPacketPayloadSize+2] = 0
		buf[MaxPacketPayloadSize+3] = 1
		buf[MaxPacketPayloadSize+4] = 99
		s := NewDummyStream(buf, nil)
		//fmt.Println("________")
		p, err := s.ReadPayload(MaxPacketPayloadSize)
		var val Uint4BytesPacket
		var ok bool
		if val, ok= p.(Uint4BytesPacket); ok && err == nil{
			//fmt.Println(val.val, len(val.bytes), val.bytes[:50])
			if val.val == 0x01020304 && len(val.bytes) == MaxPacketPayloadSize-1-4+1 {
				for i:=0; i<MaxPacketPayloadSize-1-4; i++{
					if val.bytes[i] != byte((i+5)%199){
						ok = false
						break
					}
				}
				if val.bytes[MaxPacketPayloadSize] != 99{
					ok = false
				}
			}
			if !ok {
				t.Error("Test_ReadPayload error23: ok=", val.bytes[:50], err)
			}
			return
		}
		t.Error("Test_ReadPayload error22: ok=", ok, err)
	}()

	func (){
		// MaxPacketPayloadSize*2-4个字节，全读
		buf := make([]byte, MaxPacketPayloadSize*2+4+4)
		for i:=0; i<MaxPacketPayloadSize; i++ {
			buf[i] = byte(i % 199)
		}
		for i:=0; i<MaxPacketPayloadSize; i++ {
			buf[MaxPacketPayloadSize+i+4] = byte(i % 198)
		}
		buf[0] = Uint4BytesPacketTag
		buf[1] = 4
		buf[2] = 3
		buf[3] = 2
		buf[4] = 1
		buf[MaxPacketPayloadSize+0] = 0xFF
		buf[MaxPacketPayloadSize+1] = 0xFF
		buf[MaxPacketPayloadSize+2] = 0xFF
		buf[MaxPacketPayloadSize+3] = 1
		buf[MaxPacketPayloadSize*2+4+0] = 0
		buf[MaxPacketPayloadSize*2+4+1] = 0
		buf[MaxPacketPayloadSize*2+4+2] = 0
		buf[MaxPacketPayloadSize*2+4+3] = 2
		s := NewDummyStream(buf, nil)
		p, err := s.ReadPayload(MaxPacketPayloadSize)
		var val Uint4BytesPacket
		var ok bool
		if val, ok= p.(Uint4BytesPacket); ok && err == nil{
			if val.val == 0x01020304 && len(val.bytes) == MaxPacketPayloadSize*2-1-4 {
				for i:=0; i<MaxPacketPayloadSize-1-4; i++{
					if val.bytes[i] != byte((i+5)%199){
						ok = false
						break
					}
				}
				for i:=0; i<MaxPacketPayloadSize; i++{
					if val.bytes[i+MaxPacketPayloadSize-5] != byte(i%198){
						ok = false
						break
					}
				}
			}
			if !ok {
				t.Error("Test_ReadPayload error25: ok=", val.bytes[:50], err)
			}
			return
		}
		t.Error("Test_ReadPayload error24: ok=", ok, err)
	}()

	func (){
		// MaxPacketPayloadSize*2-4+1个字节，全读
		buf := make([]byte, MaxPacketPayloadSize*2+4+4+1)
		for i:=0; i<MaxPacketPayloadSize; i++ {
			buf[i] = byte(i % 199)
		}
		for i:=0; i<MaxPacketPayloadSize; i++ {
			buf[MaxPacketPayloadSize+i+4] = byte(i % 198)
		}
		buf[0] = Uint4BytesPacketTag
		buf[1] = 4
		buf[2] = 3
		buf[3] = 2
		buf[4] = 1
		buf[MaxPacketPayloadSize+0] = 0xFF
		buf[MaxPacketPayloadSize+1] = 0xFF
		buf[MaxPacketPayloadSize+2] = 0xFF
		buf[MaxPacketPayloadSize+3] = 1
		buf[MaxPacketPayloadSize*2+4+0] = 0
		buf[MaxPacketPayloadSize*2+4+1] = 0
		buf[MaxPacketPayloadSize*2+4+2] = 0
		buf[MaxPacketPayloadSize*2+4+3] = 2
		buf[MaxPacketPayloadSize*2+4+4] = 211
		s := NewDummyStream(buf, nil)
		p, err := s.ReadPayload(MaxPacketPayloadSize)
		var val Uint4BytesPacket
		var ok bool
		if val, ok= p.(Uint4BytesPacket); ok && err == nil{
			if val.val == 0x01020304 && len(val.bytes) == MaxPacketPayloadSize*2-1-4+1 {
				for i:=0; i<MaxPacketPayloadSize-1-4; i++{
					if val.bytes[i] != byte((i+5)%199){
						ok = false
						break
					}
				}
				for i:=0; i<MaxPacketPayloadSize; i++{
					if val.bytes[i+MaxPacketPayloadSize-5] != byte(i%198){
						ok = false
						break
					}
				}
				if val.bytes[2*MaxPacketPayloadSize-5] != 211{
					ok = false
				}
			}
			if !ok {
				t.Error("Test_ReadPayload error27: ok=", val.bytes[:50], err)
			}
			return
		}
		t.Error("Test_ReadPayload error26: ok=", ok, err)
	}()
}

func Test_readHandshakeV10Packet(t *testing.T){
	buf := []byte{
		10,
		53, 46, 49, 46, 55, 51, 45, 108, 111, 103, 0,
		16, 2, 0, 0,
		116, 95, 92, 49, 33, 76, 63, 53,
		0, // filler
		255, 247,// capability flags
		8, //character set
		2, 0,// status flags
		0, 0,// capability flags (upper 2 bytes)
		0, // if capabilities & CLIENT_PLUGIN_AUTH {
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //  reserved (all [00])
		42, 116, 43, 116, 43, 115, 39, 100, 67, 97, 63, 84, 0, //???
	}
	s := NewDummyStream(buf, nil)
	p, err := s.ReadPayload(len(buf))
	val, ok :=p.(HandshakeV10)
	if !ok || err != nil {
		fmt.Println(p)
	}
	fmt.Println(val.toString())

}
func no(t *testing.T){
	fmt.Println("End")
}
