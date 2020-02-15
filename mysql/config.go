package mysql
//import "fmt"

type Config struct{
	Debug bool
	buf []byte
}
func (this *Config) clearBuf(){
	this.buf = make([]byte, 0)
}
func (this *Config) appendBuf(b []byte){
	this.buf = append(this.buf, b...)
}
func NewConfig() *Config{
	ret := &Config{}
	ret.buf = make([]byte, 0)
	return ret
}
