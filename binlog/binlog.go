package binlog

import (
	"bytes"
	"errors"
	"github.com/lostz/mirror/mysql"
	"os"
)

type Binlog struct {
	File *os.File
}

func NewBinlog(file string) (*Binlog, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	magic := make([]byte, 4)
	if _, err = f.Read(magic); err != nil {
		return nil, err
	} else if !bytes.Equal(magic, mysql.BinLogFileHeader) {
		return nil, errors.New("File is not a binary log file")
	}
	b := &Binlog{}
	b.File = f
	return b, nil
}

func (self *Binlog) FilterSql(sql string) {

}
