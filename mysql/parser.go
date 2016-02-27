package mysql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

type BinlogParser struct {
	format *FormatDescriptionEvent

	tables map[uint64]*TableMapEvent
}

func NewBinlogParser() *BinlogParser {
	p := new(BinlogParser)

	p.tables = make(map[uint64]*TableMapEvent)

	return p
}

func (self *BinlogParser) ParseFile(fileName string, offset int64) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	magic := make([]byte, 4)
	if _, err = file.Read(magic); err != nil {
		return err
	} else if !bytes.Equal(magic, BinLogFileHeader) {
		return errors.New(fmt.Sprintf("%s is not a valid binlog file, head 4 bytes must fe'bin' ", fileName))
	}
	if offset < 4 {
		offset = 4
	}
	if _, err = file.Seek(offset, os.SEEK_SET); err != nil {
		return errors.New(fmt.Sprintf("seek %s to %d error %v", fileName, offset, err))
	}

	return self.Parse(file)

}

func (self *BinlogParser) Parse(r io.Reader) error {
	self.tables = make(map[uint64]*TableMapEvent)
	var err error
	var n int64

	for {
		headBuf := make([]byte, EventHeaderSize)

		if _, err = io.ReadFull(r, headBuf); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		var h *EventHeader
		h, err = self.parseHeader(headBuf)
		if err != nil {
			return err
		}
		if h.EventSize <= uint32(EventHeaderSize) {
			return errors.New(fmt.Sprintf("invalid event header, event size is %d, too small", h.EventSize))

		}
		var buf bytes.Buffer
		if n, err = io.CopyN(&buf, r, int64(h.EventSize)-int64(EventHeaderSize)); err != nil {
			return errors.New(fmt.Sprintf("get event body err %v, need %d - %d, but got %d", err, h.EventSize, EventHeaderSize, n))
		}
		data := buf.Bytes()
		//rawData := data
		eventLen := int(h.EventSize) - EventHeaderSize
		if len(data) != eventLen {
			return errors.New(fmt.Sprintf("invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen))
		}
		//var e Event
		_, err = self.parseEvent(h, data)

	}

}

func (self *BinlogParser) parseHeader(data []byte) (*EventHeader, error) {
	h := new(EventHeader)
	err := h.Decode(data)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (self *BinlogParser) parseEvent(h *EventHeader, data []byte) (Event, error) {
	var e Event
	if h.EventType == FORMAT_DESCRIPTION_EVENT {
		self.format = &FormatDescriptionEvent{}
	} else {
		if self.format != nil && self.format.ChecksumAlgorithm == BINLOG_CHECKSUM_ALG_CRC32 {
			data = data[0 : len(data)-4]
		}
		if h.EventType == ROTATE_EVENT {
			e = &RotateEvent{}
		} else {
			switch h.EventType {
			case QUERY_EVENT:
				e = &QueryEvent{}
			case XID_EVENT:
				e = &XIDEvent{}
			case TABLE_MAP_EVENT:
				te := &TableMapEvent{}
				if self.format.EventTypeHeaderLengths[TABLE_MAP_EVENT-1] == 6 {
					te.tableIDSize = 4
				} else {
					te.tableIDSize = 6
				}
				e = te
			case WRITE_ROWS_EVENTv0,
				UPDATE_ROWS_EVENTv0,
				DELETE_ROWS_EVENTv0,
				WRITE_ROWS_EVENTv1,
				DELETE_ROWS_EVENTv1,
				UPDATE_ROWS_EVENTv1,
				WRITE_ROWS_EVENTv2,
				UPDATE_ROWS_EVENTv2,
				DELETE_ROWS_EVENTv2:
				e = self.newRowsEvent(h)
			case ROWS_QUERY_EVENT:
				e = &RowsQueryEvent{}
			case BEGIN_LOAD_QUERY_EVENT:
				e = &BeginLoadQueryEvent{}
			case EXECUTE_LOAD_QUERY_EVENT:
				e = &ExecuteLoadQueryEvent{}
			case MARIADB_ANNOTATE_ROWS_EVENT:
				e = &MariadbAnnotaeRowsEvent{}
			case MARIADB_BINLOG_CHECKPOINT_EVENT:
				e = &MariadbBinlogCheckPointEvent{}
			case MARIADB_GTID_LIST_EVENT:
				e = &MariadbGTIDListEvent{}
			case MARIADB_GTID_EVENT:
				ee := &MariadbGTIDEvent{}
				ee.GTID.ServerID = h.ServerID
				e = ee
			default:
				e = &GenericEvent{}

			}

		}

	}
	return e, nil
}

func (p *BinlogParser) newRowsEvent(h *EventHeader) *RowsEvent {
	e := &RowsEvent{}
	if p.format.EventTypeHeaderLengths[h.EventType-1] == 6 {
		e.tableIDSize = 4
	} else {
		e.tableIDSize = 6
	}
	e.needBitmap2 = false
	e.tables = p.tables
	switch h.EventType {
	case WRITE_ROWS_EVENTv0:
		e.Version = 0
	case UPDATE_ROWS_EVENTv0:
		e.Version = 0
	case DELETE_ROWS_EVENTv0:
		e.Version = 0
	case WRITE_ROWS_EVENTv1:
		e.Version = 1
	case DELETE_ROWS_EVENTv1:
		e.Version = 1
	case UPDATE_ROWS_EVENTv1:
		e.Version = 1
		e.needBitmap2 = true
	case WRITE_ROWS_EVENTv2:
		e.Version = 2
	case UPDATE_ROWS_EVENTv2:
		e.Version = 2
		e.needBitmap2 = true
	case DELETE_ROWS_EVENTv2:
		e.Version = 2
	}

	return e

}
