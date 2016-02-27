package mysql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

const (
	EventHeaderSize = 19
)

var (
	checksumVersionSplitMariaDB   []int = []int{5, 3, 0}
	checksumVersionProductMariaDB int   = (checksumVersionSplitMariaDB[0]*256+checksumVersionSplitMariaDB[1])*256 + checksumVersionSplitMariaDB[2]
)

type Event interface {
}

type EventHeader struct {
	Timestamp uint32
	EventType EventType
	ServerID  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

func (self *EventHeader) Decode(data []byte) error {
	if len(data) < EventHeaderSize {
		return errors.New(fmt.Sprintf("header size too short %d, must 19", len(data)))
	}

	pos := 0

	self.Timestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.EventType = EventType(data[pos])
	pos++

	self.ServerID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.EventSize = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.LogPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if self.EventSize < uint32(EventHeaderSize) {
		return errors.New(fmt.Sprintf("invalid event size %d, must >= 19", self.EventSize))
	}

	return nil
}

type FormatDescriptionEvent struct {
	Version uint16
	//len = 50
	ServerVersion          []byte
	CreateTimestamp        uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths []byte
	// 0 is off, 1 is for CRC32, 255 is undefined
	ChecksumAlgorithm byte
}

func (self *FormatDescriptionEvent) Decode(data []byte) error {
	pos := 0
	self.Version = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	self.ServerVersion = make([]byte, 50)
	copy(self.ServerVersion, data[pos:])
	pos += 50

	self.CreateTimestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.EventHeaderLength = data[pos]
	pos++

	if self.EventHeaderLength != byte(EventHeaderSize) {
		return errors.New(fmt.Sprintf("invalid event header length %d, must 19", self.EventHeaderLength))
	}

	checksumProduct := checksumVersionProductMariaDB

	if calcVersionProduct(string(self.ServerVersion)) >= checksumProduct {
		// here, the last 5 bytes is 1 byte check sum alg type and 4 byte checksum if exists
		self.ChecksumAlgorithm = data[len(data)-5]
		self.EventTypeHeaderLengths = data[pos : len(data)-5]
	} else {
		self.ChecksumAlgorithm = BINLOG_CHECKSUM_ALG_UNDEF
		self.EventTypeHeaderLengths = data[pos:]
	}

	return nil
}

type RotateEvent struct {
	Position    uint64
	NextLogName []byte
}

func (self *RotateEvent) Decode(data []byte) error {
	self.Position = binary.LittleEndian.Uint64(data[0:])
	self.NextLogName = data[8:]

	return nil
}

type QueryEvent struct {
	SlaveProxyID  uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []byte
	Schema        []byte
	Query         []byte
}

func (self *QueryEvent) Decode(data []byte) error {
	pos := 0

	self.SlaveProxyID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.ExecutionTime = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	schemaLength := uint8(data[pos])
	pos++

	self.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	statusVarsLength := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	self.StatusVars = data[pos : pos+int(statusVarsLength)]
	pos += int(statusVarsLength)

	self.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	//skip 0x00
	pos++

	self.Query = data[pos:]
	return nil
}

type XIDEvent struct {
	XID uint64
}

func (self *XIDEvent) Decode(data []byte) error {
	self.XID = binary.LittleEndian.Uint64(data)
	return nil
}

type BeginLoadQueryEvent struct {
	FileID    uint32
	BlockData []byte
}

func (self *BeginLoadQueryEvent) Decode(data []byte) error {
	pos := 0

	self.FileID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.BlockData = data[pos:]

	return nil
}

type ExecuteLoadQueryEvent struct {
	SlaveProxyID     uint32
	ExecutionTime    uint32
	SchemaLength     uint8
	ErrorCode        uint16
	StatusVars       uint16
	FileID           uint32
	StartPos         uint32
	EndPos           uint32
	DupHandlingFlags uint8
}

func (self *ExecuteLoadQueryEvent) Decode(data []byte) error {
	pos := 0

	self.SlaveProxyID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.ExecutionTime = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.SchemaLength = uint8(data[pos])
	pos++

	self.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	self.StatusVars = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	self.FileID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.StartPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.EndPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	self.DupHandlingFlags = uint8(data[pos])

	return nil
}

type MariadbAnnotaeRowsEvent struct {
	Query []byte
}

func (self *MariadbAnnotaeRowsEvent) Decode(data []byte) error {
	self.Query = data
	return nil
}

type MariadbBinlogCheckPointEvent struct {
	Info []byte
}

func (self *MariadbBinlogCheckPointEvent) Decode(data []byte) error {
	self.Info = data
	return nil
}

type MariadbGTIDEvent struct {
	GTID MariadbGTID
}

func (self *MariadbGTIDEvent) Decode(data []byte) error {
	self.GTID.SequenceNumber = binary.LittleEndian.Uint64(data)
	self.GTID.DomainID = binary.LittleEndian.Uint32(data[8:])

	// we don't care commit id now, maybe later

	return nil
}

type MariadbGTIDListEvent struct {
	GTIDs []MariadbGTID
}

func (self *MariadbGTIDListEvent) Decode(data []byte) error {
	pos := 0
	v := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	count := v & uint32((1<<28)-1)

	self.GTIDs = make([]MariadbGTID, count)

	for i := uint32(0); i < count; i++ {
		self.GTIDs[i].DomainID = binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		self.GTIDs[i].ServerID = binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		self.GTIDs[i].SequenceNumber = binary.LittleEndian.Uint64(data[pos:])
	}

	return nil
}

func splitServerVersion(server string) []int {
	seps := strings.Split(server, ".")
	if len(seps) < 3 {
		return []int{0, 0, 0}
	}

	x, _ := strconv.Atoi(seps[0])
	y, _ := strconv.Atoi(seps[1])

	index := 0
	for i, c := range seps[2] {
		if !unicode.IsNumber(c) {
			index = i
			break
		}
	}

	z, _ := strconv.Atoi(seps[2][0:index])

	return []int{x, y, z}
}

func calcVersionProduct(server string) int {
	versionSplit := splitServerVersion(server)

	return ((versionSplit[0]*256+versionSplit[1])*256 + versionSplit[2])
}
