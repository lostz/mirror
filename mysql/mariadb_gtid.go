package mysql

type MariadbGTID struct {
	DomainID       uint32
	ServerID       uint32
	SequenceNumber uint64
}
