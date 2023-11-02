package raft

type Log struct {
	Entries []Entry
	Index0  int
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

//创建一个初始容量为1的日志切片，初始日志Index未初始化默认为0（即第一条日志为无效日志）
func makeEmptyLog() Log {
	log := Log{
		Entries: make([]Entry, 1),
		Index0:  0,
	}
	return log
}

//创建一个初始容量为0的日志切片，Index0由调用者指定（即初始切片为空，之后的第一条日志为有效日志）
func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
}

func (l *Log) appendone(e Entry) {
	l.Entries = append(l.Entries, e)
}

func (l *Log) at(idx int) *Entry {
	return &l.Entries[idx - l.Index0]
}

func (l *Log) slice(index int) []Entry {
	return l.Entries[index-l.Index0:]
}

func (l *Log) len() int {
	return len(l.Entries)
}

func (l *Log) lastLog() *Entry {
	return l.at (l.Index0 + l.len() - 1)
}

//返回最后一条日志的日志索引（不是切片下标索引！）
func (rf *Raft) lastLogIndex() int{
	if len(rf.log.Entries) == 0{
		return rf.lastsnapshotIndex
	}else {
		return rf.log.lastLog().Index
	}
}

//返回最后一条日志的任期
func (rf *Raft) lastLogTerm() int {
	if len(rf.log.Entries) == 0{
		return rf.lastsnapshotTerm
	}else {
		return rf.log.lastLog().Term
	}
}

func (l *Log) cutend(idx int) {
	l.Entries = l.Entries[0 : idx-l.Index0]
}

func (l *Log) cutstart(idx int) {
	l.Entries = l.Entries[idx - l.Index0:]
	l.Index0 = idx
}



