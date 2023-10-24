package raft

import(
	"fmt"
	"strings"
)

type Log struct {
	Entries []Entry
	Index0  int
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}


func makeEmptyLog() Log {
	log := Log{
		Entries: make([]Entry, 1),
		Index0:  0,
	}
	return log
}

func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
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

func (rf *Raft) lastLogIndex() int{
	if len(rf.log.Entries) == 0{
		return rf.lastsnapshotIndex
	}else {
		return rf.log.lastLog().Index
	}
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log.Entries) == 0{
		return rf.lastsnapshotTerm
	}else {
		return rf.log.lastLog().Term
	}
}

func (e *Entry) String() string {
	return fmt.Sprint(e.Term)
}

func (l *Log) String() string {
	nums := []string{}
	for _, entry := range l.Entries {
		nums = append(nums, fmt.Sprintf("%4d", entry.Term))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

func (l *Log) appendone(e Entry) {
	l.Entries = append(l.Entries, e)
}

func (l *Log) cutend(idx int) {
	l.Entries = l.Entries[0 : idx-l.Index0]
}

func (l *Log) cutstart(idx int) {
	l.Entries = l.Entries[idx - l.Index0:]
	l.Index0 = idx
}



