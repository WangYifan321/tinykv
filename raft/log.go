// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	// entries包含storage的数据，空间换时间吧
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	raftLog := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	raftLog.firstIndex = firstIndex
	raftLog.applied = firstIndex - 1
	raftLog.committed = firstIndex - 1
	raftLog.stabled = lastIndex
	raftLog.entries = entries
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {

	// Your Code Here (2C).
}

// [lo, hi - 1]
func (l *RaftLog) sliceEntries(lo uint64, hi uint64) ([]pb.Entry, error) {
	if lo > l.LastIndex() {
		return nil, nil
	}
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	if lo < l.firstIndex {
		return nil, ErrCompacted
	}
	if hi > l.LastIndex()+1 {
		log.Panicf("hi out of bound hi: %d", hi)
	}
	return l.entries[lo:hi], nil

}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	//entries, err := l.storage.Entries(l.firstIndex, l.LastIndex()+1)
	//if errors.Is(err, ErrCompacted) {
	//	// 为啥？
	//	return l.allEntries()
	//}
	//if errors.Is(err, ErrUnavailable) {
	//	log.Panic("allEntries is unavailable from storage")
	//}
	//if err != nil {
	//	panic(err)
	//}
	entries := l.entries[l.firstIndex:]
	return entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	//entries, err := l.storage.Entries(l.stabled + 1, l.LastIndex()+1)
	//if errors.Is(err, ErrCompacted) {
	//	log.Panic("unstableEntries is compacted from storage")
	//}
	//if errors.Is(err, ErrUnavailable) {
	//	log.Panic("unstableEntries is unavailable from storage")
	//}
	//if err != nil {
	//	panic(err)
	//}
	entries := l.entries[l.stabled+1:]
	return entries
}

func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	entries := l.entries[l.applied+1 : l.committed+1]
	return entries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.firstIndex-1 || i > l.LastIndex() {
		return 0, nil
	}
	offset := l.entries[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(l.entries) {
		return 0, ErrUnavailable
	}
	return l.entries[i-offset].Term, nil
}

// 修改applied索引
func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	// 判断合法性
	// 新的applied ID既不能比committed大，也不能比当前的applied索引小
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}
