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

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	// 只有在传入的index大于当前commit索引，以及maxIndex对应的term与传入的term匹配时，才使用这些数据进行commit
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
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
	// 感觉应该是数组的最后一个啊
	//i, err := l.storage.LastIndex()
	//if err != nil {
	//	panic(err)
	//}
	return l.entries[len(l.entries)-1].Index
}

// 判断是否比当前节点的日志更新：1）term是否更大 2）term相同的情况下，索引是否更大
func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.LastIndex())
}

// 返回最后一个索引的term
func (l *RaftLog) lastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
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

// 将raftlog的commit索引，修改为tocommit
func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	// 首先需要判断，commit索引绝不能变小
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			// 传入的值如果比lastIndex大则是非法的
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
		log.Infof("commit to %d", tocommit)
	}
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

// 添加数据，返回最后一条日志的索引
func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	// 没有数据，直接返回最后一条日志索引
	if len(ents) == 0 {
		return l.LastIndex()
	}
	// 如果索引小于committed，则说明该数据是非法的
	if after := ents[0].Index - 1; after < l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	// 放入unstable存储中
	l.truncateAndAppend(ents)
	return l.LastIndex()
}

// 传入entries，可能会导致原先数据的截断或者添加操作
// 这就是最开始注释中说明的offset可能比持久化索引小的情况，需要做截断
func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	if len(ents) == 0 {
		return
	}

	first := l.stabled
	last := ents[0].Index + uint64(len(ents)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return
	}
	// truncate compacted entries
	if first > ents[0].Index {
		ents = ents[first-ents[0].Index:]
	}

	offset := ents[0].Index - l.firstIndex
	switch {
	case uint64(len(l.entries)) > offset:
		l.entries = append([]pb.Entry{}, l.entries[:offset]...)
		l.entries = append(l.entries, ents...)
	case uint64(len(l.entries)) == offset:
		l.entries = append(l.entries, ents...)
	default:
		log.Panicf("missing log entry [last: %d, append at: %d]",
			l.LastIndex(), ents[0].Index)
	}
}

// 如果传入的err是nil，则返回t；如果是ErrCompacted则返回0，其他情况都panic
func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	log.Panicf("unexpected error (%v)", err)
	return 0
}
