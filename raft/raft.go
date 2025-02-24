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
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
	"strings"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

var globalRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
	return updated
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	peers := c.peers
	if len(cs.Nodes) > 0 {
		if len(peers) > 0 {
			panic("cannot specify both newRaft(peers) and ConfigState.Nodes")
		}
		peers = cs.Nodes
	}
	r := &Raft{
		id:               c.ID,
		Lead:             None,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}
	for _, p := range peers {
		r.Prs[p] = &Progress{Next: 1}
	}
	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftLog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	log.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex())
	return r
}

// 超过半数的节点数量
func (r *Raft) quorum() int { return len(r.Prs)/2 + 1 }

func (r *Raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.Prs))
	for u := range r.Prs {
		nodes = append(nodes, u)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType == pb.MessageType_MsgRequestVote {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
		// 转发给leader的消息，不用管这个任期，当作本地消息处理
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// 向to节点发送RPC消息
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	m := pb.Message{
		To: to,
	}
	term, errt := r.RaftLog.Term(pr.Next - 1)
	ents, erre := r.RaftLog.sliceEntries(pr.Next, r.RaftLog.LastIndex()+1)
	if errt != nil || erre != nil {
		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.snapshot()
		if err != nil {
			if errors.Is(err, ErrSnapshotTemporarilyUnavailable) {
				log.Debugf("%v failed to send snapshot to %v because snapshot is temporatily unavailable", r.id, to)
				return false
			}
			panic(err)
		}
		if IsEmptySnap(&snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = &snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		log.Debugf("%v [firstIndex: %v, commidId: %v] sent snapshot[index: %v, term: %v] to %v [%v]", r.id, r.RaftLog.firstIndex, r.RaftLog.committed, sindex, sterm, to, pr)
		// 这里要不要像etcd一样 改变progress的状态呢？
	} else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = pr.Next - 1
		m.LogTerm = term
		entries := make([]*pb.Entry, len(ents))
		for _, v := range ents {
			entries = append(entries, &v)
		}
		m.Entries = entries
		m.Commit = r.RaftLog.committed
		if n := len(m.Entries); n != 0 {
			pr.Next = m.Entries[n-1].Index + 1
		}
	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.send(pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  min(r.Prs[to].Match, r.RaftLog.committed),
	})
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {

	// Your Code Here (2A).
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.electionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
// 尝试commit当前的日志，如果commit日志索引发生变化了就返回true
func (r *Raft) maybeCommit() bool {
	mis := make(uint64Slice, 0, len(r.Prs))
	// 拿到当前所有节点的Match到数组中
	for id := range r.Prs {
		mis = append(mis, r.Prs[id].Match)
	}
	// 逆序排列
	sort.Sort(sort.Reverse(mis))
	// 排列之后拿到中位数的Match，因为如果这个位置的Match对应的Term也等于当前的Term
	// 说明有过半的节点至少comit了mci这个索引的数据，这样leader就可以以这个索引进行commit了
	mci := mis[r.quorum()-1]
	// raft日志尝试commit
	return r.RaftLog.maybeCommit(mci, r.Term)
}

// 重置raft的一些状态
func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.leadTransferee = None
	r.votes = make(map[uint64]bool)
	// 非leader节点 充值这个有意义麻？
	// TODO 为啥不管match？
	for id := range r.Prs {
		r.Prs[id] = &Progress{
			Next: r.RaftLog.LastIndex() + 1,
		}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}

}

// 批量append一堆entries
func (r *Raft) appendEntry(es ...pb.Entry) {
	li := r.RaftLog.LastIndex()
	for i := range es {
		// 设置这些entries的Term以及index
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.RaftLog.append(es...)
	// 更新本节点的Next以及Match索引
	r.Prs[r.id].maybeUpdate(r.RaftLog.LastIndex())
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	// append之后，尝试一下是否可以进行commit
	r.maybeCommit()
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	log.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.State = StateCandidate
	log.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	ents, err := r.RaftLog.sliceEntries(r.RaftLog.committed+1, r.RaftLog.LastIndex()+1)
	if err != nil {
		log.Panicf("unexpected error getting uncommitted entries (%v)", err)
	}
	// 变成leader之前，这里还有没commit的配置变化消息
	nconf := numOfPendingConf(ents)
	if nconf > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	//if nconf == 1 {
	//	r.pendingConf = true
	//}
	// 为什么成为leader之后需要传入一个空数据？
	r.appendEntry(pb.Entry{Data: nil})
	log.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *Raft) campaign() {
	var term uint64
	var voteMsg pb.MessageType
	r.becomeCandidate()
	voteMsg = pb.MessageType_MsgRequestVote
	term = r.Term + 1
	// 调用poll函数给自己投票，同时返回当前投票给本节点的节点数量
	if r.quorum() == r.poll(r.id, pb.MessageType_MsgRequestVoteResponse, true) {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		// 有半数投票，说明通过，切换到下一个状态
		// 如果给自己投票之后，刚好超过半数的通过，那么就成为新的leader
		r.becomeLeader()
		return
	}

	// 向集群里的其他节点发送投票消息
	for id := range r.Prs {
		if id == r.id {
			// 过滤掉自己
			continue
		}
		log.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), voteMsg, id, r.Term)
		r.send(pb.Message{Term: term, To: id, MsgType: voteMsg, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.lastTerm()})
	}
}

// 轮询集群中所有节点，返回一共有多少节点已经进行了投票
func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		log.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		log.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	// 如果id没有投票过，那么更新id的投票情况
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	// 计算下都有多少节点已经投票给自己了
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	log.Infof("from:%d, to:%d, type:%s, term:%d, state:%v", m.From, m.To, m.MsgType, r.Term, r.State)

	// 先根据任期做一些统一处理
	switch {
	case m.Term == 0:
		// 收到来自本地的消息
	case m.Term > r.Term:
		lead := m.From
		if m.MsgType == pb.MessageType_MsgRequestVote {
			if r.Lead != None && r.electionElapsed < r.electionTimeout {
				log.Infof("lease is not expired")
				return nil
			}
			lead = None
		}
		log.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:
		log.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		return nil
	}

	// 投票类消息统一处理
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			ents, err := r.RaftLog.sliceEntries(r.RaftLog.applied+1, r.RaftLog.committed+1)
			if err != nil {
				log.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			// 如果其中有config消息，并且commited > applied，说明当前还有没有apply的config消息，这种情况下不能开始投票 WHY?
			if n := numOfPendingConf(ents); n != 0 && r.RaftLog.committed > r.RaftLog.applied {
				log.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			log.Infof("%x is starting a new election at term %d", r.id, r.Term)
			// 进行选举
			r.campaign()
		} else {
			log.Debugf("%x ignoring MsgHup because already leader", r.id)
		}
	case pb.MessageType_MsgRequestVote:
		if (r.Vote == None || r.Term < m.Term || r.Vote == m.From) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			log.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse})
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			// 否则拒绝投票
			log.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	}

	// 各自处理
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {

	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {

	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {

	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {

	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {

	// Your Code Here (3A).
}

func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		log.Panicf("%v state.commit %v is out of range [%v, %v]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.Term = state.Term
	r.Vote = state.Vote
	r.RaftLog.committed = state.Commit
}

// 返回消息数组中配置变化的消息数量
func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].EntryType == pb.EntryType_EntryConfChange {
			n++
		}
	}
	return n
}
