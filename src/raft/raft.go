package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const (
	HEARTBEAT_TIME    = 50
	MIN_ELECTION_TIME = 150
	MAX_ELECTION_TIME = 300
	STATE_LEADER      = 1
	STATE_FOLLOWER    = 2
	STATE_CANDIDATE   = 4
)

var seedSet bool = false

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)
	CurrentTerm int        //服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	VotedFor    int        //当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	Log         []LogEntry //日志条目

	//所有服务器上的易失性状态
	CommitIndex int //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	LastApplied int //已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	//领导人（服务器）上的易失性状态 (选举后已经重新初始化)
	NextIndex  []int //对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	MatchIndex []int //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	ApplyCh chan ApplyMsg

	State int //当前服务器状态 leader follower candidate
	ElectionTimer,
	HeartBeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int = rf.CurrentTerm
	var isLeader bool = rf.State == STATE_LEADER
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
}

//由领导人调用，用于日志条目的复制，同时也被当做心跳使用
type AppendEntriesArgs struct {
	Term, //领导人的任期
	LeaderId, //领导人 ID 因此跟随者可以对客户端进行重定向（译者注：跟随者根据领导人 ID 把客户端的请求重定向到领导人，比如有时客户端把请求发给了跟随者而不是领导人）
	PrevLogIndex, //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm, //紧邻新日志条目之前的那个日志条目的任期
	LeaderCommit int //需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	Entries []LogEntry //领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  //当前任期，对于领导人而言 它会更新自己的任期
	Success bool //如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("node %d pre-lock AppendEntries", rf.me)
	rf.mu.Lock()
	DPrintf("node %d lock AppendEntries", rf.me)
	defer DPrintf("node %d unlock AppendEntries", rf.me)
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	reply.Success = false
	if args.Term < rf.CurrentTerm {
		//如果term < currentTerm返回 false
		DPrintf("node %d receive a outdate AppendEntries", rf.me)
		return
	}
	DPrintf("node %d before rf.resetElectionTimer()", rf.me)
	rf.resetElectionTimer()
	DPrintf("node %d after rf.resetElectionTimer()", rf.me)
	if args.Term > rf.CurrentTerm {
		//如果接收到的 RPC 请求或响应中，任期号T > currentTerm，则令 currentTerm = T，并切换为跟随者状态（5.1 节）
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.toFollower(args.Term, true)
	}
	return
	//TODO
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// 由候选人负责调用用来征集选票
//
type RequestVoteArgs struct {
	// Your data here.
	Term, //候选人的任期号
	CandidateId, //请求选票的候选人的 ID
	LastLogIndex, //候选人的最后日志条目的索引值
	LastLogTerm int //候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

//
// example RequestVote RPC handler.
// 这里是接受者行为
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	DPrintf("node %d pre-lock RequestVote", rf.me)
	rf.mu.Lock()
	DPrintf("node %d lock RequestVote", rf.me)
	defer DPrintf("node %d unlock RequestVote", rf.me)
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm {
		//如果term < currentTerm返回 false
		return
	}
	rf.resetElectionTimer()
	if args.Term > rf.CurrentTerm {
		//如果接收到的 RPC 请求或响应中，任期号T > currentTerm，则令 currentTerm = T，并切换为跟随者状态（5.1 节）
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.toFollower(args.Term, false)
	}
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
		!JudgeLogNew(rf.Log[len(rf.Log)-1].Term, len(rf.Log), args.LastLogTerm, args.LastLogIndex) {
		//如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
		//两个日志文件谁的日志更新是通过比较日志中最后一条日志记录的任期和索引。
		//如果两个日志文件的最后一条日志的任期不相同，谁的任期更大谁的的日志将更新。
		//如果两条日志记录的任期相同，那么谁的索引越大，谁的日志将更新。
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		rf.toFollower(args.Term, true)
	}
}

func JudgeLogNew(term1, index1, term2, index2 int) bool {
	//true意味着 1比2新
	//false表示  1和2一样新或1更旧
	if term1 != term2 {
		return term1 > term2
	} else {
		return index1 > index2
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) resetElectionTimer() {
	//DPrintf("resetElectionTimer() 1")
	//if !rf.ElectionTimer.Stop() {
	//	//Stop返回false时说明已经超时了
	//	//DPrintf("resetElectionTimer() 2")
	//	//<-rf.ElectionTimer.C
	//}
	DPrintf("resetElectionTimer() 1")
	rf.ElectionTimer.Stop()
	DPrintf("resetElectionTimer() 2")
	rf.ElectionTimer.Reset(time.Duration(TimeoutTimerRandTime()) * time.Millisecond)
}

func (rf *Raft) toFollower(term int, reset bool) {
	if rf.State == STATE_LEADER {
		rf.HeartBeatTimer.Stop()
	}
	rf.State = STATE_FOLLOWER
	rf.CurrentTerm = term
	rf.persist()
	if reset {
		rf.resetElectionTimer()
	}
	DPrintf("node %d to follower with term %d, reset states: %t\n", rf.me, rf.CurrentTerm, reset)
}

func (rf *Raft) toCandidate() {
	if rf.State == STATE_LEADER {
		rf.HeartBeatTimer.Stop()
	}
	rf.State = STATE_CANDIDATE
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	DPrintf("node %d to Candidate\n", rf.me)
}

func (rf *Raft) toLeader() {
	rf.State = STATE_LEADER
	rf.persist()
	for i := 0; i < len(rf.peers); i++ {
		rf.MatchIndex[i] = 0
		rf.NextIndex[i] = len(rf.Log)
	}
	rf.HeartBeatTimer = time.NewTimer(HEARTBEAT_TIME * time.Millisecond)
	go rf.HeartBeatLoop()
	DPrintf("node %d to Leader\n", rf.me)
}

func TimeoutTimerRandTime() int {
	//RandElectionTime := MIN_ELECTION_TIME + rand.Intn(MAX_ELECTION_TIME-MIN_ELECTION_TIME)
	return MIN_ELECTION_TIME + rand.Intn(MAX_ELECTION_TIME-MIN_ELECTION_TIME)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//Debug = 1
	if !seedSet {
		rand.Seed(time.Now().UnixNano())
		seedSet = true
		DPrintf("set seed\n")
	}

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 1)
	rf.Log[0] = LogEntry{
		Command: nil,
		Term:    0,
	}
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.CommitIndex = 0
	rf.LastApplied = 0

	rf.ApplyCh = applyCh
	rf.ElectionTimer = time.NewTimer(HEARTBEAT_TIME * time.Millisecond)
	rf.toFollower(rf.CurrentTerm, true)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.ElectionLoop()
	return rf
}

func (rf *Raft) ElectionLoop() {
	for {
		select {
		case <-rf.ElectionTimer.C:
			rf.resetElectionTimer()
			DPrintf("node %d's electionTimer out\n", rf.me)
			if rf.State == STATE_LEADER {
				continue
			}
			DPrintf("node %d pre-lock ElectionLoop", rf.me)
			rf.mu.Lock()
			DPrintf("node %d lock ElectionLoop", rf.me)
			rf.toCandidate()
			var count = 1
			electionReplyHandler := func(reply *RequestVoteReply) {
				if rf.State == STATE_CANDIDATE {
					if reply.Term > rf.CurrentTerm {
						rf.toFollower(reply.Term, true)
						return
					}
					if reply.VoteGranted {
						count++
						if count >= (len(rf.peers)+1)/2 {
							if count == (len(rf.peers)+1)/2 {
								rf.toLeader()
							}
							return
						}
					}
				}
			}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(n int) {
						var reply RequestVoteReply
						var args RequestVoteArgs
						args = RequestVoteArgs{
							Term:         rf.CurrentTerm,
							CandidateId:  rf.me,
							LastLogIndex: len(rf.Log),
							LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
						}
						if rf.sendRequestVote(n, args, &reply) {
							if reply.VoteGranted {
								DPrintf("node %d agree for node %d\n", n, rf.me)
							} else {
								DPrintf("node %d disagree for node %d\n", n, rf.me)
							}
							electionReplyHandler(&reply)
						}
					}(i)
				}
			}
			rf.mu.Unlock()
			DPrintf("node %d unlock ElectionLoop", rf.me)
		}
	}
}

func (rf *Raft) HeartBeatLoop() {
	for {
		rf.mu.Lock()
		//DPrintf("node %d lock HeartBeatLoop", rf.me)
		if rf.State != STATE_LEADER {
			rf.mu.Unlock()
			//rf.HeartBeatTimer.Stop()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(n int) {
					var args AppendEntriesArgs
					var reply AppendEntriesReply
					args = AppendEntriesArgs{
						Term:         rf.CurrentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0, //TODO
						PrevLogTerm:  0, //TODO
						LeaderCommit: 0, //TODO
						Entries:      nil,
					}
					if rf.sendAppendEntries(n, args, &reply) {
						DPrintf("node %d sendAppendEntries to node %d success\n", rf.me, n)
						//TODO 处理reply
					}
				}(i)
			} else {
				//rf.resetElectionTimer()
			}
		}
		rf.mu.Unlock()
		<-rf.HeartBeatTimer.C
		rf.HeartBeatTimer.Reset(HEARTBEAT_TIME * time.Millisecond)
	}
}
