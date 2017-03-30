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

import "sync"
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"



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

const (
	FOLLOWER = 1
	CANDIDATE = 2
	LEADER = 3
)

type LogEntry struct {
	Index int
	Term int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role int // FOLLOWER, CANDIDATE or LEADER

	// Persistent state on all servers
	currentTerm int
	votedFor int
	log[] LogEntry // Start from index 1
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	// Volatile state on leaders
	nextIndex[] int
	matchIndex[] int


	electTimer *time.Timer
	heartBeatTimer *time.Timer
	r *rand.Rand

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = (rf.role == LEADER)
	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		DPrintf("%s: Peer %d is becoming FOLLOWER.\n", time.Now().String(), rf.me)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.VoteGranted = true
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if reply.VoteGranted {
		candidateUpToDate := false
		if args.LastLogTerm > rf.log[len(rf.log) - 1].Term {
			candidateUpToDate = true
		} else if args.LastLogTerm == rf.log[len(rf.log) - 1].Term {
			if args.LastLogIndex >= rf.log[len(rf.log) - 1].Index {
				candidateUpToDate = true
			}
		}
		if !((rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateUpToDate)  {
			reply.VoteGranted = false;

		}
	}
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.ResetElectTimer()
	}
	reply.Term = rf.currentTerm
}



type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries[] LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		DPrintf("%s: Peer %d is becoming FOLLOWER.\n", time.Now().String(), rf.me)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Success = true
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if reply.Success {
		if args.PrevLogIndex + 1 > len(rf.log) {
			reply.Success = false
		} else {
			if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Success = false;
			}
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	for _, entry := range args.Entries {
		if entry.Index + 1 <= len(rf.log) {
			if entry.Term != rf.log[entry.Index].Term {
				rf.log = rf.log[:entry.Index]
				break
			}
		}

	}
	// 4. Append any new entries not already in the log
	for _, entry := range args.Entries {
		if entry.Index + 1 > len(rf.log) {
			rf.log = append(rf.log, entry)
		}
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = math.Min(args.leaderCommit, args.Entries[len(args.Entries) - 1].Index)
	}

	if reply.Success = true {
		rf.ResetElectTimer()
	}
	reply.Term = rf.currentTerm
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.role != LEADER {
		return index, term, false
	}

	logEntry := new(LogEntry)
	logEntry.Term = rf.currentTerm
	logEntry.Index = len(rf.log)
	logEntry.Command = command
	rf.log = append(rf.log, logEntry)


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

type AppendEntriesReplyContainer struct {
	peerIndex int
	reply *AppendEntriesReply
}

func (rf *Raft) RunSendAppendEntries(peerIndex int, args *AppendEntriesArgs, replyContainerChan chan *AppendEntriesReplyContainer) {
	DPrintf("%s: Peer %d is sending AppendEntries with Term %d to peer %d.\n", time.Now().String(), rf.me, args.Term, peerIndex)
	reply := new(AppendEntriesReply)
	replyContainer := new(AppendEntriesReplyContainer)
	replyContainer.peerIndex = peerIndex
	if rf.sendAppendEntries(peerIndex, args, reply) {
		replyContainer.reply = reply
	}
	replyContainerChan <- replyContainer

}

func (rf *Raft) ReceiveSendAppendEntriesReply(replyContainerChan chan *AppendEntriesReplyContainer) {
	nReply := 0
	for replyContainer := range replyContainerChan {
		reply := replyContainer.reply
		if (reply != nil) {
			DPrintf("%s: Peer %d is receiving AppendEntries with Term %d from peer %d.\n", time.Now().String(), rf.me, reply.Term, replyContainer.peerIndex)
			if reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.role = FOLLOWER
				DPrintf("%s: Peer %d is becoming FOLLOWER.\n", time.Now().String(), rf.me)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			}
		}
		nReply++
		if nReply == (len(rf.peers) - 1) {
			break
		}
	}
}

func (rf *Raft) OnHeartBeatTimeExpire() {
	replyContainerChan := make(chan *AppendEntriesReplyContainer, len(rf.peers) - 1)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := new(AppendEntriesArgs)
			args.Term = rf.currentTerm
			go rf.RunSendAppendEntries(i, args, replyContainerChan)
		}
	}
	go rf.ReceiveSendAppendEntriesReply(replyContainerChan)

	rf.heartBeatTimer.Reset(time.Millisecond * 100)
}

func (rf *Raft) ResetElectTimer() {
	rf.electTimer.Reset(time.Millisecond * 250 + time.Millisecond * time.Duration(rf.r.Intn(100)))
}

type RequestVoteReplyContainer struct {
	peerIndex int
	reply *RequestVoteReply
}

func (rf *Raft) RunSendRequestVote(peerIndex int, args *RequestVoteArgs, replyContainerChan chan *RequestVoteReplyContainer) {
	DPrintf("%s: Peer %d is sending RequestVote with Term %d and CandidateId %d to peer %d.\n", time.Now().String(), rf.me, args.Term, args.CandidateId, peerIndex)
	replyContainer := new(RequestVoteReplyContainer)
	replyContainer.peerIndex = peerIndex
	reply := new(RequestVoteReply)
	if rf.sendRequestVote(peerIndex, args, reply) {
		replyContainer.reply = reply
	}
	replyContainerChan <- replyContainer
}

func (rf *Raft) OnElectTimerExpire() {
	for {
		rf.mu.Lock()
		DPrintf("%s: Peer %d is becoming CANDIDATE with Term %d before increase.\n", time.Now().String(), rf.me, rf.currentTerm)
		rf.role = CANDIDATE
		rf.currentTerm = rf.currentTerm + 1
		rf.votedFor = rf.me
		voteCount := 1
		rf.mu.Unlock()

		replyContainerChan := make(chan *RequestVoteReplyContainer, len(rf.peers) - 1)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := new(RequestVoteArgs)
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				go rf.RunSendRequestVote(i, args, replyContainerChan)
			}
		}

		numReply := 0
		for replyContainer := range replyContainerChan {
			reply := replyContainer.reply
			if reply != nil {
				DPrintf("%s: Peer %d is receiving RequestVote with Term %d and VoteGranted %t from peer %d.\n", time.Now().String(), rf.me, reply.Term, reply.VoteGranted, replyContainer.peerIndex)
				if reply.VoteGranted {
					voteCount++
					if voteCount * 2 > len(rf.peers) {
						break;
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.role = FOLLOWER
						DPrintf("%s: Peer %d is becoming FOLLOWER.\n", time.Now().String(), rf.me)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.ResetElectTimer()
						rf.mu.Unlock()
						return
					}
				}
			}

			numReply++
			if numReply == (len(rf.peers) - 1) {
				break;
			}
		}

		if voteCount * 2 > len(rf.peers) {
			rf.mu.Lock()
			rf.role = LEADER
			DPrintf("%s: Peer %d is becoming LEADER.\n", time.Now().String(), rf.me)
			rf.heartBeatTimer = time.AfterFunc(time.Millisecond * 100, rf.OnHeartBeatTimeExpire)
			rf.mu.Unlock()
			return

		} else {
			// Sleep until timeout.
			time.Sleep(time.Millisecond * time.Duration(rf.r.Intn(100)))
		}
	}


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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	// Set up the election timer between 300 and 600 ms.
	rf.electTimer = time.AfterFunc(time.Millisecond * 300 + time.Millisecond * time.Duration(rf.r.Intn(100)), rf.OnElectTimerExpire)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
