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
import "math/rand"
import "time"
import "fmt"


// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


	//Persistent State
	currentTerm int //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor int //candidateId that received vote in current term (or null if none)
	log_entries []string // each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile State
	commitIndex	int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	isLeader bool
	lastTimeoutTime int
	timeoutTime int


	//Volatile state on leaders: (Reinitialized after election)
	nextIndex []string  //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []string  //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)




}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	// PrevLogIndex int
	// PrevLogTerm int
	// Entries []string
	// LeaderCommit int

}

type AppendEntriesReply struct {
	//Args and reply
	Term int
	Success int
}


type RequestVoteArgs struct {
	// Your data here (3A, 3B).
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
	// Your data here (3A).
	Term int
	VoteGranted bool
}


func (rf *Raft) AppendRPCHandler(args *AppendEntriesArgs) {
	//resets the election timeout.
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//args=information we have on this prospective leader (i.e., candidate)
	//reply=information we have about follower's response to candidate requested vote

	 //Reply false if term < currentTerm
	 if (args.Term<rf.currentTerm) {
		 reply.VoteGranted=false
		 reply.Term=rf.currentTerm
		 }

		// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if (rf.votedFor==0) {
			reply.VoteGranted=true
			rf.votedFor=args.CandidateId
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

func (rf *Raft) sendAppendRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendRPCHandler", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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

	// Your code here (3B).

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
	rf.currentTerm=0

	// Your initialization code here (3A, 3B, 3C).
	//Create a background goroutine in Make() to periodically kick off leader election by:
	 //sending out RequestVote RPCs when it hasn't heard from another peer for a while. This way, if there is already a leader the peer will learn about it, or become leader itself.
	 rf.timeoutTime=rand.Intn(500)+500
	 rf.lastTimeoutTime=int(time.Now().UnixNano())/int(time.Millisecond)
	 fmt.Println(rf.lastTimeoutTime)


	 // time.Sleep(interval* time.Millisecond)

	 // go rf.StartElection()
	 go rf.test()
	 ///if I haven't heard from a leader in a while:
	 	// for peer in peers:
	 	//go sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply)



	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
func (rf *Raft) test() () {
	for(true) {
		fmt.Println("Here's the time")
		fmt.Println(int(time.Now().UnixNano())/int(time.Millisecond) - rf.lastTimeoutTime)
	}
}

func (rf *Raft) StartElection() () {
    fmt.Println(1)
    for i, _ := range rf.peers {
        args := &AppendEntriesArgs{rf.currentTerm,rf.votedFor}
        reply := &AppendEntriesReply{}
        rf.sendAppendRPC(i, args, reply)
        fmt.Println(reply)
    }
}
