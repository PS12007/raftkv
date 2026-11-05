// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", and Ongaro's PhD
// dissertation for single-server membership changes).
//
// The implementation is a deterministic, single-threaded state machine. It
// owns no goroutines, timers or sockets: the caller drives it by calling
// Tick (logical clock), Step (deliver a message) and Propose, and then calls
// Flush to persist state and collect outgoing messages and committed entries.
// This makes the exact same code usable both from a real networked server and
// from a seeded discrete-event simulator.
package raft

import "fmt"

// NodeID identifies a Raft peer. Zero means "none".
type NodeID = uint64

// None is the zero NodeID.
const None NodeID = 0

// MsgType enumerates Raft RPCs. Raft RPCs are modelled as one-way messages;
// the "response" to an RPC is simply another message.
type MsgType uint8

const (
	// MsgVote is RequestVote.
	MsgVote MsgType = iota + 1
	// MsgVoteResp is the RequestVote reply.
	MsgVoteResp
	// MsgApp is AppendEntries (also used as heartbeat).
	MsgApp
	// MsgAppResp is the AppendEntries reply (also replies to MsgSnap).
	MsgAppResp
	// MsgSnap is InstallSnapshot (sent whole, not chunked).
	MsgSnap
	// MsgPreVote asks whether a node would vote for us in the next term,
	// without anyone changing term (dissertation §9.6).
	MsgPreVote
	// MsgPreVoteResp is the PreVote reply.
	MsgPreVoteResp
	// MsgHeartbeat is an entry-less leadership check used to confirm
	// ReadIndex requests. It carries a commit index that is safe for the
	// follower (min of the leader's commit and that follower's match).
	MsgHeartbeat
	// MsgHeartbeatResp is the MsgHeartbeat reply.
	MsgHeartbeatResp
)

func (t MsgType) String() string {
	switch t {
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgApp:
		return "App"
	case MsgAppResp:
		return "AppResp"
	case MsgSnap:
		return "Snap"
	case MsgPreVote:
		return "PreVote"
	case MsgPreVoteResp:
		return "PreVoteResp"
	case MsgHeartbeat:
		return "Heartbeat"
	case MsgHeartbeatResp:
		return "HeartbeatResp"
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

// EntryType distinguishes log entry kinds.
type EntryType uint8

const (
	// EntryNormal carries an application command.
	EntryNormal EntryType = iota
	// EntryNoop is appended by every new leader (§8 of the paper) so that it
	// can commit entries from previous terms.
	EntryNoop
	// EntryConfig carries a full membership list (single-server change).
	EntryConfig
)

// Entry is a log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// Member is a cluster member.
type Member struct {
	ID   NodeID `json:"id"`
	Addr string `json:"addr"`
}

// Snapshot is a state machine snapshot plus the Raft metadata it covers.
type Snapshot struct {
	Index   uint64 // last included index
	Term    uint64 // last included term
	Members []Member
	Data    []byte
}

// Message is a Raft RPC.
type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	Term uint64

	// MsgVote
	LastLogIndex uint64
	LastLogTerm  uint64
	// MsgVoteResp
	Granted bool

	// MsgApp
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64

	// MsgAppResp
	Success       bool
	MatchIndex    uint64
	ConflictIndex uint64
	ConflictTerm  uint64

	// MsgSnap
	Snapshot *Snapshot

	// Context tags leader heartbeats for ReadIndex; followers echo it in
	// their MsgAppResp so the leader can confirm it is still leader.
	Context uint64
}

// ReadState reports that a ReadIndex request (identified by Ctx) has been
// confirmed: it is linearizable to read the state machine once it has
// applied Index.
type ReadState struct {
	Ctx   uint64
	Index uint64
}

func (m Message) String() string {
	switch m.Type {
	case MsgPreVote:
		return fmt.Sprintf("%d->%d PreVote t=%d last=%d/%d", m.From, m.To, m.Term, m.LastLogIndex, m.LastLogTerm)
	case MsgPreVoteResp:
		return fmt.Sprintf("%d->%d PreVoteResp t=%d granted=%v", m.From, m.To, m.Term, m.Granted)
	case MsgVote:
		return fmt.Sprintf("%d->%d Vote t=%d last=%d/%d", m.From, m.To, m.Term, m.LastLogIndex, m.LastLogTerm)
	case MsgVoteResp:
		return fmt.Sprintf("%d->%d VoteResp t=%d granted=%v", m.From, m.To, m.Term, m.Granted)
	case MsgApp:
		return fmt.Sprintf("%d->%d App t=%d prev=%d/%d n=%d commit=%d", m.From, m.To, m.Term, m.PrevLogIndex, m.PrevLogTerm, len(m.Entries), m.LeaderCommit)
	case MsgAppResp:
		return fmt.Sprintf("%d->%d AppResp t=%d ok=%v match=%d conflict=%d/%d", m.From, m.To, m.Term, m.Success, m.MatchIndex, m.ConflictIndex, m.ConflictTerm)
	case MsgSnap:
		return fmt.Sprintf("%d->%d Snap t=%d idx=%d/%d", m.From, m.To, m.Term, m.Snapshot.Index, m.Snapshot.Term)
	}
	return "?"
}

// HardState is the state Raft must persist before responding to RPCs.
type HardState struct {
	Term     uint64
	VotedFor NodeID
}

// ApplyMsg is handed to the service once an entry is committed, or when the
// service must replace its state with a snapshot.
type ApplyMsg struct {
	// Exactly one of Entry or Snapshot is set.
	Entry    *Entry
	Snapshot *Snapshot
}

// StateType is the role of a node.
type StateType uint8

const (
	Follower StateType = iota
	Candidate
	Leader
	PreCandidate
)

func (s StateType) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case PreCandidate:
		return "pre-candidate"
	}
	return "?"
}
