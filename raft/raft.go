package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sort"
)

// Errors returned by Propose and ProposeConfChange.
var (
	ErrNotLeader         = errors.New("raft: not leader")
	ErrConfChangePending = errors.New("raft: a membership change is already in progress")
	ErrNotReady          = errors.New("raft: leader has not committed an entry in its term yet")
	ErrBadConfChange     = errors.New("raft: invalid membership change")
)

// Config configures a Raft node.
type Config struct {
	ID NodeID
	// Peers is the bootstrap membership, including this node. It is used
	// only if Storage is empty. A node started with no Peers and empty
	// storage waits to be added to an existing cluster.
	Peers []Member
	// ElectionTicks is the minimum election timeout; the actual timeout is
	// randomised in [ElectionTicks, 2*ElectionTicks).
	ElectionTicks int
	// HeartbeatTicks is the heartbeat interval.
	HeartbeatTicks int
	// MaxEntriesPerMsg caps the number of entries in one AppendEntries.
	MaxEntriesPerMsg int
	Storage          Storage
	// Rand drives election timeout randomisation. Seed it for determinism.
	Rand *rand.Rand
	// PreVote enables the Pre-Vote phase: a node that times out first asks
	// whether it could win before incrementing its term, so a node that
	// was partitioned or restarted cannot depose a healthy leader.
	PreVote bool
	// DisableCheckQuorum stops a leader from stepping down when it loses
	// contact with a majority. Check-quorum is an optimisation; safety (and
	// ReadIndex) must not depend on it, so the simulator turns it off for
	// some seeds.
	DisableCheckQuorum bool
	// Logf, if set, receives debug output.
	Logf func(format string, args ...any)
}

type readReq struct {
	ctx, index uint64
	acks       map[NodeID]bool
}

type progress struct {
	match, next  uint64
	inflight     bool // an AppendEntries/InstallSnapshot is outstanding
	recentActive bool // heard from since the last quorum check
}

// Raft is a single Raft peer. It is not safe for concurrent use.
type Raft struct {
	id       NodeID
	state    StateType
	term     uint64
	votedFor NodeID
	leader   NodeID

	log     raftLog
	commit  uint64
	applied uint64 // highest index handed to the service by Flush

	members     []Member // sorted by ID; latest config in the log
	configIndex uint64   // index of the entry that set members (0 = snapshot)

	electionTicks, heartbeatTicks, maxEntries int
	electionElapsed, heartbeatElapsed         int
	randomizedTimeout                         int

	prs   map[NodeID]*progress
	votes map[NodeID]bool

	msgs         []Message
	reads        []*readReq  // leader: ReadIndex requests awaiting a quorum
	readStates   []ReadState // confirmed reads, drained by ReadStates()
	readBcast    bool        // a heartbeat round must go out on Flush
	replyCtx     uint64      // follower: Context to echo in MsgAppResp
	pendingSnap  *Snapshot   // snapshot to hand to the service on next Flush
	hsDirty      bool
	logDirtyFrom uint64
	snapDirty    bool

	preVote     bool
	checkQuorum bool
	storage     Storage
	rand        *rand.Rand
	logf        func(string, ...any)
}

// New creates a Raft node, restoring any persisted state.
func New(c Config) (*Raft, error) {
	if c.ElectionTicks <= 0 {
		c.ElectionTicks = 10
	}
	if c.HeartbeatTicks <= 0 {
		c.HeartbeatTicks = 2
	}
	if c.MaxEntriesPerMsg <= 0 {
		c.MaxEntriesPerMsg = 1024
	}
	if c.Rand == nil {
		c.Rand = rand.New(rand.NewSource(int64(c.ID)))
	}
	if c.Storage == nil {
		return nil, errors.New("raft: Storage is required")
	}
	r := &Raft{
		id:             c.ID,
		electionTicks:  c.ElectionTicks,
		heartbeatTicks: c.HeartbeatTicks,
		maxEntries:     c.MaxEntriesPerMsg,
		storage:        c.Storage,
		preVote:        c.PreVote,
		checkQuorum:    !c.DisableCheckQuorum,
		rand:           c.Rand,
		logf:           c.Logf,
	}
	hs, snap, ents, err := c.Storage.Load()
	if err != nil {
		return nil, err
	}
	r.term, r.votedFor = hs.Term, hs.VotedFor
	if snap != nil {
		r.log.snap = *snap
		if snap.Index > 0 {
			s := *snap
			r.pendingSnap = &s
		}
	} else if len(ents) == 0 && hs.Term == 0 && len(c.Peers) > 0 {
		// Bootstrap: the initial configuration is recorded as an empty
		// snapshot at index 0, identically on every initial member.
		r.log.snap = Snapshot{Members: sortMembers(c.Peers)}
		r.snapDirty = true
	}
	r.log.append(ents...)
	r.commit, r.applied = r.log.snap.Index, r.log.snap.Index
	r.recomputeConfig()
	r.becomeFollower(r.term, None)
	return r, nil
}

func sortMembers(ms []Member) []Member {
	out := append([]Member(nil), ms...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Raft) debug(format string, args ...any) {
	if r.logf != nil {
		r.logf("[%d t%d %s] "+format, append([]any{r.id, r.term, r.state}, args...)...)
	}
}

// ---------- accessors ----------

// ID returns this node's ID.
func (r *Raft) ID() NodeID { return r.id }

// Term returns the current term.
func (r *Raft) Term() uint64 { return r.term }

// State returns the node's role.
func (r *Raft) State() StateType { return r.state }

// IsLeader reports whether this node believes it is the leader.
func (r *Raft) IsLeader() bool { return r.state == Leader }

// Leader returns the known leader, or None.
func (r *Raft) Leader() NodeID { return r.leader }

// Members returns the current (possibly uncommitted) membership.
func (r *Raft) Members() []Member { return append([]Member(nil), r.members...) }

// LogBytes returns the approximate size of the log since the last snapshot.
func (r *Raft) LogBytes() int { return r.log.bytes }

// Status is a read-only view of a node, used by the dashboard.
type Status struct {
	ID          NodeID              `json:"id"`
	State       string              `json:"state"`
	Term        uint64              `json:"term"`
	Leader      NodeID              `json:"leader"`
	VotedFor    NodeID              `json:"votedFor"`
	Commit      uint64              `json:"commit"`
	Applied     uint64              `json:"applied"`
	FirstIndex  uint64              `json:"firstIndex"`
	LastIndex   uint64              `json:"lastIndex"`
	LastTerm    uint64              `json:"lastTerm"`
	SnapIndex   uint64              `json:"snapIndex"`
	LogBytes    int                 `json:"logBytes"`
	Members     []Member            `json:"members"`
	ConfigIndex uint64              `json:"configIndex"`
	Progress    map[NodeID]Progress `json:"progress,omitempty"`
	// RecentTerms holds the terms of the last few log entries (for the
	// dashboard's log strip), oldest first, ending at LastIndex.
	RecentTerms []uint64 `json:"recentTerms"`
}

// Progress is a leader's view of a follower.
type Progress struct {
	Match uint64 `json:"match"`
	Next  uint64 `json:"next"`
}

// Status returns a snapshot of the node's state.
func (r *Raft) Status() Status {
	s := Status{
		ID: r.id, State: r.state.String(), Term: r.term, Leader: r.leader, VotedFor: r.votedFor,
		Commit: r.commit, Applied: r.applied, FirstIndex: r.log.firstIndex(), LastIndex: r.log.lastIndex(),
		LastTerm: r.log.lastTerm(), SnapIndex: r.log.snap.Index, LogBytes: r.log.bytes,
		Members: r.Members(), ConfigIndex: r.configIndex,
	}
	if r.state == Leader {
		s.Progress = map[NodeID]Progress{}
		for id, pr := range r.prs {
			s.Progress[id] = Progress{Match: pr.match, Next: pr.next}
		}
	}
	const n = 64
	lo := r.log.firstIndex()
	if r.log.lastIndex() >= n && r.log.lastIndex()-n+1 > lo {
		lo = r.log.lastIndex() - n + 1
	}
	for i := lo; i <= r.log.lastIndex(); i++ {
		t, _ := r.log.term(i)
		s.RecentTerms = append(s.RecentTerms, t)
	}
	return s
}

// ---------- membership ----------

func (r *Raft) isMember(id NodeID) bool {
	for _, m := range r.members {
		if m.ID == id {
			return true
		}
	}
	return false
}

func (r *Raft) quorum() int { return len(r.members)/2 + 1 }

// recomputeConfig sets members from the latest config entry in the log,
// falling back to the snapshot. A server always uses the latest
// configuration in its log, committed or not (dissertation §4.1).
func (r *Raft) recomputeConfig() {
	for i := r.log.lastIndex(); i > r.log.snap.Index; i-- {
		if e := r.log.entry(i); e.Type == EntryConfig {
			r.members = DecodeMembers(e.Data)
			r.configIndex = i
			r.syncProgress()
			return
		}
	}
	r.members = sortMembers(r.log.snap.Members)
	r.configIndex = 0
	r.syncProgress()
}

// syncProgress adds/removes leader progress trackers to match members.
func (r *Raft) syncProgress() {
	if r.state != Leader {
		return
	}
	keep := map[NodeID]bool{}
	for _, m := range r.members {
		keep[m.ID] = true
		if r.prs[m.ID] == nil {
			r.prs[m.ID] = &progress{next: r.log.lastIndex() + 1, recentActive: true}
		}
	}
	for id := range r.prs {
		if !keep[id] {
			delete(r.prs, id)
		}
	}
	if pr := r.prs[r.id]; pr != nil {
		pr.match = r.log.lastIndex()
		pr.next = pr.match + 1
	}
}

// configAt returns the membership in effect at index i (i <= lastIndex).
func (r *Raft) configAt(i uint64) []Member {
	for j := i; j > r.log.snap.Index; j-- {
		if e := r.log.entry(j); e.Type == EntryConfig {
			return DecodeMembers(e.Data)
		}
	}
	return sortMembers(r.log.snap.Members)
}

// EncodeMembers serialises a membership list for an EntryConfig.
func EncodeMembers(ms []Member) []byte {
	ms = sortMembers(ms)
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(ms)))
	for _, m := range ms {
		b = binary.AppendUvarint(b, m.ID)
		b = binary.AppendUvarint(b, uint64(len(m.Addr)))
		b = append(b, m.Addr...)
	}
	return b
}

// DecodeMembers is the inverse of EncodeMembers.
func DecodeMembers(b []byte) []Member {
	n, k := binary.Uvarint(b)
	b = b[k:]
	ms := make([]Member, 0, n)
	for i := uint64(0); i < n; i++ {
		id, k := binary.Uvarint(b)
		b = b[k:]
		l, k := binary.Uvarint(b)
		b = b[k:]
		ms = append(ms, Member{ID: id, Addr: string(b[:l])})
		b = b[l:]
	}
	return ms
}

// ---------- state transitions ----------

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	r.randomizedTimeout = r.electionTicks + r.rand.Intn(r.electionTicks)
}

func (r *Raft) setTerm(t uint64) {
	if t != r.term {
		r.term = t
		r.votedFor = None
		r.hsDirty = true
	}
}

func (r *Raft) becomeFollower(term uint64, leader NodeID) {
	if r.state != Follower || term != r.term {
		r.debug("-> follower term=%d leader=%d", term, leader)
	}
	r.setTerm(term)
	r.state = Follower
	r.leader = leader
	r.prs = nil
	r.votes = nil
	r.reads = nil
	r.readBcast = false
	r.resetElectionTimer()
}

func (r *Raft) campaign() {
	r.setTerm(r.term + 1)
	r.state = Candidate
	r.leader = None
	r.votedFor = r.id
	r.hsDirty = true
	r.votes = map[NodeID]bool{r.id: true}
	r.resetElectionTimer()
	r.debug("campaign last=%d/%d", r.log.lastIndex(), r.log.lastTerm())
	if r.countVotes() >= r.quorum() {
		r.becomeLeader()
		return
	}
	for _, m := range r.members {
		if m.ID == r.id {
			continue
		}
		r.send(Message{Type: MsgVote, To: m.ID, LastLogIndex: r.log.lastIndex(), LastLogTerm: r.log.lastTerm()})
	}
}

func (r *Raft) preCampaign() {
	r.state = PreCandidate
	r.leader = None
	r.votes = map[NodeID]bool{r.id: true}
	r.resetElectionTimer()
	r.debug("pre-campaign last=%d/%d", r.log.lastIndex(), r.log.lastTerm())
	if r.countVotes() >= r.quorum() {
		r.campaign()
		return
	}
	for _, m := range r.members {
		if m.ID != r.id {
			r.send(Message{Type: MsgPreVote, To: m.ID, Term: r.term + 1, LastLogIndex: r.log.lastIndex(), LastLogTerm: r.log.lastTerm()})
		}
	}
}

func (r *Raft) countVotes() int {
	n := 0
	for _, m := range r.members {
		if r.votes[m.ID] {
			n++
		}
	}
	return n
}

func (r *Raft) becomeLeader() {
	r.debug("-> leader")
	r.state = Leader
	r.leader = r.id
	r.votes = nil
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.prs = map[NodeID]*progress{}
	r.syncProgress()
	// A no-op entry lets the leader commit entries from earlier terms (§5.4.2)
	// and learn the commit index promptly.
	r.appendEntry(Entry{Type: EntryNoop})
	r.broadcastAppend(true)
}

// ---------- driving the state machine ----------

// promotable reports whether this node may campaign.
func (r *Raft) promotable() bool { return r.isMember(r.id) }

// Tick advances the logical clock by one tick.
func (r *Raft) Tick() {
	r.electionElapsed++
	switch r.state {
	case Leader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTicks {
			r.heartbeatElapsed = 0
			r.broadcastAppend(true)
		}
		if r.electionElapsed >= r.electionTicks {
			r.electionElapsed = 0
			if r.checkQuorum {
				r.checkQuorumNow()
			}
		}
	default:
		if r.electionElapsed >= r.randomizedTimeout {
			if r.promotable() && r.preVote {
				r.preCampaign()
			} else if r.promotable() {
				r.campaign()
			} else {
				r.resetElectionTimer()
			}
		}
	}
}

// checkQuorumNow makes a leader step down if it has not heard from a majority
// within an election timeout. It lets clients find the real leader quickly
// when an old leader is partitioned away.
func (r *Raft) checkQuorumNow() {
	active := 0
	for _, m := range r.members {
		pr := r.prs[m.ID]
		if m.ID == r.id || (pr != nil && pr.recentActive) {
			active++
		}
		if pr != nil {
			pr.recentActive = false
		}
	}
	if active < r.quorum() {
		r.debug("check quorum failed (%d active)", active)
		r.becomeFollower(r.term, None)
	}
}

// inLease reports whether this node has recently heard from a leader. Such a
// node ignores RequestVote from other candidates (dissertation §4.2.3), so a
// removed or partitioned server cannot disrupt a healthy cluster.
func (r *Raft) inLease() bool {
	return r.leader != None && r.electionElapsed < r.electionTicks
}

// Step delivers a message to the node.
func (r *Raft) Step(m Message) {
	if m.To != r.id {
		return
	}
	switch {
	case m.Term > r.term:
		if (m.Type == MsgVote || m.Type == MsgPreVote) && r.inLease() {
			r.debug("ignore %v from %d in lease", m.Type, m.From)
			return
		}
		switch {
		case m.Type == MsgPreVote:
			// Never change term because of a pre-vote request.
		case m.Type == MsgPreVoteResp && m.Granted:
			// Granted pre-votes carry the term we are about to campaign in.
		default:
			leader := None
			if m.Type == MsgApp || m.Type == MsgSnap || m.Type == MsgHeartbeat {
				leader = m.From
			}
			r.becomeFollower(m.Term, leader)
		}
	case m.Term < r.term:
		// Stale sender: tell it about the newer term.
		switch m.Type {
		case MsgApp, MsgSnap, MsgHeartbeat:
			r.send(Message{Type: MsgAppResp, To: m.From, Success: false})
		case MsgVote:
			r.send(Message{Type: MsgVoteResp, To: m.From, Granted: false})
		case MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Granted: false})
		}
		return
	}

	switch m.Type {
	case MsgVote:
		r.handleVote(m)
	case MsgPreVote:
		upToDate := m.LastLogTerm > r.log.lastTerm() ||
			(m.LastLogTerm == r.log.lastTerm() && m.LastLogIndex >= r.log.lastIndex())
		if m.Term > r.term && upToDate {
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Term: m.Term, Granted: true})
		} else {
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Granted: false})
		}
	case MsgPreVoteResp:
		if r.state == PreCandidate && (!m.Granted || m.Term == r.term+1) {
			r.votes[m.From] = m.Granted
			if r.countVotes() >= r.quorum() {
				r.campaign()
			}
		}
	case MsgVoteResp:
		if r.state == Candidate {
			r.votes[m.From] = m.Granted
			if r.countVotes() >= r.quorum() {
				r.becomeLeader()
			}
		}
	case MsgApp:
		if r.state == Leader {
			panic(fmt.Sprintf("raft: two leaders in term %d (%d and %d)", r.term, r.id, m.From))
		}
		r.becomeFollowerOf(m.From)
		r.replyCtx = m.Context
		r.handleAppend(m)
		r.replyCtx = 0
	case MsgSnap:
		if r.state == Leader {
			panic(fmt.Sprintf("raft: two leaders in term %d (%d and %d)", r.term, r.id, m.From))
		}
		r.becomeFollowerOf(m.From)
		r.replyCtx = m.Context
		r.handleSnapshot(m)
		r.replyCtx = 0
	case MsgHeartbeat:
		if r.state == Leader {
			panic(fmt.Sprintf("raft: two leaders in term %d (%d and %d)", r.term, r.id, m.From))
		}
		r.becomeFollowerOf(m.From)
		// LeaderCommit was capped at our match index by the leader, so the
		// entries up to it are known to match the leader's log.
		if m.LeaderCommit > r.commit && m.LeaderCommit <= r.log.lastIndex() {
			r.commit = m.LeaderCommit
		}
		r.send(Message{Type: MsgHeartbeatResp, To: m.From, Context: m.Context})
	case MsgHeartbeatResp:
		if r.state == Leader {
			if pr := r.prs[m.From]; pr != nil {
				pr.recentActive = true
				if m.Context != 0 {
					r.ackReads(m.From, m.Context)
				}
				if !pr.inflight && pr.next <= r.log.lastIndex() {
					r.sendAppend(m.From, false)
				}
			}
		}
	case MsgAppResp:
		if r.state == Leader {
			r.handleAppendResp(m)
		}
	}
}

// broadcastHeartbeat sends an entry-less leadership check to every peer.
func (r *Raft) broadcastHeartbeat() {
	for _, m := range r.members {
		if pr := r.prs[m.ID]; m.ID != r.id && pr != nil {
			r.send(Message{Type: MsgHeartbeat, To: m.ID, LeaderCommit: min(r.commit, pr.match)})
		}
	}
}

func (r *Raft) becomeFollowerOf(leader NodeID) {
	if r.state != Follower {
		r.becomeFollower(r.term, leader)
	}
	r.leader = leader
	r.electionElapsed = 0
}

func (r *Raft) handleVote(m Message) {
	canVote := r.votedFor == m.From || (r.votedFor == None && r.leader == None)
	upToDate := m.LastLogTerm > r.log.lastTerm() ||
		(m.LastLogTerm == r.log.lastTerm() && m.LastLogIndex >= r.log.lastIndex())
	grant := canVote && upToDate
	if grant {
		r.votedFor = m.From
		r.hsDirty = true
		r.resetElectionTimer()
	}
	r.debug("vote for %d: %v", m.From, grant)
	r.send(Message{Type: MsgVoteResp, To: m.From, Granted: grant})
}

func (r *Raft) handleAppend(m Message) {
	prev, prevTerm, ents := m.PrevLogIndex, m.PrevLogTerm, m.Entries
	// Entries already covered by our snapshot are committed and therefore
	// identical to the leader's; skip them.
	if prev < r.log.snap.Index {
		skip := r.log.snap.Index - prev
		if skip >= uint64(len(ents)) {
			r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: r.log.snap.Index})
			return
		}
		ents = ents[skip:]
		prev, prevTerm = r.log.snap.Index, r.log.snap.Term
	}
	if prev > r.log.lastIndex() {
		r.send(Message{Type: MsgAppResp, To: m.From, ConflictIndex: r.log.lastIndex() + 1})
		return
	}
	if t, _ := r.log.term(prev); t != prevTerm {
		r.send(Message{Type: MsgAppResp, To: m.From, ConflictTerm: t, ConflictIndex: r.log.firstIndexOfTermAt(prev)})
		return
	}
	for i := range ents {
		e := &ents[i]
		if e.Index > r.log.lastIndex() {
			r.appendFollower(ents[i:])
			break
		}
		if t, _ := r.log.term(e.Index); t != e.Term {
			if e.Index <= r.commit {
				panic(fmt.Sprintf("raft: %d truncating committed entry %d", r.id, e.Index))
			}
			r.log.truncateFrom(e.Index)
			r.markLogDirty(e.Index)
			r.appendFollower(ents[i:])
			r.recomputeConfig()
			break
		}
	}
	lastNew := prev + uint64(len(ents))
	if m.LeaderCommit > r.commit {
		r.commit = min(m.LeaderCommit, lastNew)
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew})
}

func (r *Raft) appendFollower(ents []Entry) {
	if len(ents) == 0 {
		return
	}
	r.markLogDirty(ents[0].Index)
	cfg := false
	for i := range ents {
		if ents[i].Type == EntryConfig {
			cfg = true
		}
	}
	r.log.append(append([]Entry(nil), ents...)...)
	if cfg {
		r.recomputeConfig()
	}
}

func (r *Raft) handleSnapshot(m Message) {
	s := *m.Snapshot
	if s.Index <= r.commit {
		r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: r.commit})
		return
	}
	r.debug("install snapshot %d/%d", s.Index, s.Term)
	r.log.compact(s)
	r.commit = s.Index
	r.applied = s.Index
	r.pendingSnap = &s
	r.snapDirty = true
	r.recomputeConfig()
	r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: s.Index})
}

func (r *Raft) handleAppendResp(m Message) {
	pr := r.prs[m.From]
	if pr == nil {
		return
	}
	pr.recentActive = true
	pr.inflight = false
	if m.Context != 0 {
		r.ackReads(m.From, m.Context)
	}
	if m.Success {
		if m.MatchIndex > pr.match {
			pr.match = m.MatchIndex
			r.maybeCommit()
			if r.state != Leader {
				return // stepped down after committing its own removal
			}
		}
		if pr.next < pr.match+1 {
			pr.next = pr.match + 1
		}
	} else {
		next := m.ConflictIndex
		if m.ConflictTerm != 0 {
			if idx := r.log.lastIndexOfTerm(m.ConflictTerm); idx != 0 {
				next = idx + 1
			}
		}
		next = min(next, r.log.lastIndex()+1)
		next = max(next, pr.match+1, 1)
		pr.next = next
	}
	if pr.next <= r.log.lastIndex() {
		r.sendAppend(m.From, false)
	}
}

func (r *Raft) maybeCommit() {
	for n := r.log.lastIndex(); n > r.commit; n-- {
		if t, _ := r.log.term(n); t != r.term {
			break // only entries from the current term are committed by counting (§5.4.2)
		}
		cnt := 0
		for _, mem := range r.members {
			if pr := r.prs[mem.ID]; pr != nil && pr.match >= n {
				cnt++
			}
		}
		if cnt >= r.quorum() {
			r.commit = n
			break
		}
	}
	// A leader that has committed its own removal steps down (§4.2.2).
	if r.configIndex <= r.commit && !r.isMember(r.id) {
		r.debug("removed from cluster; stepping down")
		r.becomeFollower(r.term, None)
	}
}

func (r *Raft) markLogDirty(i uint64) {
	if r.logDirtyFrom == 0 || i < r.logDirtyFrom {
		r.logDirtyFrom = i
	}
}

// appendEntry appends an entry to the leader's own log.
func (r *Raft) appendEntry(e Entry) uint64 {
	e.Term = r.term
	e.Index = r.log.lastIndex() + 1
	r.log.append(e)
	r.markLogDirty(e.Index)
	if e.Type == EntryConfig {
		r.recomputeConfig()
	}
	if pr := r.prs[r.id]; pr != nil {
		pr.match = e.Index
		pr.next = e.Index + 1
	}
	r.maybeCommit()
	return e.Index
}

func (r *Raft) send(m Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.term
	}
	switch {
	case m.Type == MsgAppResp:
		m.Context = r.replyCtx
	case m.Type == MsgHeartbeatResp:
		// Context already set from the heartbeat.
	case (m.Type == MsgApp || m.Type == MsgSnap || m.Type == MsgHeartbeat) && len(r.reads) > 0:
		// Every append/heartbeat carries the newest pending read's context;
		// an echo confirms all reads registered before it was sent.
		m.Context = r.reads[len(r.reads)-1].ctx
	}
	r.msgs = append(r.msgs, m)
}

// ReadIndex starts a linearizable read identified by ctx (which must be
// non-zero and increasing per node). When a majority confirms this node is
// still leader, a ReadState{ctx, index} is returned by ReadStates; the read
// may then be served once the state machine has applied index (§6.4 of the
// dissertation). It returns ErrNotReady if the leader has not yet committed
// an entry in its term; callers can fall back to a log read.
func (r *Raft) ReadIndex(ctx uint64) error {
	if r.state != Leader {
		return ErrNotLeader
	}
	if t, _ := r.log.term(r.commit); t != r.term {
		return ErrNotReady
	}
	rr := &readReq{ctx: ctx, index: r.commit, acks: map[NodeID]bool{r.id: true}}
	if r.countAcks(rr) >= r.quorum() {
		r.readStates = append(r.readStates, ReadState{Ctx: ctx, Index: rr.index})
		return nil
	}
	r.reads = append(r.reads, rr)
	r.readBcast = true
	return nil
}

func (r *Raft) countAcks(rr *readReq) int {
	n := 0
	for _, m := range r.members {
		if rr.acks[m.ID] {
			n++
		}
	}
	return n
}

// ackReads records that from has acknowledged leadership as of context c.
func (r *Raft) ackReads(from NodeID, c uint64) {
	for _, rr := range r.reads {
		if rr.ctx <= c {
			rr.acks[from] = true
		}
	}
	for len(r.reads) > 0 && r.countAcks(r.reads[0]) >= r.quorum() {
		rr := r.reads[0]
		r.reads = r.reads[1:]
		r.readStates = append(r.readStates, ReadState{Ctx: rr.ctx, Index: rr.index})
	}
}

// ReadStates returns and clears the confirmed reads.
func (r *Raft) ReadStates() []ReadState {
	rs := r.readStates
	r.readStates = nil
	return rs
}

func (r *Raft) broadcastAppend(force bool) {
	for _, m := range r.members {
		if m.ID != r.id {
			r.sendAppend(m.ID, force)
		}
	}
}

func (r *Raft) sendAppend(to NodeID, force bool) {
	pr := r.prs[to]
	if pr == nil || (pr.inflight && !force) {
		return
	}
	pr.inflight = true
	if pr.next <= r.log.snap.Index {
		s := r.log.snap
		r.send(Message{Type: MsgSnap, To: to, Snapshot: &s})
		return
	}
	prev := pr.next - 1
	prevTerm, _ := r.log.term(prev)
	hi := min(r.log.lastIndex()+1, pr.next+uint64(r.maxEntries))
	r.send(Message{
		Type: MsgApp, To: to, PrevLogIndex: prev, PrevLogTerm: prevTerm,
		Entries: r.log.slice(pr.next, hi), LeaderCommit: r.commit,
	})
}

// Propose appends a command to the leader's log. It returns the index and
// term the entry will occupy if it commits.
func (r *Raft) Propose(data []byte) (index, term uint64, err error) {
	if r.state != Leader {
		return 0, 0, ErrNotLeader
	}
	idx := r.appendEntry(Entry{Type: EntryNormal, Data: data})
	r.broadcastAppend(false)
	return idx, r.term, nil
}

// ProposeConfChange proposes adding (add=true) or removing a single server.
func (r *Raft) ProposeConfChange(add bool, m Member) (index, term uint64, err error) {
	if r.state != Leader {
		return 0, 0, ErrNotLeader
	}
	if r.configIndex > r.commit {
		return 0, 0, ErrConfChangePending
	}
	// Ongaro's fix for single-server changes: a leader must commit an entry
	// in its own term before changing the configuration.
	if t, _ := r.log.term(r.commit); t != r.term {
		return 0, 0, ErrNotReady
	}
	var next []Member
	found := false
	for _, x := range r.members {
		if x.ID == m.ID {
			found = true
			if !add {
				continue
			}
		}
		next = append(next, x)
	}
	if add == found || m.ID == None || (!add && len(next) == 0) {
		return 0, 0, ErrBadConfChange
	}
	if add {
		next = append(next, m)
	}
	idx := r.appendEntry(Entry{Type: EntryConfig, Data: EncodeMembers(next)})
	r.broadcastAppend(false)
	return idx, r.term, nil
}

// Snapshot tells Raft that the service has captured its state up to and
// including index (which must have been applied). Raft discards the log
// prefix.
func (r *Raft) Snapshot(index uint64, data []byte) {
	if index <= r.log.snap.Index || index > r.applied {
		return
	}
	t, _ := r.log.term(index)
	r.log.compact(Snapshot{Index: index, Term: t, Members: r.configAt(index), Data: data})
	r.snapDirty = true
}

// Flush persists any dirty state and then returns the messages to send and
// the entries (or snapshot) to apply, in order. Messages must only be sent
// after Flush returns, which is what guarantees that state is durable before
// other nodes can observe it.
func (r *Raft) Flush() (msgs []Message, apply []ApplyMsg) {
	if r.readBcast && r.state == Leader {
		// One heartbeat round confirms every read registered since the last.
		r.broadcastHeartbeat()
	}
	r.readBcast = false
	r.persist()
	msgs, r.msgs = r.msgs, nil
	if r.pendingSnap != nil {
		apply = append(apply, ApplyMsg{Snapshot: r.pendingSnap})
		if r.applied < r.pendingSnap.Index {
			r.applied = r.pendingSnap.Index
		}
		r.pendingSnap = nil
	}
	for r.applied < r.commit {
		r.applied++
		e := *r.log.entry(r.applied)
		apply = append(apply, ApplyMsg{Entry: &e})
	}
	return msgs, apply
}

func (r *Raft) persist() {
	hs := HardState{Term: r.term, VotedFor: r.votedFor}
	var err error
	switch {
	case r.snapDirty:
		err = r.storage.SaveSnapshot(hs, r.log.snap, r.log.entries)
	case r.logDirtyFrom != 0:
		err = r.storage.Save(hs, r.logDirtyFrom, r.log.slice(r.logDirtyFrom, r.log.lastIndex()+1))
	case r.hsDirty:
		err = r.storage.Save(hs, 0, nil)
	default:
		return
	}
	if err != nil {
		panic(fmt.Sprintf("raft: storage failure: %v", err))
	}
	r.snapDirty, r.logDirtyFrom, r.hsDirty = false, 0, false
}

// SnapIndex returns the last index covered by the snapshot.
func (r *Raft) SnapIndex() uint64 { return r.log.snap.Index }

// Commit returns the commit index.
func (r *Raft) Commit() uint64 { return r.commit }
