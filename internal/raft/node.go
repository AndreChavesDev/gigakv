package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/AndreChavesDev/gigakv/api"
	"github.com/AndreChavesDev/gigakv/internal/storage"
)

type NodeState int

const (
	Follower NodeState = iota
	PreCandidate
	Candidate
	Leader
)

func (s NodeState) String() string {
	return [...]string{"Follower", "PreCandidate", "Candidate", "Leader"}[s]
}

type PeerTransport interface {
	SendRequestVote(ctx context.Context, req *api.VoteRequest) (*api.VoteResponse, error)
	SendRequestPreVote(ctx context.Context, req *api.PreVoteRequest) (*api.PreVoteResponse, error)
	SendAppendEntries(ctx context.Context, req *api.AppendRequest) (*api.AppendResponse, error)
	SendInstallSnapshot(ctx context.Context, req *api.InstallSnapshotRequest) (*api.InstallSnapshotResponse, error)
}

type RaftNode struct {
	mu                sync.Mutex
	id                int
	state             NodeState
	currentTerm       int
	votedFor          int
	log               []*api.LogEntry
	commitIndex       int
	lastApplied       int
	storage           *storage.Engine
	metaStorage       StateStorage
	peers             map[int]PeerTransport
	heartbeatTimeout  time.Duration
	stopChan          chan struct{}
	resetElectionChan chan struct{}

	nextIndex   map[int]int
	matchIndex  map[int]int
	commitChans map[int]chan bool
}

func NewRaftNode(id int, metaStorage StateStorage) *RaftNode {
	rn := &RaftNode{
		id:                id,
		state:             Follower,
		currentTerm:       0,
		votedFor:          -1,
		log:               make([]*api.LogEntry, 0),
		commitIndex:       -1,
		lastApplied:       -1,
		storage:           nil,
		metaStorage:       metaStorage,
		heartbeatTimeout:  100 * time.Millisecond,
		stopChan:          make(chan struct{}),
		resetElectionChan: make(chan struct{}, 1),
		peers:             make(map[int]PeerTransport),
		nextIndex:         make(map[int]int),
		matchIndex:        make(map[int]int),
		commitChans:       make(map[int]chan bool),
	}

	term, votedFor, found, err := rn.metaStorage.LoadState()
	if err != nil {
		fmt.Printf("[Raft-Node %d] Erro ao carregar metadados do disco: %v. Iniciando do zero.\n", id, err)
	} else if found {
		rn.currentTerm = term
		rn.votedFor = votedFor
		fmt.Printf("[Raft-Node %d] Estado estável recuperado! Termo anterior: %d, Votou em: %d\n", id, term, votedFor)
	}

	return rn
}

func (rn *RaftNode) SetPeers(peers map[int]PeerTransport) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.peers = peers
}

func (rn *RaftNode) SetStorage(store *storage.Engine) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.storage = store
}

func (rn *RaftNode) getLastLogInfo() (int64, int32) {
	if len(rn.log) == 0 {
		return -1, 0
	}
	lastEntry := rn.log[len(rn.log)-1]
	return int64(len(rn.log) - 1), lastEntry.Term
}

func (rn *RaftNode) cancelPendingProposalsLocked() {
	for idx, ch := range rn.commitChans {
		select {
		case ch <- false:
		default:
		}
		delete(rn.commitChans, idx)
	}
}

func (rn *RaftNode) Propose(key, value string) bool {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return false
	}

	entry := &api.LogEntry{
		Key:   key,
		Value: value,
		Term:  int32(rn.currentTerm),
	}
	rn.log = append(rn.log, entry)
	targetIndex := len(rn.log) - 1

	ch := make(chan bool, 1)
	rn.commitChans[targetIndex] = ch
	rn.mu.Unlock()

	fmt.Printf("[Raft-Node %d] [LÍDER] Nova proposta recebida: %s=%s (Índice Alvo: %d)\n", rn.id, key, value, targetIndex)

	select {
	case ok := <-ch:
		return ok
	case <-time.After(4 * time.Second):
		rn.mu.Lock()
		delete(rn.commitChans, targetIndex)
		rn.mu.Unlock()
		return false
	}
}

func (rn *RaftNode) GetLocal(key string) (string, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.storage == nil {
		return "", false
	}
	return rn.storage.Get(key)
}

func (rn *RaftNode) Start() {
	go rn.runEventLoop()
}

func (rn *RaftNode) runEventLoop() {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rn.id)))

	for {
		rn.mu.Lock()
		currentState := rn.state
		rn.mu.Unlock()

		if currentState == Leader {
			select {
			case <-rn.stopChan:
				return
			case <-time.After(rn.heartbeatTimeout):
				rn.sendHeartbeats()
			}
		} else {
			// Window ajustada para 600-1000ms para acomodar latência de disco no Windows/OneDrive
			randomTimeout := time.Duration(600+r.Intn(400)) * time.Millisecond
			electionTimer := time.NewTimer(randomTimeout)

			select {
			case <-rn.stopChan:
				electionTimer.Stop()
				return
			case <-rn.resetElectionChan:
				if !electionTimer.Stop() {
					select {
					case <-electionTimer.C:
					default:
					}
				}
			case <-electionTimer.C:
				rn.startPreVote()
			}
		}
	}
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	if rn.state == Leader {
		rn.mu.Unlock()
		return
	}
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id

	_ = rn.metaStorage.SaveState(rn.currentTerm, rn.votedFor)

	currentTerm := rn.currentTerm
	lastLogIndex, lastLogTerm := rn.getLastLogInfo()
	totalNodes := len(rn.peers) + 1
	quorumNeeded := (totalNodes / 2) + 1
	votesReceived := 1
	var voteMu sync.Mutex
	electionFinished := false
	rn.mu.Unlock()

	for id, peer := range rn.peers {
		go func(peerID int, transport PeerTransport) {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			req := &api.VoteRequest{
				Term:         int32(currentTerm),
				CandidateId:  int32(rn.id),
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			resp, err := transport.SendRequestVote(ctx, req)
			if err == nil {
				rn.mu.Lock()
				if int(resp.Term) > rn.currentTerm {
					rn.currentTerm = int(resp.Term)
					rn.state = Follower
					rn.votedFor = -1
					rn.cancelPendingProposalsLocked()
					_ = rn.metaStorage.SaveState(rn.currentTerm, rn.votedFor)
					rn.mu.Unlock()
					return
				}
				rn.mu.Unlock()

				if resp.VoteGranted {
					voteMu.Lock()
					votesReceived++
					if votesReceived >= quorumNeeded && !electionFinished {
						rn.mu.Lock()
						if rn.state == Candidate && rn.currentTerm == currentTerm {
							electionFinished = true
							rn.mu.Unlock()
							voteMu.Unlock()
							rn.promoteToLeader(currentTerm)
							return
						}
						rn.mu.Unlock()
					}
					voteMu.Unlock()
				}
			}
		}(id, peer)
	}
}

func (rn *RaftNode) startPreVote() {
	rn.mu.Lock()
	if rn.state == Leader {
		rn.mu.Unlock()
		return
	}
	rn.state = PreCandidate
	currentTerm := rn.currentTerm
	lastLogIndex, lastLogTerm := rn.getLastLogInfo()
	peers := rn.peers
	rn.mu.Unlock()

	votesReceived := 1
	var voteMu sync.Mutex
	quorumNeeded := (len(peers) + 1) / 2 + 1
	electionTriggered := false

	for id, peer := range peers {
		go func(peerID int, transport PeerTransport) {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			req := &api.PreVoteRequest{
				Term:         int32(currentTerm),
				CandidateId:  int32(rn.id),
				LastLogIndex: lastLogIndex,
				LastLogTerm:  int32(lastLogTerm),
			}

			resp, err := transport.SendRequestPreVote(ctx, req)
			if err == nil && resp.VoteGranted {
				voteMu.Lock()
				votesReceived++
				if votesReceived >= quorumNeeded && !electionTriggered {
					rn.mu.Lock()
					if rn.state == PreCandidate && rn.currentTerm == currentTerm {
						electionTriggered = true
						rn.mu.Unlock()
						voteMu.Unlock()
						rn.startElection()
						return
					}
					rn.mu.Unlock()
				}
				voteMu.Unlock()
			}
		}(id, peer)
	}
}

func (rn *RaftNode) HandleRequestPreVote(req *api.PreVoteRequest) (bool, int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term < int32(rn.currentTerm) {
		return false, rn.currentTerm
	}

	// Não concede PreVote se o nó atual for o Líder ativo do mesmo termo
	if rn.state == Leader && req.Term == int32(rn.currentTerm) {
		return false, rn.currentTerm
	}

	myLastLogIndex, myLastLogTerm := rn.getLastLogInfo()
	logUpToDate := false

	if req.LastLogTerm > myLastLogTerm {
		logUpToDate = true
	} else if req.LastLogTerm == myLastLogTerm && req.LastLogIndex >= myLastLogIndex {
		logUpToDate = true
	}

	if logUpToDate {
		return true, rn.currentTerm
	}

	return false, rn.currentTerm
}

func (rn *RaftNode) promoteToLeader(electionTerm int) {
	rn.mu.Lock()
	if rn.state != Candidate || rn.currentTerm != electionTerm {
		rn.mu.Unlock()
		return
	}
	rn.state = Leader
	fmt.Printf("[Raft-Node %d] LÍDER no Termo %d!\n", rn.id, rn.currentTerm)

	lastLogIndex := len(rn.log) - 1
	for peerID := range rn.peers {
		rn.nextIndex[peerID] = lastLogIndex + 1
		rn.matchIndex[peerID] = -1
	}
	rn.mu.Unlock()

	select {
	case rn.resetElectionChan <- struct{}{}:
	default:
	}

	rn.sendHeartbeats()
}

func (rn *RaftNode) sendHeartbeats() {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return
	}
	term := rn.currentTerm
	leaderCommit := rn.commitIndex
	rn.mu.Unlock()

	for id, peer := range rn.peers {
		go func(peerID int, transport PeerTransport) {
			rn.mu.Lock()
			if rn.state != Leader || rn.currentTerm != term {
				rn.mu.Unlock()
				return
			}

			nextIdx := rn.nextIndex[peerID]

			if nextIdx <= 0 && len(rn.log) == 0 && rn.commitIndex >= 0 {
				data, err := rn.storage.GetSnapshotData()
				if err != nil {
					rn.mu.Unlock()
					return
				}

				req := &api.InstallSnapshotRequest{
					Term:              int32(term),
					LeaderId:          int32(rn.id),
					LastIncludedIndex: int64(rn.commitIndex),
					Data:              data,
				}
				rn.mu.Unlock()

				_, _ = transport.SendInstallSnapshot(context.Background(), req)
				return
			}

			prevLogIndex := nextIdx - 1
			var prevLogTerm int32 = 0
			if prevLogIndex >= 0 && prevLogIndex < len(rn.log) {
				prevLogTerm = rn.log[prevLogIndex].Term
			}

			var entriesToSend []*api.LogEntry
			if nextIdx >= 0 && nextIdx < len(rn.log) {
				entriesToSend = rn.log[nextIdx:]
			}

			req := &api.AppendRequest{
				Term:         int32(term),
				LeaderId:     int32(rn.id),
				PrevLogIndex: int64(prevLogIndex),
				PrevLogTerm:  prevLogTerm,
				Entries:      entriesToSend,
				LeaderCommit: int64(leaderCommit),
			}
			rn.mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			resp, err := transport.SendAppendEntries(ctx, req)
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if int(resp.Term) > rn.currentTerm {
				rn.currentTerm = int(resp.Term)
				rn.state = Follower
				rn.votedFor = -1
				rn.cancelPendingProposalsLocked()
				_ = rn.metaStorage.SaveState(rn.currentTerm, rn.votedFor)
				return
			}

			if rn.state != Leader || rn.currentTerm != term {
				return
			}

			if resp.Success {
				rn.nextIndex[peerID] = prevLogIndex + 1 + len(entriesToSend)
				rn.matchIndex[peerID] = rn.nextIndex[peerID] - 1

				for N := len(rn.log) - 1; N > rn.commitIndex; N-- {
					if rn.log[N].Term == int32(rn.currentTerm) {
						count := 1
						for pID := range rn.peers {
							if rn.matchIndex[pID] >= N {
								count++
							}
						}
						if count >= (len(rn.peers)+1)/2+1 {
							rn.commitLogs(N + 1)
							break
						}
					}
				}
			} else {
				if rn.nextIndex[peerID] > 0 {
					rn.nextIndex[peerID]--
				}
			}
		}(id, peer)
	}
}

func (rn *RaftNode) commitLogs(upToSize int) {
	targetCommit := upToSize - 1
	if targetCommit > rn.commitIndex && targetCommit < len(rn.log) {
		rn.commitIndex = targetCommit
		fmt.Printf("[Raft-Node %d] [LÍDER] Logs consolidados até o índice %d\n", rn.id, rn.commitIndex)

		var entriesToApply []*api.LogEntry
		var chansToNotify []chan bool

		for rn.lastApplied < rn.commitIndex {
			rn.lastApplied++
			entry := rn.log[rn.lastApplied]
			entriesToApply = append(entriesToApply, entry)

			if ch, ok := rn.commitChans[rn.lastApplied]; ok {
				chansToNotify = append(chansToNotify, ch)
				delete(rn.commitChans, rn.lastApplied)
			}
		}

		storage := rn.storage
		if storage != nil && len(entriesToApply) > 0 {
			// Desescala o Lock temporariamente para a gravação no disco não travar requisições de rede
			rn.mu.Unlock()
			for _, entry := range entriesToApply {
				if err := storage.Put(entry.Key, entry.Value); err != nil {
					fmt.Printf("[Raft-Node %d] [ERRO] Falha ao persistir no WAL: %v\n", rn.id, err)
				} else {
					fmt.Printf("[Raft-Node %d] [MÁQUINA DE ESTADOS] Log consolidado e persistido no WAL: %s = %s\n", rn.id, entry.Key, entry.Value)
				}
			}
			rn.mu.Lock()
		}

		// Notifica o cliente SOMENTE APÓS a gravação na máquina de estados estar concluída
		for _, ch := range chansToNotify {
			ch <- true
		}

		rn.maybeSnapshotLocked()
	}
}

func (rn *RaftNode) HandleRequestVote(req *api.VoteRequest) (bool, int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	candidateTerm := int(req.Term)
	candidateID := int(req.CandidateId)

	if candidateTerm < rn.currentTerm {
		return false, rn.currentTerm
	}

	stateChanged := false
	if candidateTerm > rn.currentTerm {
		rn.currentTerm = candidateTerm
		rn.state = Follower
		rn.votedFor = -1
		rn.cancelPendingProposalsLocked()
		stateChanged = true
	}

	myLastLogIndex, myLastLogTerm := rn.getLastLogInfo()
	logUpToDate := false

	if req.LastLogTerm > myLastLogTerm {
		logUpToDate = true
	} else if req.LastLogTerm == myLastLogTerm && req.LastLogIndex >= myLastLogIndex {
		logUpToDate = true
	}

	if (rn.votedFor == -1 || rn.votedFor == candidateID) && logUpToDate {
		rn.votedFor = candidateID
		select {
		case rn.resetElectionChan <- struct{}{}:
		default:
		}
		_ = rn.metaStorage.SaveState(rn.currentTerm, rn.votedFor)
		return true, rn.currentTerm
	}

	if stateChanged {
		_ = rn.metaStorage.SaveState(rn.currentTerm, rn.votedFor)
	}

	return false, rn.currentTerm
}

func (rn *RaftNode) HandleAppendEntries(req *api.AppendRequest) (bool, int) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term < int32(rn.currentTerm) {
		return false, rn.currentTerm
	}

	stateChanged := false
	if int(req.Term) > rn.currentTerm {
		rn.currentTerm = int(req.Term)
		rn.votedFor = -1
		stateChanged = true
	}

	rn.state = Follower
	rn.cancelPendingProposalsLocked()

	select {
	case rn.resetElectionChan <- struct{}{}:
	default:
	}

	if stateChanged {
		_ = rn.metaStorage.SaveState(rn.currentTerm, rn.votedFor)
	}

	prevLogIndex := int(req.PrevLogIndex)

	if prevLogIndex >= 0 && prevLogIndex >= len(rn.log) {
		return false, rn.currentTerm
	}

	if prevLogIndex >= 0 && rn.log[prevLogIndex].Term != req.PrevLogTerm {
		rn.log = rn.log[:prevLogIndex]
		return false, rn.currentTerm
	}

	if len(req.Entries) > 0 {
		insertIdx := prevLogIndex + 1
		if insertIdx < len(rn.log) {
			rn.log = rn.log[:insertIdx]
		}
		rn.log = append(rn.log, req.Entries...)
	}

	leaderCommit := int(req.LeaderCommit)
	if leaderCommit > rn.commitIndex {
		lastNewEntryIdx := len(rn.log) - 1
		if leaderCommit < lastNewEntryIdx {
			rn.commitIndex = leaderCommit
		} else {
			rn.commitIndex = lastNewEntryIdx
		}

		for rn.lastApplied < rn.commitIndex {
			rn.lastApplied++
			entry := rn.log[rn.lastApplied]
			if rn.storage != nil {
				if err := rn.storage.Put(entry.Key, entry.Value); err != nil {
					fmt.Printf("[Raft-Node %d] [ERRO] Falha ao persistir no WAL: %v\n", rn.id, err)
				}
			}
		}
	}

	return true, rn.currentTerm
}

func (rn *RaftNode) Stop() { close(rn.stopChan) }

func (rn *RaftNode) GetState() NodeState {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state
}

const MaxLogEntriesBeforeSnapshot = 500

func (rn *RaftNode) maybeSnapshotLocked() {
	if len(rn.log) < MaxLogEntriesBeforeSnapshot {
		return
	}

	snapshotData, err := rn.storage.GetSnapshotData()
	if err != nil {
		fmt.Printf("[Raft-Node %d] Falha ao capturar snapshot: %v\n", rn.id, err)
		return
	}

	lastIndex := len(rn.log) - 1
	rn.log = nil

	fmt.Printf("[Raft-Node %d] Snapshot realizado com %d bytes. Log compactado (Tamanho anterior: %d)\n",
		rn.id, len(snapshotData), lastIndex+1)
}

func (rn *RaftNode) InstallSnapshot(ctx context.Context, req *api.InstallSnapshotRequest) (*api.InstallSnapshotResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if req.Term < int32(rn.currentTerm) {
		return &api.InstallSnapshotResponse{Term: int32(rn.currentTerm)}, nil
	}

	if err := rn.storage.RestoreFromSnapshot(req.Data); err != nil {
		return nil, fmt.Errorf("falha ao restaurar snapshot no engine: %v", err)
	}

	rn.commitIndex = int(req.LastIncludedIndex)
	rn.lastApplied = int(req.LastIncludedIndex)
	rn.log = nil

	fmt.Printf("[Raft-Node %d] Snapshot instalado! Líder: %d, Índice: %d\n", rn.id, req.LeaderId, req.LastIncludedIndex)

	return &api.InstallSnapshotResponse{Term: int32(rn.currentTerm)}, nil
}