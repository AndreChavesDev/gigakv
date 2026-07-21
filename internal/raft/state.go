package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// StateStorage define a interface para persistir os metadados do Raft
type StateStorage interface {
	SaveState(term int, votedFor int) error
	LoadState() (term int, votedFor int, found bool, err error)
}

// FileStateStorage implementa a gravação de estado em um arquivo local
type FileStateStorage struct {
	mu       sync.Mutex
	filePath string
}

func NewFileStateStorage(nodeID int) *FileStateStorage {
	return &FileStateStorage{
		filePath: fmt.Sprintf("raft_state_%d.metadata", nodeID),
	}
}

type stateData struct {
	Term     int `json:"term"`
	VotedFor int `json:"votedFor"`
}

func (f *FileStateStorage) SaveState(term int, votedFor int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data := stateData{Term: term, VotedFor: votedFor}
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	
	// Salva o arquivo no disco (equivalente a um Sync)
	return os.WriteFile(f.filePath, bytes, 0644)
}

func (f *FileStateStorage) LoadState() (int, int, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	bytes, err := os.ReadFile(f.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, -1, false, nil // Arquivo não existe, primeira inicialização
		}
		return 0, -1, false, err
	}

	var data stateData
	if err := json.Unmarshal(bytes, &data); err != nil {
		return 0, -1, false, err
	}

	return data.Term, data.VotedFor, true, nil
}