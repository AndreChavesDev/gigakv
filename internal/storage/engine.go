package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type LogEntry struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

type Engine struct {
	mu      sync.RWMutex
	kv      map[string]string
	walFile *os.File
	walPath string
	encoder *json.Encoder
}

// NewEngine inicializa o estado, tentando carregar primeiro um snapshot e depois o WAL.
func NewEngine(walPath string) (*Engine, error) {
	engine := &Engine{
		kv:      make(map[string]string),
		walPath: walPath,
	}

	// 1. Carrega Snapshot se existir (Estado base)
	snapshotPath := walPath + ".snapshot"
	if _, err := os.Stat(snapshotPath); err == nil {
		data, err := os.ReadFile(snapshotPath)
		if err == nil {
			engine.RestoreFromSnapshot(data)
		}
	}

	// 2. Recupera operações do WAL ocorridas após o snapshot
	if _, err := os.Stat(walPath); err == nil {
		if err := engine.recoverFromWAL(); err != nil {
			return nil, fmt.Errorf("falha ao recuperar WAL: %v", err)
		}
	}

	// 3. Abre WAL para novas operações
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	engine.walFile = file
	engine.encoder = json.NewEncoder(file)

	return engine, nil
}

// SaveSnapshotToFile persiste o estado atual em disco e trunca o WAL.
func (e *Engine) SaveSnapshotToFile() error {
    snapshotPath := e.walPath + ".snapshot"
    
    data, err := e.GetSnapshotData()
    if err != nil {
        return err
    }

    // Salva o snapshot em um arquivo temporário e depois renomeia para o final (atômico)
    // Isso evita que, se o servidor cair durante o WriteFile, você fique com um arquivo corrompido.
    tmpPath := snapshotPath + ".tmp"
    if err := os.WriteFile(tmpPath, data, 0644); err != nil {
        return err
    }
    if err := os.Rename(tmpPath, snapshotPath); err != nil {
        return err
    }

    // Limpa o WAL
    if e.walFile != nil {
        if err := e.walFile.Truncate(0); err != nil {
            return fmt.Errorf("erro ao truncar WAL: %v", err)
        }
        if _, err := e.walFile.Seek(0, 0); err != nil {
            return fmt.Errorf("erro ao resetar ponteiro do WAL: %v", err)
        }
        // Garante que o truncamento seja persistido no disco
        if err := e.walFile.Sync(); err != nil {
            return fmt.Errorf("erro ao sincronizar WAL após truncate: %v", err)
        }
    }
    return nil
}

func (e *Engine) Put(key, value string) error {
	entry := LogEntry{Operation: "PUT", Key: key, Value: value}
	if err := e.writeToWAL(entry); err != nil {
		return err
	}
	e.mu.Lock()
	e.kv[key] = value
	e.mu.Unlock()
	return nil
}

func (e *Engine) Get(key string) (string, bool) {
	e.mu.RLock()
	val, exists := e.kv[key]
	e.mu.RUnlock()
	return val, exists
}

func (e *Engine) GetSnapshotData() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return json.Marshal(e.kv)
}

func (e *Engine) RestoreFromSnapshot(data []byte) error {
	var backup map[string]string
	if err := json.Unmarshal(data, &backup); err != nil {
		return err
	}
	e.mu.Lock()
	e.kv = backup
	e.mu.Unlock()
	return nil
}

func (e *Engine) writeToWAL(entry LogEntry) error {
	if err := e.encoder.Encode(entry); err != nil {
		return err
	}
	return e.walFile.Sync()
}

func (e *Engine) recoverFromWAL() error {
    file, err := os.Open(e.walPath)
    if err != nil {
        return err
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        var entry LogEntry
        if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
            // Opcional: registrar que uma linha corrompida foi ignorada
            continue
        }

        if entry.Operation == "PUT" {
            e.kv[entry.Key] = entry.Value
        }
    }

    // AQUI ESTÁ A CORREÇÃO:
    // Verifica se o loop parou por um erro real de leitura (ex: erro de hardware)
    if err := scanner.Err(); err != nil {
        return fmt.Errorf("erro fatal ao ler o WAL: %v", err)
    }

    return nil
}

func (e *Engine) Close() error {
	if e.walFile != nil {
		return e.walFile.Close()
	}
	return nil
}
