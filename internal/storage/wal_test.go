package storage

import (
	"os"
	"testing"
)

func TestEngine_ResilienceAndRecovery(t *testing.T) {
	walPath := "test_cluster.wal"
	// Limpeza pós-teste
	defer os.Remove(walPath)

	// Fase 1: Inicializa o motor e insere dados persistentes
	engine, err := NewEngine(walPath)
	if err != nil {
		t.Fatalf("Erro ao iniciar Engine: %v", err)
	}

	if err := engine.Put("user:100", "Andre Cruz"); err != nil {
		t.Fatalf("Erro ao executar Put: %v", err)
	}
	engine.Close() // Simulação de encerramento do processo do servidor

	// Fase 2: Instancia um novo motor apontando para o mesmo arquivo (Simulando Recovery pós-crash)
	recoveredEngine, err := NewEngine(walPath)
	if err != nil {
		t.Fatalf("Erro ao iniciar Engine para recuperação: %v", err)
	}
	defer recoveredEngine.Close()

	// Validação de integridade: O dado gravado antes da "queda" precisa estar na RAM
	val, exists := recoveredEngine.Get("user:100")
	if !exists {
		t.Fatal("Falha crítica: A chave não foi recuperada a partir do Write-Ahead Log")
	}

	if val != "Andre Cruz" {
		t.Errorf("Esperava 'Andre Cruz', mas obteve: '%s'", val)
	} else {
		t.Log("Sucesso! O estado foi reidratado via WAL de forma idêntica.")
	}
}
