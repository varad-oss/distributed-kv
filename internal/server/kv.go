package server

import (
	"encoding/json"

	"github.com/varad/distributed-kv/internal/storage"
)

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func SerializeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func DeserializeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}

type KVStateMachine struct {
	engine *storage.Engine
}

func NewKVStateMachine(engine *storage.Engine) *KVStateMachine {
	return &KVStateMachine{engine: engine}
}

func (sm *KVStateMachine) Apply(command []byte) ([]byte, error) {
	cmd, err := DeserializeCommand(command)
	if err != nil {
		return nil, err
	}

	switch cmd.Op {
	case "SET":
		err = sm.engine.Put(cmd.Key, cmd.Value)
	case "DELETE":
		err = sm.engine.Delete(cmd.Key)
	}
	return nil, err
}

func (sm *KVStateMachine) Snapshot() ([]byte, error) {
	return sm.engine.Snapshot()
}

func (sm *KVStateMachine) Restore(data []byte) error {
	return sm.engine.Restore(data)
}
