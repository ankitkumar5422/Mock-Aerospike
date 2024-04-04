package main

import (
	"fmt"
	"sync"
)

type AerospikeClient interface {
	Put(namespace, set string, key interface{}, columns map[string]interface{}) error
	Get(namespace, set, key string) (map[string]interface{}, error)
}

type MockAerospike struct {
	data map[string]map[string]map[string]interface{}
	mu   sync.Mutex
}

func NewMockAerospike() *MockAerospike {
	return &MockAerospike{
		data: make(map[string]map[string]map[string]interface{}),
	}
}

func (m *MockAerospike) PutMock(namespace, set string, key interface{}, columns map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Assert the key to a string type
	keyStr, ok := key.(string)
	if !ok {
		return fmt.Errorf("key is not a string")
	}

	// Add data to the map
	if _, ok := m.data[namespace]; !ok {
		m.data[namespace] = make(map[string]map[string]interface{})
	}
	if _, ok := m.data[namespace][set]; !ok {
		m.data[namespace][set] = make(map[string]interface{})
	}
	m.data[namespace][set][keyStr] = columns

	return nil
}

// GetMock simulates getting data from the mock Aerospike database.
func (m *MockAerospike) GetMock(namespace, set, key string) (map[string]interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if the namespace exists
	ns, ok := m.data[namespace]
	if !ok {
		return nil, fmt.Errorf("namespace %s not found", namespace)
	}

	// Check if the set exists
	s, ok := ns[set]
	if !ok {
		return nil, fmt.Errorf("set %s not found in namespace %s", set, namespace)
	}

	// Check if the key exists
	data, ok := s[key]
	if !ok {
		return nil, fmt.Errorf("key %s not found in set %s of namespace %s", key, set, namespace)
	}

	// Assert the data to map[string]interface{}
	result, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected data type for key %s in set %s of namespace %s", key, set, namespace)
	}

	return result, nil
}

func main() {
	// Create a mock Aerospike client
	aerospikeClient := NewMockAerospike()

	// Put some data into Aerospike using PutMock
	err := aerospikeClient.PutMock("myNamespace", "mySet", "myKey", map[string]interface{}{
		"bin1": "value1",
		// "bin2": 123,
	})
	if err != nil {
		fmt.Println("Error putting data into Aerospike:", err)
	} else {
		fmt.Println("Data successfully put into Aerospike")
	}

	// Get the data from Aerospike using GetMock
	data, err := aerospikeClient.GetMock("myNamespace", "mySet", "myKey")
	if err != nil {
		fmt.Println("Error getting data from Aerospike:", err)
	} else {
		fmt.Println("Data retrieved from Aerospike:", data)
	}
}
