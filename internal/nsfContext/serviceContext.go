package nsfContext

import (
	"encoding/json"
	"fmt"
	"nsf/internal/db"
	"nsf/logger"
	"slices"
	"strings"
	"sync"
)

const serviceRecordPrefix = "nsf/nf/"

type serviceContextIE struct {
	db.DbIf
	*logger.NsfLogger
}

type serviceContext struct {
	db.DbIf
	*logger.NsfLogger
	mu sync.RWMutex
}

// Each NF is one persistent record, so registering several services is atomic.
// The DB is the source of truth; discovery works immediately after a restart.
type serviceRecord struct {
	NfName     string   `json:"nf_name"`
	SbiAddress string   `json:"sbi_address"`
	SbiPort    int      `json:"sbi_port"`
	Services   []string `json:"services"`
}

func newServiceContext(ie *serviceContextIE) *serviceContext {
	return &serviceContext{DbIf: ie.DbIf, NsfLogger: ie.NsfLogger}
}

func (s *serviceContext) release() {
	s.NsfLogger.SvcLog.Infoln("Release serviceContext...")
	// dbContext owns the connection and closes it after releasing this context.
	// Registration data must survive shutdown and unexpected process termination.
	s.NsfLogger.SvcLog.Infoln("serviceContext released")
}

func (s *serviceContext) registerServices(nfName, sbiAddr string, sbiPort int, services []string) error {
	s.NsfLogger.SvcLog.Tracef("Registering %s services", nfName)
	s.mu.Lock()
	defer s.mu.Unlock()

	key := serviceRecordPrefix + nfName
	record, err := s.loadRecord(key)
	if err != nil {
		return err
	}
	record.NfName, record.SbiAddress, record.SbiPort = nfName, sbiAddr, sbiPort
	for _, service := range services {
		if !slices.Contains(record.Services, service) {
			record.Services = append(record.Services, service)
		}
	}
	return s.saveRecord(key, record)
}

func (s *serviceContext) deregisterServices(nfName string, services []string) error {
	s.NsfLogger.SvcLog.Tracef("De-registering %s services", nfName)
	s.mu.Lock()
	defer s.mu.Unlock()

	key := serviceRecordPrefix + nfName
	record, err := s.loadRecord(key)
	if err != nil {
		return err
	}
	if record.NfName == "" {
		return nil
	}
	remaining := make([]string, 0, len(record.Services))
	for _, service := range record.Services {
		if !slices.Contains(services, service) {
			remaining = append(remaining, service)
		}
	}
	if len(remaining) == 0 {
		if err := s.Remove(key); err != nil {
			return fmt.Errorf("remove NF %s: %w", nfName, err)
		}
		return nil
	}
	record.Services = remaining
	return s.saveRecord(key, record)
}

func (s *serviceContext) discoverService(service string) (string, string, int, error) {
	s.NsfLogger.SvcLog.Tracef("Discovering service %s", service)
	s.mu.RLock()
	defer s.mu.RUnlock()

	records, err := s.LoadAll()
	if err != nil {
		return "", "", 0, fmt.Errorf("load services: %w", err)
	}
	keys := make([]string, 0, len(records))
	for key := range records {
		if strings.HasPrefix(key, serviceRecordPrefix) {
			keys = append(keys, key)
		}
	}
	// Stable selection when multiple NFs provide the same service.
	slices.Sort(keys)
	for _, key := range keys {
		var record serviceRecord
		if err := json.Unmarshal([]byte(records[key]), &record); err != nil {
			return "", "", 0, fmt.Errorf("decode NF %s: %w", key, err)
		}
		if slices.Contains(record.Services, service) {
			return record.NfName, record.SbiAddress, record.SbiPort, nil
		}
	}
	return "", "", 0, fmt.Errorf("service %q not found", service)
}

func (s *serviceContext) loadRecord(key string) (serviceRecord, error) {
	var record serviceRecord
	exists, err := s.Exist(key)
	if err != nil {
		return record, fmt.Errorf("check NF %s: %w", key, err)
	}
	if !exists {
		return record, nil
	}
	value, err := s.Load(key)
	if err != nil {
		return record, fmt.Errorf("load NF %s: %w", key, err)
	}
	if err := json.Unmarshal([]byte(value), &record); err != nil {
		return record, fmt.Errorf("decode NF %s: %w", key, err)
	}
	return record, nil
}

func (s *serviceContext) saveRecord(key string, record serviceRecord) error {
	value, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode NF %s: %w", key, err)
	}
	if err := s.Save(key, string(value)); err != nil {
		return fmt.Errorf("save NF %s: %w", key, err)
	}
	return nil
}
