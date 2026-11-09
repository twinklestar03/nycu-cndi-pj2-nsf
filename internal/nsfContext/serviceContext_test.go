package nsfContext_test

import (
	"fmt"
	"nsf/internal/db"
	"nsf/internal/nsfContext"
	"nsf/logger"
	"path/filepath"
	"sync"
	"testing"

	loggergoUtil "github.com/Alonza0314/logger-go/v2/util"
)

/*
This test file is designed to validate the functionality of the service context (ServiceContext) in the nsfContext package.
It includes tests for service registration, discovery, and deregistration.
Each test case checks for the correct behavior of the ServiceContext methods and ensures that services are correctly registered, discovered, and deregistered within the context.
*/

var testServiceContextCases = []struct {
	name       string
	nfName     string
	sbiAddress string
	sbiPort    int
	services   []string
}{
	{
		name:       "Service 1",
		nfName:     "NF1",
		sbiAddress: "127.0.0.1",
		sbiPort:    8080,
		services:   []string{"service1", "service2"},
	},
	{
		name:       "Service 2",
		nfName:     "NF2",
		sbiAddress: "127.0.0.2",
		sbiPort:    8081,
		services:   []string{"service3", "service4"},
	},
}

func TestServiceContext(t *testing.T) {
	t.Run("Service Registration", testServiceRegistration)
	t.Run("Service Discovery", testServiceDiscovery)
	t.Run("Service Deregistration", testServiceDeregistration)
}

func testServiceRegistration(t *testing.T) {
	for _, tc := range testServiceContextCases {
		if err := nsfCtx.RegisterServices(tc.nfName, tc.sbiAddress, tc.sbiPort, tc.services); err != nil {
			t.Errorf("Failed to register services for %s: %v", tc.name, err)
		}
	}
}

func testServiceDiscovery(t *testing.T) {
	for _, tc := range testServiceContextCases {
		for _, service := range tc.services {
			nfName, sbiAddr, sbiPort, err := nsfCtx.DiscoverService(service)
			if err != nil {
				t.Errorf("Failed to discover service %s: %v", service, err)
			} else if nfName != tc.nfName || sbiAddr != tc.sbiAddress || sbiPort != tc.sbiPort {
				t.Errorf("Discovered service %s has incorrect details: got (%s, %s, %d), want (%s, %s, %d)", service, nfName, sbiAddr, sbiPort, tc.nfName, tc.sbiAddress, tc.sbiPort)
			}
		}
	}
}

func testServiceDeregistration(t *testing.T) {
	for _, tc := range testServiceContextCases {
		if err := nsfCtx.DeregisterServices(tc.nfName, tc.services); err != nil {
			t.Errorf("Failed to deregister services for %s: %v", tc.name, err)
		}
	}
}

func openServiceContext(t *testing.T, path string) *nsfContext.NsfContext {
	t.Helper()
	originalPath := db.NSF_DB_PATH
	db.NSF_DB_PATH = path
	defer func() { db.NSF_DB_PATH = originalPath }()
	log := logger.NewNsfLogger(loggergoUtil.LogLevelString("error"))
	ctx := nsfContext.NewNsfContext(&nsfContext.NsfContextIE{DbType: "bbolt", NsfLogger: &log})
	if ctx == nil {
		t.Fatal("failed to open isolated service context")
	}
	return ctx
}

func requireService(t *testing.T, ctx *nsfContext.NsfContext, service, wantNF, wantAddress string, wantPort int) {
	t.Helper()
	nf, address, port, err := ctx.DiscoverService(service)
	if err != nil || nf != wantNF || address != wantAddress || port != wantPort {
		t.Fatalf("discover %q = (%q, %q, %d, %v), want (%q, %q, %d, nil)", service, nf, address, port, err, wantNF, wantAddress, wantPort)
	}
}

func TestServiceRecoveryAndPartialDeregistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	ctx := openServiceContext(t, path)
	if err := ctx.RegisterServices("NF-A", "127.0.0.10", 8010, []string{"shared", "a-only", "a-only"}); err != nil {
		ctx.Release()
		t.Fatal(err)
	}
	if err := ctx.RegisterServices("NF-B", "127.0.0.11", 8011, []string{"shared", "b-only"}); err != nil {
		ctx.Release()
		t.Fatal(err)
	}
	if err := ctx.RegisterServices("NF-A", "127.0.0.10", 8010, []string{"a-new"}); err != nil {
		ctx.Release()
		t.Fatal(err)
	}
	requireService(t, ctx, "a-only", "NF-A", "127.0.0.10", 8010)
	requireService(t, ctx, "a-new", "NF-A", "127.0.0.10", 8010)
	if err := ctx.DeregisterServices("NF-A", []string{"shared"}); err != nil {
		ctx.Release()
		t.Fatal(err)
	}
	ctx.Release()

	ctx = openServiceContext(t, path)
	requireService(t, ctx, "a-only", "NF-A", "127.0.0.10", 8010)
	requireService(t, ctx, "shared", "NF-B", "127.0.0.11", 8011)
	if err := ctx.DeregisterServices("NF-A", []string{"a-only", "a-new"}); err != nil {
		ctx.Release()
		t.Fatal(err)
	}
	ctx.Release()

	ctx = openServiceContext(t, path)
	defer ctx.Release()
	if nf, address, port, err := ctx.DiscoverService("a-only"); err == nil || nf != "" || address != "" || port != 0 {
		t.Fatalf("deleted service returned (%q, %q, %d, %v)", nf, address, port, err)
	}
	requireService(t, ctx, "b-only", "NF-B", "127.0.0.11", 8011)
	if _, _, _, err := ctx.DiscoverService("never-registered"); err == nil {
		t.Fatal("missing service should return an error")
	}
}

func TestConcurrentServiceOperations(t *testing.T) {
	ctx := openServiceContext(t, filepath.Join(t.TempDir(), "concurrent.db"))
	defer ctx.Release()
	const count = 32
	var workers sync.WaitGroup
	for i := range count {
		workers.Go(func() {
			service := fmt.Sprintf("service-%d", i)
			if err := ctx.RegisterServices("NF", "127.0.0.1", 8080, []string{service}); err != nil {
				t.Errorf("register %s: %v", service, err)
			}
		})
	}
	workers.Wait()
	for i := range count {
		requireService(t, ctx, fmt.Sprintf("service-%d", i), "NF", "127.0.0.1", 8080)
	}
	for i := range count {
		workers.Go(func() {
			service := fmt.Sprintf("service-%d", i)
			if _, _, _, err := ctx.DiscoverService(service); err != nil {
				t.Errorf("discover %s: %v", service, err)
			}
			if err := ctx.DeregisterServices("NF", []string{service}); err != nil {
				t.Errorf("deregister %s: %v", service, err)
			}
		})
	}
	workers.Wait()
	for i := range count {
		if _, _, _, err := ctx.DiscoverService(fmt.Sprintf("service-%d", i)); err == nil {
			t.Errorf("deleted service-%d is still discoverable", i)
		}
	}
}
