/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package yandex

import (
	"context"
	"fmt"

	"github.com/yandex-cloud/go-genproto/yandex/cloud/dns/v1"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/operation"
	"google.golang.org/grpc"

	"sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/provider"
)

type recordSetAndZoneID struct {
	RecordSet *dns.RecordSet
	ZoneID    string
}

type mockDNSZoneClient struct {
	zones      map[string]*dns.DnsZone
	recordSets map[string]map[string]*dns.RecordSet
}

func newMockYandexProviderWithDefaultZones(defaultZones []*dns.DnsZone, folder string, domainFilter endpoint.DomainFilter, zoneIDFilter provider.ZoneIDFilter, zoneType string, dryRun bool) (*YandexProvider, error) {
	return newMockYandexProviderWithDefaultZonesAndRecordSets(defaultZones, []*recordSetAndZoneID{}, folder, domainFilter, zoneIDFilter, zoneType, dryRun)
}

func newMockYandexProviderWithDefaultZonesAndRecordSets(defaultZones []*dns.DnsZone, defaultRecords []*recordSetAndZoneID, folder string, domainFilter endpoint.DomainFilter, zoneIDFilter provider.ZoneIDFilter, zoneType string, dryRun bool) (*YandexProvider, error) {
	dnsZones := make(map[string]*dns.DnsZone)
	recordSets := make(map[string]map[string]*dns.RecordSet)
	for _, dnsZone := range defaultZones {
		dnsZones[dnsZone.Id] = dnsZone
		recordSets[dnsZone.Id] = make(map[string]*dns.RecordSet)
	}

	for _, recordSetAndZoneID := range defaultRecords {
		recordSets[recordSetAndZoneID.ZoneID][getRecordSetKey(recordSetAndZoneID.RecordSet)] = recordSetAndZoneID.RecordSet
	}

	mockClient := mockDNSZoneClient{
		zones:      dnsZones,
		recordSets: recordSets,
	}

	return NewYandexProviderWithCustomDNSZoneClient(&mockClient, folder, domainFilter, zoneIDFilter, zoneType, dryRun)
}

func (c *mockDNSZoneClient) List(ctx context.Context, in *dns.ListDnsZonesRequest, opts ...grpc.CallOption) (*dns.ListDnsZonesResponse, error) {
	var dnsZones []*dns.DnsZone
	for _, dnsZone := range c.zones {
		dnsZones = append(dnsZones, dnsZone)
	}

	response := dns.ListDnsZonesResponse{
		DnsZones:      dnsZones,
		NextPageToken: "",
	}

	return &response, nil
}

// UpdateRecordSets *operation.Operation is always nil
func (c *mockDNSZoneClient) UpdateRecordSets(ctx context.Context, in *dns.UpdateRecordSetsRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	if err := c.deleteRecords(in.DnsZoneId, in.Deletions); err != nil {
		return nil, err
	}

	err := c.addRecords(in.DnsZoneId, in.Additions)

	return nil, err
}

func (c *mockDNSZoneClient) ListRecordSets(ctx context.Context, in *dns.ListDnsZoneRecordSetsRequest, opts ...grpc.CallOption) (*dns.ListDnsZoneRecordSetsResponse, error) {
	zoneID := in.DnsZoneId
	if _, zoneExists := c.zones[zoneID]; !zoneExists {
		return nil, fmt.Errorf("zone with ID %s does not exists", zoneID)
	}

	var recordSets []*dns.RecordSet
	for _, recordSet := range c.recordSets[zoneID] {
		recordSets = append(recordSets, recordSet)
	}

	response := dns.ListDnsZoneRecordSetsResponse{
		RecordSets:    recordSets,
		NextPageToken: "",
	}

	return &response, nil
}

func (c *mockDNSZoneClient) deleteRecords(zoneID string, deletions []*dns.RecordSet) error {
	for _, recordSet := range deletions {
		if !c.recordSetExists(zoneID, recordSet) || !equalRecordSets(recordSet, c.recordSets[zoneID][getRecordSetKey(recordSet)]) {
			return fmt.Errorf("record set not found '%v' in zone with ID '%s'", recordSet, zoneID)
		}
	}

	for _, recordSet := range deletions {
		delete(c.recordSets[zoneID], getRecordSetKey(recordSet))
	}

	return nil
}

func (c *mockDNSZoneClient) addRecords(zoneID string, additions []*dns.RecordSet) error {
	for _, recordSet := range additions {
		if c.recordSetExists(zoneID, recordSet) && !equalRecordSets(recordSet, c.recordSets[zoneID][getRecordSetKey(recordSet)]) {
			return fmt.Errorf("record set with name'%s' and type '%s' already exists in zone with ID '%s'", recordSet.Name, recordSet.Type, zoneID)
		}
	}

	for _, recordSet := range additions {

		if !c.recordSetExists(zoneID, recordSet) {
			c.recordSets[zoneID][getRecordSetKey(recordSet)] = recordSet
		}
	}

	return nil
}

func (c *mockDNSZoneClient) recordSetExists(zoneID string, recordSet *dns.RecordSet) bool {
	if _, zoneExists := c.recordSets[zoneID]; !zoneExists {
		return false
	}

	_, recordSetExists := c.recordSets[zoneID][getRecordSetKey(recordSet)]

	return recordSetExists
}

func getRecordSetKey(recordSet *dns.RecordSet) string {
	return recordSet.Name + "|key|" + recordSet.Type
}

func equalRecordSets(first *dns.RecordSet, second *dns.RecordSet) bool {
	return equalRecordSetsData(first.Data, second.Data) &&
		first.Name == second.Name &&
		first.Type == second.Type &&
		first.Ttl == second.Ttl
}

func equalRecordSetsData(first []string, second []string) bool {
	if len(first) != len(second) {
		return false
	}

	firstMap := make(map[string]bool)
	for _, str := range first {
		firstMap[str] = true
	}

	secondMap := make(map[string]bool)
	for _, str := range second {
		secondMap[str] = true
	}

	for k, v := range firstMap {
		if v != secondMap[k] {
			return false
		}
	}

	return true
}
