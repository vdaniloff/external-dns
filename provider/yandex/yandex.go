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
	"io/ioutil"

	log "github.com/sirupsen/logrus"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/dns/v1"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/operation"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"github.com/yandex-cloud/go-sdk/iamkey"
	"google.golang.org/grpc"

	"sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/plan"
	"sigs.k8s.io/external-dns/provider"
)

const (
	yandexRecordTTL = 300
)

type dnsZoneClient interface {
	List(ctx context.Context, in *dns.ListDnsZonesRequest, opts ...grpc.CallOption) (*dns.ListDnsZonesResponse, error)
	UpdateRecordSets(ctx context.Context, in *dns.UpdateRecordSetsRequest, opts ...grpc.CallOption) (*operation.Operation, error)
	ListRecordSets(ctx context.Context, in *dns.ListDnsZoneRecordSetsRequest, opts ...grpc.CallOption) (*dns.ListDnsZoneRecordSetsResponse, error)
}

type YandexProvider struct {
	provider.BaseProvider
	// The Yandex Cloud folder to work in
	folder string
	// Enabled dry-run will print any modifying actions rather than execute them.
	dryRun bool
	// only consider hosted zones managing domains ending in this suffix
	domainFilter endpoint.DomainFilter
	// filter for zones based on visibility
	zoneTypeFilter provider.ZoneTypeFilter
	// only consider hosted zones ending with this zone id
	zoneIDFilter provider.ZoneIDFilter
	// Yandex Cloud SDK
	dnsZoneClient dnsZoneClient
}

func getSdkDNSZoneClient(ctx context.Context, iamKeyFile string) (dnsZoneClient, error) {
	contents, err := ioutil.ReadFile(iamKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read Yandex Cloud config file '%s': %v", iamKeyFile, err)
	}
	key := &iamkey.Key{}

	if err = key.UnmarshalJSON(contents); err != nil {
		return nil, fmt.Errorf("failed to read Yandex Cloud config file '%s': %v", iamKeyFile, err)
	}

	credentials, err := ycsdk.ServiceAccountKey(key)
	if err != nil {
		return nil, err
	}

	config := ycsdk.Config{Credentials: credentials}
	sdk, err := ycsdk.Build(ctx, config)
	if err != nil {
		return nil, err
	}

	dnsZoneClient := sdk.DNS().DnsZone()
	return dnsZoneClient, nil
}

//NewYandexProvider initializes a new Yandex Cloud DNS based Provider.
func NewYandexProvider(ctx context.Context, domainFilter endpoint.DomainFilter, zoneIDFilter provider.ZoneIDFilter, iamKeyFile string, folder string, zoneType string, dryRun bool) (*YandexProvider, error) {
	dnsZoneClient, err := getSdkDNSZoneClient(ctx, iamKeyFile)
	if err != nil {
		return nil, err
	}

	zoneTypeFilter := provider.NewZoneTypeFilter(zoneType)

	yandexProvider := YandexProvider{
		folder:         folder,
		dryRun:         dryRun,
		domainFilter:   domainFilter,
		zoneTypeFilter: zoneTypeFilter,
		zoneIDFilter:   zoneIDFilter,
		dnsZoneClient:  dnsZoneClient,
	}

	return &yandexProvider, nil
}

func NewYandexProviderWithCustomDNSZoneClient(dnsZoneClient dnsZoneClient, folder string, domainFilter endpoint.DomainFilter, zoneIDFilter provider.ZoneIDFilter, zoneType string, dryRun bool) (*YandexProvider, error) {
	zoneTypeFilter := provider.NewZoneTypeFilter(zoneType)

	yandexProvider := YandexProvider{
		folder:         folder,
		dryRun:         dryRun,
		domainFilter:   domainFilter,
		zoneTypeFilter: zoneTypeFilter,
		zoneIDFilter:   zoneIDFilter,
		dnsZoneClient:  dnsZoneClient,
	}

	return &yandexProvider, nil
}

func (p *YandexProvider) Records(ctx context.Context) (endpoints []*endpoint.Endpoint, err error) {
	zones, err := p.zones(ctx)
	if err != nil {
		return nil, err
	}
	for _, zone := range zones {
		req := dns.ListDnsZoneRecordSetsRequest{
			DnsZoneId: zone.Id,
		}

		records, err := p.dnsZoneClient.ListRecordSets(ctx, &req)
		if err != nil {
			return nil, err
		}

		for _, record := range records.RecordSets {
			if provider.SupportedRecordType(record.Type) {
				endpoints = append(endpoints, endpoint.NewEndpointWithTTL(record.Name, record.Type, endpoint.TTL(record.Ttl), record.Data...))
			}
		}
	}

	return endpoints, nil
}

func (p *YandexProvider) ApplyChanges(ctx context.Context, changes *plan.Changes) error {
	updateRequest := dns.UpdateRecordSetsRequest{
		Additions: p.endpointsToRecordSets(changes.Create),
		Deletions: p.endpointsToRecordSets(changes.Delete),
	}

	updateRequest.Deletions = append(updateRequest.Deletions, p.endpointsToRecordSets(changes.UpdateOld)...)
	updateRequest.Additions = append(updateRequest.Additions, p.endpointsToRecordSets(changes.UpdateNew)...)

	zones, err := p.zones(ctx)
	if err != nil {
		return err
	}

	separatedChanges := separateChange(zones, &updateRequest)

	for _, request := range separatedChanges {
		for _, del := range request.Deletions {
			log.Infof("Del records: %s %s %s %d", del.Name, del.Type, del.Data, del.Ttl)
		}

		for _, add := range request.Additions {
			log.Infof("Add records: %s %s %s %d", add.Name, add.Type, add.Data, add.Ttl)
		}

		if p.dryRun {
			continue
		}

		if _, err = p.dnsZoneClient.UpdateRecordSets(ctx, request); err != nil {
			return err
		}
	}

	return nil
}

func (p *YandexProvider) zones(ctx context.Context) (map[string]*dns.DnsZone, error) {
	zones := make(map[string]*dns.DnsZone)
	req := dns.ListDnsZonesRequest{
		FolderId: p.folder,
	}

	listZonesResp, err := p.dnsZoneClient.List(ctx, &req)
	if err != nil {
		return nil, err
	}

	log.Debugf("Matching zones against filters: domain: %v, type: %v, id: %v", p.domainFilter.Filters, p.zoneTypeFilter, p.zoneIDFilter.ZoneIDs)

	for _, zone := range listZonesResp.DnsZones {
		if p.domainFilter.Match(zone.Zone) && p.zoneTypeFilter.Match(getZoneType(zone)) && (p.zoneIDFilter.Match(fmt.Sprintf("%v", zone.Id)) || p.zoneIDFilter.Match(fmt.Sprintf("%v", zone.Name))) {
			zones[zone.Id] = zone
			log.Debugf("Matched zone: %s name: %s visibility: %s", zone.Zone, zone.Name, getZoneType(zone))
		} else {
			log.Debugf("Filtered zone: %s name: %s visibility: %s", zone.Zone, zone.Name, getZoneType(zone))
		}
	}

	return zones, nil
}

func (p *YandexProvider) endpointsToRecordSets(endpoints []*endpoint.Endpoint) (recordSets []*dns.RecordSet) {
	for _, endpoint := range endpoints {
		if p.domainFilter.Match(endpoint.DNSName) {
			recordSets = append(recordSets, endpointToRecordSet(endpoint))
		}
	}

	return recordSets
}

func endpointToRecordSet(e *endpoint.Endpoint) *dns.RecordSet {
	targets := make([]string, len(e.Targets))
	copy(targets, e.Targets)
	if e.RecordType == endpoint.RecordTypeCNAME {
		targets[0] = provider.EnsureTrailingDot(targets[0])
	}

	var ttl int64 = yandexRecordTTL
	if e.RecordTTL.IsConfigured() {
		ttl = int64(e.RecordTTL)
	}

	return &dns.RecordSet{
		Name: provider.EnsureTrailingDot(e.DNSName),
		Data: targets,
		Ttl:  ttl,
		Type: e.RecordType,
	}
}

func separateChange(zones map[string]*dns.DnsZone, change *dns.UpdateRecordSetsRequest) map[string]*dns.UpdateRecordSetsRequest {
	changes := make(map[string]*dns.UpdateRecordSetsRequest)
	zoneNameIDMapper := provider.ZoneIDName{}

	for _, zone := range zones {
		zoneNameIDMapper[zone.Id] = zone.Zone
		changes[zone.Id] = &dns.UpdateRecordSetsRequest{
			DnsZoneId: zone.Id,
			Additions: []*dns.RecordSet{},
			Deletions: []*dns.RecordSet{},
		}
	}

	for _, a := range change.Additions {
		if zoneName, _ := zoneNameIDMapper.FindZone(provider.EnsureTrailingDot(a.Name)); zoneName != "" {
			changes[zoneName].Additions = append(changes[zoneName].Additions, a)
		} else {
			log.Warnf("No matching zone for record addition: %s %s %s %d", a.Name, a.Type, a.Data, a.Ttl)
		}
	}

	for _, d := range change.Deletions {
		if zoneName, _ := zoneNameIDMapper.FindZone(provider.EnsureTrailingDot(d.Name)); zoneName != "" {
			changes[zoneName].Deletions = append(changes[zoneName].Deletions, d)
		} else {
			log.Warnf("No matching zone for record deletion: %s %s %s %d", d.Name, d.Type, d.Data, d.Ttl)
		}
	}

	for zone, change := range changes {
		if len(change.Additions) == 0 && len(change.Deletions) == 0 {
			delete(changes, zone)
		}
	}

	return changes
}

func getZoneType(zone *dns.DnsZone) string {
	if zone.PublicVisibility != nil {
		return "public"
	}

	return "private"
}
