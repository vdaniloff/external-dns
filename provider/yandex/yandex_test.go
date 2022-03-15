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
	"sigs.k8s.io/external-dns/plan"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/dns/v1"
	"golang.org/x/net/context"

	"sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/provider"
)

const (
	defaultFolderId = "default-folder-id"

	firstDefaultPublicZone   = "default.zone.public.first."
	secondDefaultPublicZone  = "default.zone.public.second."
	thirdDefaultPublicZone   = "default.zone.public.third."
	firstDefaultPrivateZone  = "default.zone.private.first."
	secondDefaultPrivateZone = "default.zone.private.second."
	thirdDefaultPrivateZone  = "default.zone.private.third."

	firstDefaultRecord  = "default.record.first"
	secondDefaultRecord = "default.record.second"
	thirdDefaultRecord  = "default.record.third"

	firstAddedRecord  = "added.record.first"
	secondAddedRecord = "added.record.second"
	thirdAddedRecord  = "added.record.third"
)

var (
	defaultPublicZones  = []string{firstDefaultPublicZone, secondDefaultPublicZone, thirdDefaultPublicZone}
	defaultPrivateZones = []string{firstDefaultPrivateZone, secondDefaultPrivateZone, thirdDefaultPrivateZone}

	defaultRecords = []string{firstDefaultRecord, secondDefaultRecord, thirdDefaultRecord}
	addedRecords   = []string{firstAddedRecord, secondAddedRecord, thirdAddedRecord}

	emptyDnsZones               []*dns.DnsZone
	defaultDnsZones             = append(defaultPublicDnsZones, defaultPrivateDnsZones...)
	defaultPublicDnsZones       = []*dns.DnsZone{firstDefaultPublicDnsZone, secondDefaultPublicDnsZone, thirdDefaultPublicDnsZone}
	defaultPrivateDnsZones      = []*dns.DnsZone{firstDefaultPrivateDnsZone, secondDefaultPrivateDnsZone, thirdDefaultPrivateDnsZone}
	firstDefaultPublicDnsZone   = getPublicDnsZoneByZone(firstDefaultPublicZone)
	secondDefaultPublicDnsZone  = getPublicDnsZoneByZone(secondDefaultPublicZone)
	thirdDefaultPublicDnsZone   = getPublicDnsZoneByZone(thirdDefaultPublicZone)
	firstDefaultPrivateDnsZone  = getPrivateDnsZoneByZone(firstDefaultPrivateZone)
	secondDefaultPrivateDnsZone = getPrivateDnsZoneByZone(secondDefaultPrivateZone)
	thirdDefaultPrivateDnsZone  = getPrivateDnsZoneByZone(thirdDefaultPrivateZone)

	emptyDnsRecordSets               []*recordSetAndZoneID
	defaultRecordSets                = append(defaultTXTRecordSets, defaultARecordSets...)
	defaultTXTRecordSets             = append(defaultTXTRecordSetsPublicZones, defaultTXTRecordSetsPrivateZones...)
	defaultTXTRecordSetsPublicZones  = getRecordSetsByZonesFunction(defaultPublicZones, getDefaultTXTRecordSetsByZone)
	defaultTXTRecordSetsPrivateZones = getRecordSetsByZonesFunction(defaultPrivateZones, getDefaultTXTRecordSetsByZone)
	defaultARecordSets               = append(defaultARecordSetsPublicZones, defaultARecordSetsPrivateZones...)
	defaultARecordSetsPublicZones    = getRecordSetsByZonesFunction(defaultPublicZones, getDefaultARecordSetsByZone)
	defaultARecordSetsPrivateZones   = getRecordSetsByZonesFunction(defaultPrivateZones, getDefaultARecordSetsByZone)

	addedRecordSets                = append(addedTXTRecordSets, addedARecordSets...)
	addedTXTRecordSets             = append(addedTXTRecordSetsPublicZones, addedTXTRecordSetsPrivateZones...)
	addedTXTRecordSetsPublicZones  = getRecordSetsByZonesFunction(defaultPublicZones, getAddedTXTRecordSetsByZone)
	addedTXTRecordSetsPrivateZones = getRecordSetsByZonesFunction(defaultPrivateZones, getAddedTXTRecordSetsByZone)
	addedARecordSets               = append(addedARecordSetsPublicZones, addedARecordSetsPrivateZones...)
	addedARecordSetsPublicZones    = getRecordSetsByZonesFunction(defaultPublicZones, getAddedARecordSetsByZone)
	addedARecordSetsPrivateZones   = getRecordSetsByZonesFunction(defaultPrivateZones, getAddedARecordSetsByZone)

	allRecordSets                = append(allTXTRecordSets, allARecordSets...)
	allTXTRecordSets             = append(allTXTRecordSetsPublicZones, allTXTRecordSetsPrivateZones...)
	allTXTRecordSetsPublicZones  = append(defaultTXTRecordSetsPublicZones, addedTXTRecordSetsPublicZones...)
	allTXTRecordSetsPrivateZones = append(defaultTXTRecordSetsPrivateZones, addedTXTRecordSetsPrivateZones...)
	allARecordSets               = append(allARecordSetsPublicZones, allARecordSetsPrivateZones...)
	allARecordSetsPublicZones    = append(defaultARecordSetsPublicZones, addedARecordSetsPublicZones...)
	allARecordSetsPrivateZones   = append(defaultARecordSetsPrivateZones, addedARecordSetsPrivateZones...)

	firstRecordSetWithDiffTTL        = []*recordSetAndZoneID{getDefaultRecord(firstDefaultPublicZone, firstDefaultRecord, "A", yandexRecordTTL+1, "192.0.2.1")}
	firstRecordSetWithDiffData       = []*recordSetAndZoneID{getDefaultRecord(firstDefaultPublicZone, firstDefaultRecord, "A", yandexRecordTTL, "192.0.2.2")}
	firstRecordSetWithDiffTTLAndData = []*recordSetAndZoneID{getDefaultRecord(firstDefaultPublicZone, firstDefaultRecord, "A", yandexRecordTTL+1, "192.0.2.2")}

	firstAddedRecordSet = []*recordSetAndZoneID{getDefaultRecord(firstDefaultPublicZone, firstAddedRecord, "A", yandexRecordTTL, "192.0.2.1")}

	invalidTypeRecordSet = []*recordSetAndZoneID{getDefaultRecord(firstDefaultPublicZone, firstDefaultRecord, "UNSUPPORTED", yandexRecordTTL, "unsupported")}
)

func getDnsZoneIdByZone(zone string) string {
	return strings.ReplaceAll(zone, ".", "-") + "id"
}

func getDnsZoneDescriptionByZone(zone string) string {
	return strings.ReplaceAll(zone, ".", "-") + "description"
}

func getDnsZoneNameByZone(zone string) string {
	return strings.ReplaceAll(zone, ".", "-") + "name"
}

func getPublicDnsZoneByZone(zone string) *dns.DnsZone {
	return &dns.DnsZone{
		Id:                getDnsZoneIdByZone(zone),
		FolderId:          defaultFolderId,
		CreatedAt:         nil,
		Name:              getDnsZoneNameByZone(zone),
		Description:       getDnsZoneDescriptionByZone(zone),
		Labels:            nil,
		Zone:              zone,
		PrivateVisibility: nil,
		PublicVisibility:  &dns.PublicVisibility{},
	}
}

func getPrivateDnsZoneByZone(zone string) *dns.DnsZone {
	return &dns.DnsZone{
		Id:                getDnsZoneIdByZone(zone),
		FolderId:          defaultFolderId,
		CreatedAt:         nil,
		Name:              getDnsZoneNameByZone(zone),
		Description:       getDnsZoneDescriptionByZone(zone),
		Labels:            nil,
		Zone:              zone,
		PrivateVisibility: &dns.PrivateVisibility{},
		PublicVisibility:  nil,
	}
}

func getRecordSetNameByZoneAndRecord(zone string, record string) string {
	return record + "." + zone
}

func getDefaultRecord(zone string, record string, recordType string, ttl int64, data ...string) *recordSetAndZoneID {
	recordSet := &dns.RecordSet{
		Name: getRecordSetNameByZoneAndRecord(zone, record),
		Type: recordType,
		Ttl:  ttl,
		Data: data,
	}

	return &recordSetAndZoneID{ZoneID: getDnsZoneIdByZone(zone), RecordSet: recordSet}
}

func getARecordSetsByZoneAndRecords(zone string, records []string) (recordSetAndZoneIDs []*recordSetAndZoneID) {
	for _, record := range records {
		recordSetAndZoneIDs = append(recordSetAndZoneIDs, getDefaultRecord(zone, record, "A", yandexRecordTTL, "192.0.2.1"))
	}

	return recordSetAndZoneIDs
}

func getTXTRecordSetsByZoneAndRecords(zone string, records []string) (recordSetAndZoneIDs []*recordSetAndZoneID) {
	var data []string
	for i, record := range records {
		data = append(data, record+strconv.Itoa(i))
		recordSetAndZoneIDs = append(recordSetAndZoneIDs, getDefaultRecord(zone, record, "TXT", yandexRecordTTL, data...))
	}

	return recordSetAndZoneIDs
}

func getDefaultARecordSetsByZone(zone string) []*recordSetAndZoneID {
	return getARecordSetsByZoneAndRecords(zone, defaultRecords)
}

func getDefaultTXTRecordSetsByZone(zone string) []*recordSetAndZoneID {
	return getTXTRecordSetsByZoneAndRecords(zone, defaultRecords)
}

func getAddedARecordSetsByZone(zone string) []*recordSetAndZoneID {
	return getARecordSetsByZoneAndRecords(zone, addedRecords)
}

func getAddedTXTRecordSetsByZone(zone string) []*recordSetAndZoneID {
	return getTXTRecordSetsByZoneAndRecords(zone, addedRecords)
}

func getRecordSetsByZonesFunction(zones []string, getRecordSetsFunction func(string) []*recordSetAndZoneID) (recordSetAndZoneIDs []*recordSetAndZoneID) {
	for _, zone := range zones {
		recordSetAndZoneIDs = append(recordSetAndZoneIDs, getRecordSetsFunction(zone)...)
	}

	return recordSetAndZoneIDs
}

func TestYandexProvider(t *testing.T) {
	TestYandexDnsZoneFilter(t)
	TestYandexRecords(t)
	TestYandexApplyChanges(t)
}

func TestYandexDnsZoneFilter(t *testing.T) {
	TestYandexDnsZoneTypeFilter(t)
	TestYandexDnsZoneDomainFilter(t)
	TestYandexDnsZoneIDFilter(t)
	TestYandexDnsZoneAllFilters(t)
}

func TestYandexApplyChanges(t *testing.T) {
	TestYandexApplyChangesCreate(t)
	TestYandexApplyChangesUpdate(t)
	TestYandexApplyChangesDelete(t)
	TestYandexApplyAllChanges(t)
}

func TestYandexApplyAllChanges(t *testing.T) {
	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(addedRecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(defaultRecordSets),
	}, false, defaultDnsZones, defaultRecordSets, addedRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(defaultRecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(defaultRecordSets),
	}, false, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(defaultRecordSets),
		UpdateOld: recordSetAndZoneIDsToEndpoints(addedRecordSets),
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(defaultRecordSets),
	}, false, defaultDnsZones, allRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

}

func TestYandexApplyChangesCreate(t *testing.T) {
	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(addedRecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, emptyDnsRecordSets, addedRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(addedRecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, defaultRecordSets, allRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(addedRecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, defaultRecordSets, allRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(firstRecordSetWithDiffTTL),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, true, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(firstRecordSetWithDiffData),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, true, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(firstRecordSetWithDiffTTLAndData),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, true, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(append(firstAddedRecordSet, firstRecordSetWithDiffTTL...)),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, true, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(append(firstAddedRecordSet, firstRecordSetWithDiffData...)),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, true, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(append(firstAddedRecordSet, firstRecordSetWithDiffTTLAndData...)),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, true, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(addedARecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, emptyDnsRecordSets, addedARecordSetsPublicZones, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "public", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(addedARecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, emptyDnsRecordSets, addedARecordSetsPrivateZones, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "private", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(addedARecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, emptyDnsRecordSets, getRecordSetsByZonesFunction([]string{firstDefaultPublicZone, firstDefaultPrivateZone}, getAddedARecordSetsByZone), endpoint.NewDomainFilter([]string{"first."}), provider.NewZoneIDFilter([]string{""}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(addedARecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, emptyDnsRecordSets, getRecordSetsByZonesFunction([]string{firstDefaultPublicZone}, getAddedARecordSetsByZone), endpoint.NewDomainFilter([]string{"public.first."}), provider.NewZoneIDFilter([]string{""}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(addedARecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, emptyDnsRecordSets, emptyDnsRecordSets, endpoint.NewDomainFilter([]string{"public.first."}), provider.NewZoneIDFilter([]string{""}), "private", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(addedRecordSets),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, emptyDnsZones, emptyDnsRecordSets, emptyDnsRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    recordSetAndZoneIDsToEndpoints(invalidTypeRecordSet),
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

}

func TestYandexApplyChangesUpdate(t *testing.T) {
	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: recordSetAndZoneIDsToEndpoints(defaultRecordSets),
		UpdateNew: recordSetAndZoneIDsToEndpoints(defaultRecordSets),
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: recordSetAndZoneIDsToEndpoints(addedRecordSets),
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, allRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: recordSetAndZoneIDsToEndpoints(addedRecordSets),
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, defaultRecordSets, allRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: recordSetAndZoneIDsToEndpoints(allRecordSets),
		UpdateNew: recordSetAndZoneIDsToEndpoints(defaultRecordSets),
		Delete:    []*endpoint.Endpoint{},
	}, false, defaultDnsZones, allRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)
}

func TestYandexApplyChangesDelete(t *testing.T) {
	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(addedRecordSets),
	}, false, defaultDnsZones, allRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(addedRecordSets),
	}, false, defaultDnsZones, addedRecordSets, emptyDnsRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(addedRecordSets),
	}, false, defaultDnsZones, addedRecordSets, emptyDnsRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(addedRecordSets),
	}, true, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(firstRecordSetWithDiffTTL),
	}, true, defaultDnsZones, allRecordSets, allRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(firstRecordSetWithDiffData),
	}, true, defaultDnsZones, allRecordSets, allRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(firstRecordSetWithDiffTTLAndData),
	}, true, defaultDnsZones, allRecordSets, allRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(append(firstAddedRecordSet, firstRecordSetWithDiffTTL...)),
	}, true, defaultDnsZones, allRecordSets, allRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(append(firstAddedRecordSet, firstRecordSetWithDiffData...)),
	}, true, defaultDnsZones, allRecordSets, allRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	applyChangesTest(t, &plan.Changes{
		Create:    []*endpoint.Endpoint{},
		UpdateOld: []*endpoint.Endpoint{},
		UpdateNew: []*endpoint.Endpoint{},
		Delete:    recordSetAndZoneIDsToEndpoints(append(firstAddedRecordSet, firstRecordSetWithDiffTTLAndData...)),
	}, true, defaultDnsZones, allRecordSets, allRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

}

func TestYandexRecords(t *testing.T) {
	recordsTest(t, defaultDnsZones, defaultARecordSets, defaultARecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)
	recordsTest(t, defaultDnsZones, defaultTXTRecordSets, defaultTXTRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)
	recordsTest(t, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)
	recordsTest(t, defaultDnsZones, defaultRecordSets, defaultRecordSets, endpoint.NewDomainFilter([]string{"first.", "second.", "third."}), provider.NewZoneIDFilter([]string{}), "", false)
	recordsTest(t, defaultDnsZones, defaultTXTRecordSets, getRecordSetsByZonesFunction(defaultPublicZones, getDefaultTXTRecordSetsByZone), endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "public", false)
}

func TestYandexDnsZoneTypeFilter(t *testing.T) {
	filterTest(t, defaultDnsZones, defaultPublicDnsZones, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "public", false)
	filterTest(t, defaultDnsZones, defaultPrivateDnsZones, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "private", false)
}

func TestYandexDnsZoneDomainFilter(t *testing.T) {
	filterTest(t, defaultDnsZones, defaultDnsZones, endpoint.NewDomainFilter([]string{"first.", "second.", "third."}), provider.NewZoneIDFilter([]string{}), "", false)
	filterTest(t, defaultDnsZones, []*dns.DnsZone{firstDefaultPublicDnsZone, firstDefaultPrivateDnsZone}, endpoint.NewDomainFilter([]string{"first."}), provider.NewZoneIDFilter([]string{}), "", false)
	filterTest(t, defaultDnsZones, []*dns.DnsZone{firstDefaultPublicDnsZone}, endpoint.NewDomainFilter([]string{"public.first."}), provider.NewZoneIDFilter([]string{}), "", false)
	filterTest(t, defaultDnsZones, emptyDnsZones, endpoint.NewDomainFilter([]string{"com."}), provider.NewZoneIDFilter([]string{}), "", false)
}

func TestYandexDnsZoneIDFilter(t *testing.T) {
	filterTest(t, defaultDnsZones, defaultDnsZones, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter(getDnsZonesID(defaultDnsZones)), "", false)
	filterTest(t, defaultDnsZones, emptyDnsZones, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{"no such dns zone id"}), "", false)
}

func TestYandexDnsZoneAllFilters(t *testing.T) {
	filterTest(t, defaultDnsZones, defaultDnsZones, endpoint.NewDomainFilter([]string{""}), provider.NewZoneIDFilter([]string{}), "", false)

	filterTest(t, defaultDnsZones, defaultPublicDnsZones, endpoint.NewDomainFilter([]string{"first.", "second.", "third."}), provider.NewZoneIDFilter(getDnsZonesID(defaultPublicDnsZones)), "public", false)
	filterTest(t, defaultDnsZones, defaultPrivateDnsZones, endpoint.NewDomainFilter([]string{"first.", "second.", "third."}), provider.NewZoneIDFilter(getDnsZonesID(defaultPrivateDnsZones)), "private", false)

	filterTest(t, defaultDnsZones, emptyDnsZones, endpoint.NewDomainFilter([]string{"first.", "second.", "third."}), provider.NewZoneIDFilter(getDnsZonesID(defaultPublicDnsZones)), "private", false)
	filterTest(t, defaultDnsZones, emptyDnsZones, endpoint.NewDomainFilter([]string{"first.", "second.", "third."}), provider.NewZoneIDFilter(getDnsZonesID(defaultPrivateDnsZones)), "public", false)
	filterTest(t, defaultDnsZones, emptyDnsZones, endpoint.NewDomainFilter([]string{"first.", "second.", "third."}), provider.NewZoneIDFilter(getDnsZonesID(defaultPrivateDnsZones)), "public", false)
	filterTest(t, defaultDnsZones, emptyDnsZones, endpoint.NewDomainFilter([]string{"first.", "second.", "third."}), provider.NewZoneIDFilter(getDnsZonesID(defaultPublicDnsZones)), "private", false)
	filterTest(t, defaultDnsZones, emptyDnsZones, endpoint.NewDomainFilter([]string{"private.first.", "private.second.", "private.third."}), provider.NewZoneIDFilter(getDnsZonesID(defaultPublicDnsZones)), "public", false)
	filterTest(t, defaultDnsZones, emptyDnsZones, endpoint.NewDomainFilter([]string{"public.first.", "public.second.", "public.third."}), provider.NewZoneIDFilter(getDnsZonesID(defaultPrivateDnsZones)), "private", false)

	filterTest(t, defaultDnsZones, []*dns.DnsZone{firstDefaultPublicDnsZone}, endpoint.NewDomainFilter([]string{"first."}), provider.NewZoneIDFilter([]string{}), "public", false)
	filterTest(t, defaultDnsZones, []*dns.DnsZone{firstDefaultPublicDnsZone}, endpoint.NewDomainFilter([]string{"public.first."}), provider.NewZoneIDFilter([]string{}), "", false)
}

func filterTest(t *testing.T, defaultZones []*dns.DnsZone, expectedZones []*dns.DnsZone, domainFilter endpoint.DomainFilter, zoneIDFilter provider.ZoneIDFilter, zoneType string, dryRun bool) {
	yandexProvider, _ := newMockYandexProviderWithDefaultZones(defaultZones, defaultFolderId, domainFilter, zoneIDFilter, zoneType, dryRun)
	dnsZonesMap, err := yandexProvider.zones(context.Background())
	require.NoError(t, err)
	expectedDnsZonesMap := dnsZonesToDnsZonesMap(expectedZones)
	assert.True(t, equalDnsZonesMaps(dnsZonesMap, expectedDnsZonesMap))
}

func equalDnsZonesMaps(first map[string]*dns.DnsZone, second map[string]*dns.DnsZone) bool {
	if len(first) != len(second) {
		return false
	}

	for zoneName, firstDnsZone := range first {
		if secondDnsZone, ok := second[zoneName]; !ok || !equalDnsZones(firstDnsZone, secondDnsZone) {
			return false
		}
	}

	return true
}

func dnsZonesToDnsZonesMap(dnsZones []*dns.DnsZone) map[string]*dns.DnsZone {
	dnsZonesMap := make(map[string]*dns.DnsZone)

	for _, dnsZone := range dnsZones {
		dnsZonesMap[dnsZone.Id] = dnsZone
	}

	return dnsZonesMap
}

func equalDnsZones(first *dns.DnsZone, second *dns.DnsZone) bool {
	if first == nil && second == nil {
		return true
	}

	if first == nil || second == nil {
		return false
	}

	return first.Zone == second.Zone &&
		first.Name == second.Name &&
		first.Id == second.Id &&
		first.FolderId == second.FolderId &&
		first.Description == second.Description &&
		getZoneType(first) == getZoneType(second)
}

func getDnsZonesID(dnsZones []*dns.DnsZone) (dnsZonesID []string) {
	for _, dnsZone := range dnsZones {
		dnsZonesID = append(dnsZonesID, dnsZone.Id)
	}

	return dnsZonesID
}

func recordsTest(t *testing.T, defaultZones []*dns.DnsZone, defaultRecordSetAndZoneIDs []*recordSetAndZoneID, expectedRecordSetAndZoneIDs []*recordSetAndZoneID, domainFilter endpoint.DomainFilter, zoneIDFilter provider.ZoneIDFilter, zoneType string, dryRun bool) {
	yandexProvider, _ := newMockYandexProviderWithDefaultZonesAndRecordSets(defaultZones, defaultRecordSetAndZoneIDs, defaultFolderId, domainFilter, zoneIDFilter, zoneType, dryRun)
	expectedRecordSetsMap := recordSetAndZoneIDsToRecordSetsMap(expectedRecordSetAndZoneIDs)

	endpoints, err := yandexProvider.Records(context.Background())
	require.NoError(t, err)

	recordSetsMap := recordSetsToRecordSetsMap(yandexProvider.endpointsToRecordSets(endpoints))

	assert.True(t, equalRecordSetsMaps(expectedRecordSetsMap, recordSetsMap))
}

func recordSetAndZoneIDsToRecordSetsMap(recordSetAndZoneIDs []*recordSetAndZoneID) map[string]*dns.RecordSet {
	recordSetsMap := make(map[string]*dns.RecordSet)

	for _, recordSetAndZoneID := range recordSetAndZoneIDs {
		recordSetsMap[getRecordSetKey(recordSetAndZoneID.RecordSet)] = recordSetAndZoneID.RecordSet
	}

	return recordSetsMap
}

func recordSetsToRecordSetsMap(recordSets []*dns.RecordSet) map[string]*dns.RecordSet {
	recordSetsMap := make(map[string]*dns.RecordSet)

	for _, recordSet := range recordSets {
		recordSetsMap[getRecordSetKey(recordSet)] = recordSet
	}

	return recordSetsMap
}

func equalRecordSetsMaps(first map[string]*dns.RecordSet, second map[string]*dns.RecordSet) bool {
	if len(first) != len(second) {
		return false
	}

	for key, firstRecordSet := range first {
		if secondRecordSet, ok := second[key]; !ok || !equalRecordSets(firstRecordSet, secondRecordSet) {
			return false
		}
	}

	return true
}

func applyChangesTest(t *testing.T, changes *plan.Changes, errorExpected bool, defaultZones []*dns.DnsZone, defaultrecordSetAndZoneIDs []*recordSetAndZoneID, expectedrecordSetAndZoneIDs []*recordSetAndZoneID, domainFilter endpoint.DomainFilter, zoneIDFilter provider.ZoneIDFilter, zoneType string, dryRun bool) {
	if !dryRun {
		applyChangesTest(t, changes, false, defaultZones, defaultrecordSetAndZoneIDs, defaultrecordSetAndZoneIDs, domainFilter, zoneIDFilter, zoneType, true)
	}

	yandexProvider, _ := newMockYandexProviderWithDefaultZonesAndRecordSets(defaultZones, defaultrecordSetAndZoneIDs, defaultFolderId, domainFilter, zoneIDFilter, zoneType, dryRun)
	expectedRecordSetsMap := recordSetAndZoneIDsToRecordSetsMap(expectedrecordSetAndZoneIDs)

	err := yandexProvider.ApplyChanges(context.Background(), changes)

	require.Equal(t, errorExpected, err != nil)

	endpoints, err := yandexProvider.Records(context.Background())
	require.NoError(t, err)

	recordSetsMap := recordSetsToRecordSetsMap(yandexProvider.endpointsToRecordSets(endpoints))

	assert.True(t, equalRecordSetsMaps(expectedRecordSetsMap, recordSetsMap))
}

func recordSetToEndpoint(recordSet *dns.RecordSet) *endpoint.Endpoint {
	return endpoint.NewEndpointWithTTL(recordSet.Name, recordSet.Type, endpoint.TTL(recordSet.Ttl), recordSet.Data...)
}

func recordSetAndZoneIDsToEndpoints(recordSetAndZoneIDs []*recordSetAndZoneID) (endpoints []*endpoint.Endpoint) {
	for _, recordSetAndZoneID := range recordSetAndZoneIDs {
		endpoints = append(endpoints, recordSetToEndpoint(recordSetAndZoneID.RecordSet))
	}

	return endpoints
}
