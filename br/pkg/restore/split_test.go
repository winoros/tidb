// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TestClient struct {
	mu                  sync.RWMutex
	stores              map[uint64]*metapb.Store
	regions             map[uint64]*split.RegionInfo
	regionsInfo         *pdtypes.RegionTree // For now it's only used in ScanRegions
	nextRegionID        uint64
	injectInScatter     func(*split.RegionInfo) error
	injectInOperator    func(uint64) (*pdpb.GetOperatorResponse, error)
	supportBatchScatter bool

	scattered   map[uint64]bool
	InjectErr   bool
	InjectTimes int32
}

func NewTestClient(
	stores map[uint64]*metapb.Store,
	regions map[uint64]*split.RegionInfo,
	nextRegionID uint64,
) *TestClient {
	regionsInfo := &pdtypes.RegionTree{}
	for _, regionInfo := range regions {
		regionsInfo.SetRegion(pdtypes.NewRegionInfo(regionInfo.Region, regionInfo.Leader))
	}
	return &TestClient{
		stores:          stores,
		regions:         regions,
		regionsInfo:     regionsInfo,
		nextRegionID:    nextRegionID,
		scattered:       map[uint64]bool{},
		injectInScatter: func(*split.RegionInfo) error { return nil },
	}
}

func (c *TestClient) InstallBatchScatterSupport() {
	c.supportBatchScatter = true
}

// ScatterRegions scatters regions in a batch.
func (c *TestClient) ScatterRegions(ctx context.Context, regionInfo []*split.RegionInfo) error {
	if !c.supportBatchScatter {
		return status.Error(codes.Unimplemented, "Ah, yep")
	}
	regions := map[uint64]*split.RegionInfo{}
	for _, region := range regionInfo {
		regions[region.Region.Id] = region
	}
	var err error
	for i := 0; i < 3; i++ {
		if len(regions) == 0 {
			return nil
		}
		for id, region := range regions {
			splitErr := c.ScatterRegion(ctx, region)
			if splitErr == nil {
				delete(regions, id)
			}
			err = multierr.Append(err, splitErr)
		}
	}
	return nil
}

func (c *TestClient) GetAllRegions() map[uint64]*split.RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regions
}

func (c *TestClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	store, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store not found")
	}
	return store, nil
}

func (c *TestClient) GetRegion(ctx context.Context, key []byte) (*split.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, region := range c.regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(key, region.Region.EndKey) < 0) {
			return region, nil
		}
	}
	return nil, errors.Errorf("region not found: key=%s", string(key))
}

func (c *TestClient) GetRegionByID(ctx context.Context, regionID uint64) (*split.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	region, ok := c.regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found: id=%d", regionID)
	}
	return region, nil
}

func (c *TestClient) SplitRegion(
	ctx context.Context,
	regionInfo *split.RegionInfo,
	key []byte,
) (*split.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var target *split.RegionInfo
	splitKey := codec.EncodeBytes([]byte{}, key)
	for _, region := range c.regions {
		if bytes.Compare(splitKey, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(splitKey, region.Region.EndKey) < 0) {
			target = region
		}
	}
	if target == nil {
		return nil, errors.Errorf("region not found: key=%s", string(key))
	}
	newRegion := &split.RegionInfo{
		Region: &metapb.Region{
			Peers:    target.Region.Peers,
			Id:       c.nextRegionID,
			StartKey: target.Region.StartKey,
			EndKey:   splitKey,
		},
	}
	c.regions[c.nextRegionID] = newRegion
	c.nextRegionID++
	target.Region.StartKey = splitKey
	c.regions[target.Region.Id] = target
	return newRegion, nil
}

func (c *TestClient) BatchSplitRegionsWithOrigin(
	ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte,
) (*split.RegionInfo, []*split.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	newRegions := make([]*split.RegionInfo, 0)
	var region *split.RegionInfo
	for _, key := range keys {
		var target *split.RegionInfo
		splitKey := codec.EncodeBytes([]byte{}, key)
		for _, region := range c.regions {
			if region.ContainsInterior(splitKey) {
				target = region
			}
		}
		if target == nil {
			continue
		}
		newRegion := &split.RegionInfo{
			Region: &metapb.Region{
				Peers:    target.Region.Peers,
				Id:       c.nextRegionID,
				StartKey: target.Region.StartKey,
				EndKey:   splitKey,
			},
		}
		c.regions[c.nextRegionID] = newRegion
		c.nextRegionID++
		target.Region.StartKey = splitKey
		c.regions[target.Region.Id] = target
		region = target
		newRegions = append(newRegions, newRegion)
	}
	return region, newRegions, nil
}

func (c *TestClient) BatchSplitRegions(
	ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte,
) ([]*split.RegionInfo, error) {
	_, newRegions, err := c.BatchSplitRegionsWithOrigin(ctx, regionInfo, keys)
	return newRegions, err
}

func (c *TestClient) ScatterRegion(ctx context.Context, regionInfo *split.RegionInfo) error {
	return c.injectInScatter(regionInfo)
}

func (c *TestClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	if c.injectInOperator != nil {
		return c.injectInOperator(regionID)
	}
	return &pdpb.GetOperatorResponse{
		Header: new(pdpb.ResponseHeader),
	}, nil
}

func (c *TestClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*split.RegionInfo, error) {
	if c.InjectErr && c.InjectTimes > 0 {
		c.InjectTimes -= 1
		return nil, status.Error(codes.Unavailable, "not leader")
	}

	infos := c.regionsInfo.ScanRange(key, endKey, limit)
	regions := make([]*split.RegionInfo, 0, len(infos))
	for _, info := range infos {
		regions = append(regions, &split.RegionInfo{
			Region: info.Meta,
			Leader: info.Leader,
		})
	}
	return regions, nil
}

func (c *TestClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (r pdtypes.Rule, err error) {
	return
}

func (c *TestClient) SetPlacementRule(ctx context.Context, rule pdtypes.Rule) error {
	return nil
}

func (c *TestClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return nil
}

func (c *TestClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	return nil
}

type assertRetryLessThanBackoffer struct {
	max     int
	already int
	t       *testing.T
}

func assertRetryLessThan(t *testing.T, times int) utils.Backoffer {
	return &assertRetryLessThanBackoffer{
		max:     times,
		already: 0,
		t:       t,
	}
}

// NextBackoff returns a duration to wait before retrying again
func (b *assertRetryLessThanBackoffer) NextBackoff(err error) time.Duration {
	b.already++
	if b.already >= b.max {
		b.t.Logf("retry more than %d time: test failed", b.max)
		b.t.FailNow()
	}
	return 0
}

// Attempt returns the remain attempt times
func (b *assertRetryLessThanBackoffer) Attempt() int {
	return b.max - b.already
}

func TestScatterFinishInTime(t *testing.T) {
	client := initTestClient(false)
	ranges := initRanges()
	rewriteRules := initRewriteRules()
	regionSplitter := restore.NewRegionSplitter(client)

	ctx := context.Background()
	err := regionSplitter.Split(ctx, ranges, rewriteRules, false, func(key [][]byte) {})
	require.NoError(t, err)
	regions := client.GetAllRegions()
	if !validateRegions(regions) {
		for _, region := range regions {
			t.Logf("region: %v\n", region.Region)
		}
		t.Log("get wrong result")
		t.Fail()
	}

	regionInfos := make([]*split.RegionInfo, 0, len(regions))
	for _, info := range regions {
		regionInfos = append(regionInfos, info)
	}
	failed := map[uint64]int{}
	client.injectInScatter = func(r *split.RegionInfo) error {
		failed[r.Region.Id]++
		if failed[r.Region.Id] > 7 {
			return nil
		}
		return status.Errorf(codes.Unknown, "region %d is not fully replicated", r.Region.Id)
	}

	// When using a exponential backoffer, if we try to backoff more than 40 times in 10 regions,
	// it would cost time unacceptable.
	regionSplitter.ScatterRegionsWithBackoffer(ctx,
		regionInfos,
		assertRetryLessThan(t, 40))
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
// rewrite rules: aa -> xx,  cc -> bb
// expected regions after split:
//
//	[, aay), [aay, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//	[bbj, cca), [cca, xxe), [xxe, xxz), [xxz, )
func TestSplitAndScatter(t *testing.T) {
	t.Run("BatchScatter", func(t *testing.T) {
		client := initTestClient(false)
		client.InstallBatchScatterSupport()
		runTestSplitAndScatterWith(t, client)
	})
	t.Run("BackwardCompatibility", func(t *testing.T) {
		client := initTestClient(false)
		runTestSplitAndScatterWith(t, client)
	})
	t.Run("WaitScatter", func(t *testing.T) {
		client := initTestClient(false)
		client.InstallBatchScatterSupport()
		runWaitScatter(t, client)
	})
}

// +------------+----------------------------
// |   region   | states
// +------------+----------------------------
// | [   , aay) | SUCCESS
// +------------+----------------------------
// | [aay, bba) | CANCEL, SUCCESS
// +------------+----------------------------
// | [bba, bbh) | RUNNING, TIMEOUT, SUCCESS
// +------------+----------------------------
// | [bbh, cca) | <NOT_SCATTER_OPEARTOR>
// +------------+----------------------------
// | [cca,    ) | CANCEL, RUNNING, SUCCESS
// +------------+----------------------------
// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
// states:
func runWaitScatter(t *testing.T, client *TestClient) {
	// configuration
	type Operatorstates struct {
		index  int
		status []pdpb.OperatorStatus
	}
	results := map[string]*Operatorstates{
		"": {status: []pdpb.OperatorStatus{pdpb.OperatorStatus_SUCCESS}},
		string(codec.EncodeBytesExt([]byte{}, []byte("aay"), false)): {status: []pdpb.OperatorStatus{pdpb.OperatorStatus_CANCEL, pdpb.OperatorStatus_SUCCESS}},
		string(codec.EncodeBytesExt([]byte{}, []byte("bba"), false)): {status: []pdpb.OperatorStatus{pdpb.OperatorStatus_RUNNING, pdpb.OperatorStatus_TIMEOUT, pdpb.OperatorStatus_SUCCESS}},
		string(codec.EncodeBytesExt([]byte{}, []byte("bbh"), false)): {},
		string(codec.EncodeBytesExt([]byte{}, []byte("cca"), false)): {status: []pdpb.OperatorStatus{pdpb.OperatorStatus_CANCEL, pdpb.OperatorStatus_RUNNING, pdpb.OperatorStatus_SUCCESS}},
	}
	// after test done, the `leftScatterCount` should be empty
	leftScatterCount := map[string]int{
		string(codec.EncodeBytesExt([]byte{}, []byte("aay"), false)): 1,
		string(codec.EncodeBytesExt([]byte{}, []byte("bba"), false)): 1,
		string(codec.EncodeBytesExt([]byte{}, []byte("cca"), false)): 1,
	}
	client.injectInScatter = func(ri *split.RegionInfo) error {
		states, ok := results[string(ri.Region.StartKey)]
		require.True(t, ok)
		require.NotEqual(t, 0, len(states.status))
		require.NotEqual(t, pdpb.OperatorStatus_SUCCESS, states.status[states.index])
		states.index += 1
		cnt, ok := leftScatterCount[string(ri.Region.StartKey)]
		require.True(t, ok)
		if cnt == 1 {
			delete(leftScatterCount, string(ri.Region.StartKey))
		} else {
			leftScatterCount[string(ri.Region.StartKey)] = cnt - 1
		}
		return nil
	}
	regionsMap := client.GetAllRegions()
	leftOperatorCount := map[string]int{
		"": 1,
		string(codec.EncodeBytesExt([]byte{}, []byte("aay"), false)): 2,
		string(codec.EncodeBytesExt([]byte{}, []byte("bba"), false)): 3,
		string(codec.EncodeBytesExt([]byte{}, []byte("bbh"), false)): 1,
		string(codec.EncodeBytesExt([]byte{}, []byte("cca"), false)): 3,
	}
	client.injectInOperator = func(u uint64) (*pdpb.GetOperatorResponse, error) {
		ri := regionsMap[u]
		cnt, ok := leftOperatorCount[string(ri.Region.StartKey)]
		require.True(t, ok)
		if cnt == 1 {
			delete(leftOperatorCount, string(ri.Region.StartKey))
		} else {
			leftOperatorCount[string(ri.Region.StartKey)] = cnt - 1
		}
		states, ok := results[string(ri.Region.StartKey)]
		require.True(t, ok)
		if len(states.status) == 0 {
			return &pdpb.GetOperatorResponse{
				Desc: []byte("other"),
			}, nil
		}
		if states.status[states.index] == pdpb.OperatorStatus_RUNNING {
			states.index += 1
			return &pdpb.GetOperatorResponse{
				Desc:   []byte("scatter-region"),
				Status: states.status[states.index-1],
			}, nil
		}
		return &pdpb.GetOperatorResponse{
			Desc:   []byte("scatter-region"),
			Status: states.status[states.index],
		}, nil
	}

	// begin to test
	ctx := context.Background()
	regions := make([]*split.RegionInfo, 0, len(regionsMap))
	for _, info := range regionsMap {
		regions = append(regions, info)
	}
	regionSplitter := restore.NewRegionSplitter(client)
	leftCnt := regionSplitter.WaitForScatterRegions(ctx, regions, 2000*time.Second)
	require.Equal(t, leftCnt, 0)
}

func runTestSplitAndScatterWith(t *testing.T, client *TestClient) {
	ranges := initRanges()
	rewriteRules := initRewriteRules()
	regionSplitter := restore.NewRegionSplitter(client)

	ctx := context.Background()
	err := regionSplitter.Split(ctx, ranges, rewriteRules, false, func(key [][]byte) {})
	require.NoError(t, err)
	regions := client.GetAllRegions()
	if !validateRegions(regions) {
		for _, region := range regions {
			t.Logf("region: %v\n", region.Region)
		}
		t.Log("get wrong result")
		t.Fail()
	}
	regionInfos := make([]*split.RegionInfo, 0, len(regions))
	for _, info := range regions {
		regionInfos = append(regionInfos, info)
	}
	scattered := map[uint64]bool{}
	const alwaysFailedRegionID = 1
	client.injectInScatter = func(regionInfo *split.RegionInfo) error {
		if _, ok := scattered[regionInfo.Region.Id]; !ok || regionInfo.Region.Id == alwaysFailedRegionID {
			scattered[regionInfo.Region.Id] = false
			return status.Errorf(codes.Unknown, "region %d is not fully replicated", regionInfo.Region.Id)
		}
		scattered[regionInfo.Region.Id] = true
		return nil
	}
	regionSplitter.ScatterRegions(ctx, regionInfos)
	for key := range regions {
		if key == alwaysFailedRegionID {
			require.Falsef(t, scattered[key], "always failed region %d was scattered successfully", key)
		} else if !scattered[key] {
			t.Fatalf("region %d has not been scattered: %#v", key, regions[key])
		}
	}
}

func TestRawSplit(t *testing.T) {
	// Fix issue #36490.
	ranges := []rtree.Range{
		{
			StartKey: []byte{0},
			EndKey:   []byte{},
		},
	}
	client := initTestClient(true)
	ctx := context.Background()

	regionSplitter := restore.NewRegionSplitter(client)
	err := regionSplitter.Split(ctx, ranges, nil, true, func(key [][]byte) {})
	require.NoError(t, err)
	regions := client.GetAllRegions()
	expectedKeys := []string{"", "aay", "bba", "bbh", "cca", ""}
	if !validateRegionsExt(regions, expectedKeys, true) {
		for _, region := range regions {
			t.Logf("region: %v\n", region.Region)
		}
		t.Log("get wrong result")
		t.Fail()
	}
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
func initTestClient(isRawKv bool) *TestClient {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	keys := [6]string{"", "aay", "bba", "bbh", "cca", ""}
	regions := make(map[uint64]*split.RegionInfo)
	for i := uint64(1); i < 6; i++ {
		startKey := []byte(keys[i-1])
		if len(startKey) != 0 {
			startKey = codec.EncodeBytesExt([]byte{}, startKey, isRawKv)
		}
		endKey := []byte(keys[i])
		if len(endKey) != 0 {
			endKey = codec.EncodeBytesExt([]byte{}, endKey, isRawKv)
		}
		regions[i] = &split.RegionInfo{
			Leader: &metapb.Peer{
				Id: i,
			},
			Region: &metapb.Region{
				Id:       i,
				Peers:    peers,
				StartKey: startKey,
				EndKey:   endKey,
			},
		}
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	return NewTestClient(stores, regions, 6)
}

// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
func initRanges() []rtree.Range {
	var ranges [4]rtree.Range
	ranges[0] = rtree.Range{
		StartKey: []byte("aaa"),
		EndKey:   []byte("aae"),
	}
	ranges[1] = rtree.Range{
		StartKey: []byte("aae"),
		EndKey:   []byte("aaz"),
	}
	ranges[2] = rtree.Range{
		StartKey: []byte("ccd"),
		EndKey:   []byte("ccf"),
	}
	ranges[3] = rtree.Range{
		StartKey: []byte("ccf"),
		EndKey:   []byte("ccj"),
	}
	return ranges[:]
}

func initRewriteRules() *restore.RewriteRules {
	var rules [2]*import_sstpb.RewriteRule
	rules[0] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("aa"),
		NewKeyPrefix: []byte("xx"),
	}
	rules[1] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("cc"),
		NewKeyPrefix: []byte("bb"),
	}
	return &restore.RewriteRules{
		Data: rules[:],
	}
}

// expected regions after split:
//
//	[, aay), [aay, bba), [bba, bbf), [bbf, bbh), [bbh, bbj),
//	[bbj, cca), [cca, xxe), [xxe, xxz), [xxz, )
func validateRegions(regions map[uint64]*split.RegionInfo) bool {
	keys := [...]string{"", "aay", "bba", "bbf", "bbh", "bbj", "cca", "xxe", "xxz", ""}
	return validateRegionsExt(regions, keys[:], false)
}

func validateRegionsExt(regions map[uint64]*split.RegionInfo, expectedKeys []string, isRawKv bool) bool {
	if len(regions) != len(expectedKeys)-1 {
		return false
	}
FindRegion:
	for i := 1; i < len(expectedKeys); i++ {
		for _, region := range regions {
			startKey := []byte(expectedKeys[i-1])
			if len(startKey) != 0 {
				startKey = codec.EncodeBytesExt([]byte{}, startKey, isRawKv)
			}
			endKey := []byte(expectedKeys[i])
			if len(endKey) != 0 {
				endKey = codec.EncodeBytesExt([]byte{}, endKey, isRawKv)
			}
			if bytes.Equal(region.Region.GetStartKey(), startKey) &&
				bytes.Equal(region.Region.GetEndKey(), endKey) {
				continue FindRegion
			}
		}
		return false
	}
	return true
}

func TestNeedSplit(t *testing.T) {
	testNeedSplit(t, false)
	testNeedSplit(t, true)
}

func testNeedSplit(t *testing.T, isRawKv bool) {
	regions := []*split.RegionInfo{
		{
			Region: &metapb.Region{
				StartKey: codec.EncodeBytesExt(nil, []byte("b"), isRawKv),
				EndKey:   codec.EncodeBytesExt(nil, []byte("d"), isRawKv),
			},
		},
	}
	// Out of region
	require.Nil(t, restore.NeedSplit([]byte("a"), regions, isRawKv))
	// Region start key
	require.Nil(t, restore.NeedSplit([]byte("b"), regions, isRawKv))
	// In region
	region := restore.NeedSplit([]byte("c"), regions, isRawKv)
	require.Equal(t, 0, bytes.Compare(region.Region.GetStartKey(), codec.EncodeBytesExt(nil, []byte("b"), isRawKv)))
	require.Equal(t, 0, bytes.Compare(region.Region.GetEndKey(), codec.EncodeBytesExt(nil, []byte("d"), isRawKv)))
	// Region end key
	require.Nil(t, restore.NeedSplit([]byte("d"), regions, isRawKv))
	// Out of region
	require.Nil(t, restore.NeedSplit([]byte("e"), regions, isRawKv))
}

func TestRegionConsistency(t *testing.T) {
	cases := []struct {
		startKey []byte
		endKey   []byte
		err      string
		regions  []*split.RegionInfo
	}{
		{
			codec.EncodeBytes([]byte{}, []byte("a")),
			codec.EncodeBytes([]byte{}, []byte("a")),
			"scan region return empty result, startKey: (.*?), endKey: (.*?)",
			[]*split.RegionInfo{},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("a")),
			codec.EncodeBytes([]byte{}, []byte("a")),
			"first region's startKey > startKey, startKey: (.*?), regionStartKey: (.*?)",
			[]*split.RegionInfo{
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
			},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("b")),
			codec.EncodeBytes([]byte{}, []byte("e")),
			"last region's endKey < endKey, endKey: (.*?), regionEndKey: (.*?)",
			[]*split.RegionInfo{
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
			},
		},
		{
			codec.EncodeBytes([]byte{}, []byte("c")),
			codec.EncodeBytes([]byte{}, []byte("e")),
			"region endKey not equal to next region startKey(.*?)",
			[]*split.RegionInfo{
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("b")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("d")),
					},
				},
				{
					Region: &metapb.Region{
						StartKey: codec.EncodeBytes([]byte{}, []byte("e")),
						EndKey:   codec.EncodeBytes([]byte{}, []byte("f")),
					},
				},
			},
		},
	}
	for _, ca := range cases {
		err := split.CheckRegionConsistency(ca.startKey, ca.endKey, ca.regions)
		require.Error(t, err)
		require.Regexp(t, ca.err, err.Error())
	}
}

type fakeRestorer struct {
	mu sync.Mutex

	errorInSplit  bool
	splitRanges   []rtree.Range
	restoredFiles []*backuppb.File
}

func (f *fakeRestorer) SplitRanges(ctx context.Context, ranges []rtree.Range, rewriteRules *restore.RewriteRules, updateCh glue.Progress, isRawKv bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	f.splitRanges = append(f.splitRanges, ranges...)
	if f.errorInSplit {
		err := errors.Annotatef(berrors.ErrRestoreSplitFailed,
			"the key space takes many efforts and finally get together, how dare you split them again... :<")
		log.Error("error happens :3", logutil.ShortError(err))
		return err
	}
	return nil
}

func (f *fakeRestorer) RestoreSSTFiles(ctx context.Context, files []*backuppb.File, rewriteRules *restore.RewriteRules, updateCh glue.Progress) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	f.restoredFiles = append(f.restoredFiles, files...)
	err := errors.Annotatef(berrors.ErrRestoreWriteAndIngest, "the files to restore are taken by a hijacker, meow :3")
	log.Error("error happens :3", logutil.ShortError(err))
	return err
}

func fakeRanges(keys ...string) (r restore.DrainResult) {
	for i := range keys {
		if i+1 == len(keys) {
			return
		}
		r.Ranges = append(r.Ranges, rtree.Range{
			StartKey: []byte(keys[i]),
			EndKey:   []byte(keys[i+1]),
			Files:    []*backuppb.File{{Name: "fake.sst"}},
		})
	}
	return
}

type errorInTimeSink struct {
	ctx   context.Context
	errCh chan error
	t     *testing.T
}

func (e errorInTimeSink) EmitTables(tables ...restore.CreatedTable) {}

func (e errorInTimeSink) EmitError(err error) {
	e.errCh <- err
}

func (e errorInTimeSink) Close() {}

func (e errorInTimeSink) Wait() {
	select {
	case <-e.ctx.Done():
		e.t.Logf("The context is canceled but no error happen")
		e.t.FailNow()
	case <-e.errCh:
	}
}

func assertErrorEmitInTime(ctx context.Context, t *testing.T) errorInTimeSink {
	errCh := make(chan error, 1)
	return errorInTimeSink{
		ctx:   ctx,
		errCh: errCh,
		t:     t,
	}
}

func TestRestoreFailed(t *testing.T) {
	ranges := []restore.DrainResult{
		fakeRanges("aax", "abx", "abz"),
		fakeRanges("abz", "bbz", "bcy"),
		fakeRanges("bcy", "cad", "xxy"),
	}
	r := &fakeRestorer{}
	sender, err := restore.NewTiKVSender(context.TODO(), r, nil, 1)
	require.NoError(t, err)
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sink := assertErrorEmitInTime(dctx, t)
	sender.PutSink(sink)
	for _, r := range ranges {
		sender.RestoreBatch(r)
	}
	sink.Wait()
	sink.Close()
	sender.Close()
	require.GreaterOrEqual(t, len(r.restoredFiles), 1)
}

func TestSplitFailed(t *testing.T) {
	ranges := []restore.DrainResult{
		fakeRanges("aax", "abx", "abz"),
		fakeRanges("abz", "bbz", "bcy"),
		fakeRanges("bcy", "cad", "xxy"),
	}
	r := &fakeRestorer{errorInSplit: true}
	sender, err := restore.NewTiKVSender(context.TODO(), r, nil, 1)
	require.NoError(t, err)
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sink := assertErrorEmitInTime(dctx, t)
	sender.PutSink(sink)
	for _, r := range ranges {
		sender.RestoreBatch(r)
	}
	sink.Wait()
	sender.Close()
	require.GreaterOrEqual(t, len(r.splitRanges), 2)
	require.Len(t, r.restoredFiles, 0)
}
