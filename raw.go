package tikv

import (
	"strings"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/codec"
)

// Codec encode bytes to be sortable
type Codec interface {
	EncodeBytes(b []byte, data []byte) []byte
	DecodeBytes(b []byte, buf []byte) ([]byte, []byte, error)
}

// RawTiKVClient is the client of TiKV raw API
type RawTiKVClient struct {
	raw   *tikv.RawKVClient
	mvcc  bool
	codec Codec
}

// TiKVCodec implements interface Codec and encodes the key bytes the same as a txn client
type TiKVCodec struct{}

// EncodeBytes guarantees the encoded value is in ascending order for comparison,
// encoding with the following rule:
//  [group1][marker1]...[groupN][markerN]
//  group is 8 bytes slice which is padding with 0.
//  marker is `0xFF - padding 0 count`
// For example:
//   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
//   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
//   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
//   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
func (c *TiKVCodec) EncodeBytes(b []byte, data []byte) []byte {
	return codec.EncodeBytes(b, data)
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before,
// returns the leftover bytes and decoded value if no error.
// `buf` is used to buffer data to avoid the cost of makeslice in decodeBytes when DecodeBytes is called by Decoder.DecodeOne.
func (c *TiKVCodec) DecodeBytes(b []byte, buf []byte) ([]byte, []byte, error) {
	return codec.DecodeBytes(b, buf)
}

// TiKVOption supplies options when new a TiKV client
type TiKVOption func(c *RawTiKVClient)

// WithCodec set a codec to encode or decode keys
func WithCodec(codec Codec) TiKVOption {
	return func(c *RawTiKVClient) {
		c.codec = codec
	}
}

// NewRawTiKVClient creates a TiKV raw client
func NewRawTiKVClient(addrs string, options ...TiKVOption) (*RawTiKVClient, error) {
	c := &RawTiKVClient{}
	for _, o := range options {
		o(c)
	}
	raw, err := tikv.NewRawKVClient(strings.Split(addrs, ";"), config.Security{})
	if err != nil {
		return nil, err
	}
	c.raw = raw
	return c, nil
}

// Put a key value pair and return the version of the key
func (c *RawTiKVClient) Put(key, val []byte) error {
	if c.codec != nil {
		key = c.codec.EncodeBytes(nil, key)
	}
	return c.raw.Put(key, val)
}

// Get return the value of the key
func (c *RawTiKVClient) Get(key []byte) ([]byte, error) {
	if c.codec != nil {
		key = c.codec.EncodeBytes(nil, key)
	}
	return c.raw.Get(key)
}

// Delete a key
func (c *RawTiKVClient) Delete(key []byte) error {
	if c.codec != nil {
		key = c.codec.EncodeBytes(nil, key)
	}
	return c.raw.Delete(key)
}

// Close the client
func (c *RawTiKVClient) Close() error {
	return c.raw.Close()
}

// ClusterID returns the TiKV cluster ID.
func (c *RawTiKVClient) ClusterID() uint64 {
	return c.raw.ClusterID()
}

// BatchGet queries values with the keys.
func (c *RawTiKVClient) BatchGet(keys [][]byte) ([][]byte, error) {
	if c.codec != nil {
		encoded := make([][]byte, len(keys))
		for i := range keys {
			encoded[i] = c.codec.EncodeBytes(nil, keys[i])
		}
		return c.raw.BatchGet(encoded)
	}
	return c.raw.BatchGet(keys)
}

// BatchPut stores key-value pairs to TiKV.
func (c *RawTiKVClient) BatchPut(keys, values [][]byte) error {
	if c.codec != nil {
		encoded := make([][]byte, len(keys))
		for i := range keys {
			encoded[i] = c.codec.EncodeBytes(nil, keys[i])
		}
		return c.raw.BatchPut(encoded, values)
	}
	return c.raw.BatchPut(keys, values)
}

// BatchDelete deletes key-value pairs from TiKV
func (c *RawTiKVClient) BatchDelete(keys [][]byte) error {
	if c.codec != nil {
		encoded := make([][]byte, len(keys))
		for i := range keys {
			encoded[i] = c.codec.EncodeBytes(nil, keys[i])
		}
		return c.raw.BatchDelete(encoded)
	}
	return c.raw.BatchDelete(keys)
}

// Scan queries continuous kv pairs, starts from startKey, up to limit pairs.                                                                                                                        |   -clusterID : uint64
// If you want to exclude the startKey, append a '\0' to the key: `Scan(append(startKey, '\0'), limit)`.
func (c *RawTiKVClient) Scan(startKey []byte, limit int) ([][]byte, [][]byte, error) {
	encoded := c.codec.EncodeBytes(nil, startKey)
	keys, vals, err := c.raw.Scan(encoded, limit)
	if err != nil {
		return nil, nil, err
	}

	if c.codec != nil {
		decoded := make([][]byte, len(keys))
		for i := range keys {
			_, key, err := c.codec.DecodeBytes(keys[i], nil)
			if err != nil {
				return nil, nil, err
			}
			decoded[i] = key
		}
		return decoded, vals, nil
	}
	return keys, vals, nil
}
