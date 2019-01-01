package tikv

import (
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/fperf/fperf"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

const seqPlaceHolder = "__seq_int__"
const randPlaceHolder = "__rand_int__"

var seq func() string = seqCreater(0)
var random func() string = randCreater(10000000000000000)

func seqCreater(begin int64) func() string {
	// filled map, filled generated to 16 bytes
	l := []string{
		"",
		"0",
		"00",
		"000",
		"0000",
		"00000",
		"000000",
		"0000000",
		"00000000",
		"000000000",
		"0000000000",
		"00000000000",
		"000000000000",
		"0000000000000",
		"00000000000000",
		"000000000000000",
	}
	v := begin
	m := &sync.Mutex{}
	return func() string {
		m.Lock()
		s := strconv.FormatInt(v, 10)
		v += 1
		m.Unlock()

		filled := len(l) - len(s)
		if filled <= 0 {
			return s
		}
		return l[filled] + s
	}
}

func randCreater(max int64) func() string {
	// filled map, filled generated to 16 bytes
	l := []string{
		"",
		"0",
		"00",
		"000",
		"0000",
		"00000",
		"000000",
		"0000000",
		"00000000",
		"000000000",
		"0000000000",
		"00000000000",
		"000000000000",
		"0000000000000",
		"00000000000000",
		"000000000000000",
	}
	return func() string {
		s := strconv.FormatInt(rand.Int63n(max), 10)
		filled := len(l) - len(s)
		if filled <= 0 {
			return s
		}
		return l[filled] + s
	}
}

func replaceSeq(s string) string {
	return strings.Replace(s, seqPlaceHolder, seq(), -1)
}
func replaceRand(s string) string {
	return strings.Replace(s, randPlaceHolder, random(), -1)
}

func replace(s string) string {
	if strings.Index(s, seqPlaceHolder) >= 0 {
		s = replaceSeq(s)
	}
	if strings.Index(s, randPlaceHolder) >= 0 {
		s = replaceRand(s)
	}
	return s
}

type options struct {
	raw     bool
	verbose bool
	limit   int
}

type Client struct {
	opt  options
	s    kv.Storage
	raw  *RawTiKVClient
	args []string
}

func New(flag *fperf.FlagSet) fperf.Client {
	c := &Client{}

	flag.BoolVar(&c.opt.raw, "raw", false, "using the raw client, notice that the raw client and txn client must not be used together")
	flag.IntVar(&c.opt.limit, "n", 0, "scan limit")
	flag.BoolVar(&c.opt.verbose, "v", false, "verbose output")
	flag.Parse()
	c.args = flag.Args()
	if len(c.args) == 0 {
		flag.Usage()
		os.Exit(0)
	}
	return c
}

func (c *Client) Dial(addr string) error {
	s, err := tikv.Driver{}.Open(addr)
	if err != nil {
		return err
	}
	raw, err := NewRawTiKVClient(addr, WithCodec(&TiKVCodec{}))
	if err != nil {
		return err
	}
	c.s = s
	c.raw = raw
	return nil
}

func (c *Client) Request() error {
	args := make([]string, len(c.args))
	for i := range c.args {
		args[i] = replace(c.args[i])
	}
	return Call(c, args)
}

//Register to fperf
func init() {
	//rand.Seed(time.Now().UnixNano())
	fperf.Register("tikv", New, "tikv performance benchmark")
}
