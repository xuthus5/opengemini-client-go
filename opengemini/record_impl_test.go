package opengemini

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewRPCClient(t *testing.T) {
	c := newRecordWriterClient(nil)
	ctx := context.Background()
	db := "db0"
	rp := "autogen"
	mst := "m0"
	err := c.WritePoint(ctx, db, rp, &Point{
		Measurement: mst,
		Time:        time.Now(),
		Tags: map[string]string{
			"t1": "t1",
			"t2": "t2",
		},
		Fields: map[string]interface{}{
			"i":  100,
			"b":  true,
			"f":  3.14,
			"s1": "pi1",
			"s2": "pi2",
			"s3": "pi3",
			"s4": "pi4",
		},
	})
	assert.Nil(t, err)
	err = c.Flush(ctx, db, rp)
	assert.Nil(t, err)

	time.Sleep(time.Second)
	err = c.WritePoint(ctx, db, rp, &Point{
		Measurement: mst,
		Time:        time.Now(),
		Tags: map[string]string{
			"a1": "a1",
			"a2": "a2",
		},
		Fields: map[string]interface{}{
			"mem": 3,
		},
	})
	assert.Nil(t, err)

	time.Sleep(time.Second)

	err = c.WritePoint(ctx, db, rp, &Point{
		Measurement: mst,
		Time:        time.Now(),
		Tags: map[string]string{
			"a1": "a1",
			"a2": "a2",
		},
		Fields: map[string]interface{}{
			"mem": 1,
		},
	})
	assert.Nil(t, err)

	time.Sleep(time.Second)

	err = c.WritePoint(ctx, db, rp, &Point{
		Measurement: mst,
		Time:        time.Now(),
		Tags: map[string]string{
			"a1": "a1",
			"a2": "a2",
		},
		Fields: map[string]interface{}{
			"mem": 2,
		},
	})
	assert.Nil(t, err)

	time.Sleep(time.Second)

	err = c.Flush(ctx, db, rp)
	assert.Nil(t, err)
}
