package concurrent

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	cmap := NewMap[string]()

	assert.False(t, cmap.Exist("key"))

	cmap.Set("key", "hello")

	assert.True(t, cmap.Exist("key"))

	val := cmap.Get("key")
	assert.NotNil(t, val)
	assert.Equal(t, "hello", *val)

	cmap.Set("key", "world")

	val = cmap.Get("key")
	assert.NotNil(t, val)
	assert.Equal(t, "world", *val)

	cmap.Remove("key")

	assert.False(t, cmap.Exist("key"))

	val = cmap.Get("key")
	assert.Nil(t, val)
}

func TestConcurrency(t *testing.T) {
	mvmap := NewMap[string]()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			ver := int64(0)
			for j := 0; j < 10; j++ {
				mvmap.Set("key1", fmt.Sprint(rand.Int()))
				mvmap.Get("key1")
				ver += int64(rand.Int() % 100)

				mvmap.Set("key2", fmt.Sprint(rand.Int()))
				mvmap.Get("key1")
				ver += int64(rand.Int() % 100)

				mvmap.Set("key1", fmt.Sprint(rand.Int()))
				mvmap.Get("key1")
				ver += int64(rand.Int() % 100)

				mvmap.Set("key2", fmt.Sprint(rand.Int()))
				mvmap.Get("key1")
				ver += int64(rand.Int() % 100)
			}

			wg.Done()
		}()
	}
	wg.Wait()
}
