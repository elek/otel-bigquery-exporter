package bigquery

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_safeTableName(t *testing.T) {
	require.Equal(t, "storj_io_storj_storagenode_hashstore_store_compact", safeTableName("storj.io/storj/storagenode/hashstore.(*Store).Compact"))
}
