package cete

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestCounting(t *testing.T) {
	if testing.Short() {
		t.Parallel()
	}

	dir, err := ioutil.TempDir("", "cete_")
	panicNotNil(err)

	t.Log("testing directory:", dir)
	defer func() {
		if !t.Failed() {
			os.RemoveAll(dir)
		}
	}()

	db, err := Open(dir + "/data")
	panicNotNil(err)

	defer db.Close()

	err = db.NewTable("count_testing")
	panicNotNil(err)

	err = db.Table("count_testing").NewIndex("Age")
	panicNotNil(err)

	for i := 1; i <= 1000; i++ {
		panicNotNil(db.Table("count_testing").Set(paddedItoa(i), Person{Age: i}))
	}

	if db.Table("count_testing").CountBetween(MinValue, "0010") != 10 {
		t.Fatal("count should be 10, but isn't")
	}

	if db.Table("count_testing").CountBetween(MinValue, "0100") != 100 {
		t.Fatal("count should be 100, but isn't")
	}

	if db.Table("count_testing").CountBetween("0901", MaxValue) != 100 {
		t.Fatal("count should be 100, but isn't")
	}

	if db.Table("count_testing").CountBetween("0101", "0200") != 100 {
		t.Fatal("count should be 100, but isn't")
	}

	if db.Table("count_testing").CountBetween("0100", "0010") != 0 {
		t.Fatal("count should be 0, but isn't")
	}

	if db.Table("count_testing").CountBetween(MinValue, MaxValue) != 1000 {
		t.Fatal("count should be 1000, but isn't")
	}

	if db.Table("count_testing").CountBetween(MaxValue, MinValue) != 0 {
		t.Fatal("count should be 0, but isn't")
	}

	if db.Table("count_testing").CountBetween(MinValue, MinValue) != 0 {
		t.Fatal("count should be 0, but isn't")
	}

	if db.Table("count_testing").CountBetween(MaxValue, MaxValue) != 0 {
		t.Fatal("count should be 0, but isn't")
	}

	if db.Table("count_testing").CountBetween("99999999", MaxValue) != 0 {
		t.Fatal("count should be 0, but isn't")
	}

	if db.Table("count_testing").Index("Age").CountBetween(MinValue, 10) != 10 {
		t.Fatal("count should be 10, but isn't")
	}

	if db.Table("count_testing").Index("Age").CountBetween(MinValue, 100) != 100 {
		t.Fatal("count should be 100, but isn't")
	}

	if db.Table("count_testing").Index("Age").CountBetween(901, MaxValue) != 100 {
		t.Fatal("count should be 100, but isn't")
	}

	if db.Table("count_testing").Index("Age").CountBetween(101, 200) != 100 {
		t.Fatal("count should be 100, but isn't")
	}

	if db.Table("count_testing").Index("Age").CountBetween(100, 10) != 0 {
		t.Fatal("count should be 0, but isn't")
	}

	if db.Table("count_testing").Index("Age").CountBetween(MinValue, MaxValue) != 1000 {
		t.Fatal("count should be 1000, but isn't")
	}

	if db.Table("count_testing").Index("Age").CountBetween(MaxValue, MinValue) != 0 {
		t.Fatal("count should be 0, but isn't")
	}

	if db.Table("count_testing").Index("Age").CountBetween(MinValue, MinValue) != 0 {
		t.Fatal("count should be 0, but isn't")
	}

	if db.Table("count_testing").Index("Age").CountBetween(MaxValue, MaxValue) != 0 {
		t.Fatal("count should be 0, but isn't")
	}

	if db.Table("count_testing").Index("Age").CountBetween(10000, MaxValue) != 0 {
		t.Fatal("count should be 0, but isn't")
	}
}

func paddedItoa(number int) string {
	num := strconv.Itoa(number)
	return strings.Repeat("0", 4-len(num)) + num
}
