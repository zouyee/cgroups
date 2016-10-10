package cgroups

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func NewBlkio(root string) *Blkio {
	return &Blkio{
		root: filepath.Join(root, "blkio"),
	}
}

type Blkio struct {
	root string
}

func (b *Blkio) Path(path string) string {
	return filepath.Join(b.root, path)
}

func (b *Blkio) Create(path string, resources *specs.Resources) error {
	if err := os.MkdirAll(b.Path(path), defaultDirPerm); err != nil {
		return err
	}
	if resources.BlockIO == nil {
		return nil
	}
	for _, t := range createBlkioSettings(resources.BlockIO) {
		if t.value != nil {
			if err := ioutil.WriteFile(
				filepath.Join(b.Path(path), fmt.Sprintf("blkio.%s", t.name)),
				t.format(t.value),
				0,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Blkio) Update(path string, resources *specs.Resources) error {
	return b.Create(path, resources)
}

func (b *Blkio) Stat(path string, stats *Stats) error {
	stats.Blkio = &BlkioStat{}
	settings := []blkioStatSettings{
		{
			name:  "throttle.io_serviced",
			entry: stats.Blkio.IoServicedRecursive,
		},
		{
			name:  "throttle.io_service_bytes",
			entry: stats.Blkio.IoServiceBytesRecursive,
		},
	}
	// Try to read CFQ stats available on all CFQ enabled kernels first
	if _, err := os.Lstat(filepath.Join(b.Path(path), fmt.Sprintf("blkio.io_serviced_recursive"))); err == nil {
		settings = append(settings,
			blkioStatSettings{
				name:  "sectors_recursive",
				entry: stats.Blkio.SectorsRecursive,
			},
			blkioStatSettings{
				name:  "io_service_bytes_recursive",
				entry: stats.Blkio.IoServiceBytesRecursive,
			},
			blkioStatSettings{
				name:  "io_serviced_recursive",
				entry: stats.Blkio.IoServicedRecursive,
			},
			blkioStatSettings{
				name:  "io_queue_recursive",
				entry: stats.Blkio.IoQueuedRecursive,
			},
			blkioStatSettings{
				name:  "io_service_time_recursive",
				entry: stats.Blkio.IoServiceTimeRecursive,
			},
			blkioStatSettings{
				name:  "io_wait_time_recursive",
				entry: stats.Blkio.IoWaitTimeRecursive,
			},
			blkioStatSettings{
				name:  "io_merged_recursive",
				entry: stats.Blkio.IoMergedRecursive,
			},
			blkioStatSettings{
				name:  "time_recursive",
				entry: stats.Blkio.IoTimeRecursive,
			},
		)
	}
	for _, t := range settings {
		if err := b.readEntry(path, t.name, t.entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *Blkio) readEntry(path, name string, entry []BlkioEntry) error {
	f, err := os.Open(filepath.Join(b.Path(path), fmt.Sprintf("blkio.%s", name)))
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if err := sc.Err(); err != nil {
			return err
		}
		// format: dev type amount
		fields := strings.FieldsFunc(sc.Text(), splitBlkioStatLine)
		if len(fields) < 3 {
			if len(fields) == 2 && fields[0] == "Total" {
				// skip total line
				continue
			} else {
				return fmt.Errorf("Invalid line found while parsing %s: %s", path, sc.Text())
			}
		}
		major, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return err
		}
		minor, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return err
		}
		op := ""
		valueField := 2
		if len(fields) == 4 {
			op = fields[2]
			valueField = 3
		}
		v, err := strconv.ParseUint(fields[valueField], 10, 64)
		if err != nil {
			return err
		}
		entry = append(entry, BlkioEntry{
			Major: major,
			Minor: minor,
			Op:    op,
			Value: v,
		})
	}
	return nil
}
func createBlkioSettings(blkio *specs.BlockIO) []blkioSettings {
	settings := []blkioSettings{
		{
			name:   "weight",
			value:  blkio.Weight,
			format: uintf,
		},
		{
			name:   "leaf_weight",
			value:  blkio.LeafWeight,
			format: uintf,
		},
	}
	for _, wd := range blkio.WeightDevice {
		settings = append(settings,
			blkioSettings{
				name:   "weight_device",
				value:  wd,
				format: weightdev,
			},
			blkioSettings{
				name:   "leaf_weight_device",
				value:  wd,
				format: weightleafdev,
			})
	}
	for _, t := range []struct {
		name string
		list []specs.ThrottleDevice
	}{
		{
			name: "throttle.read_bps_device",
			list: blkio.ThrottleReadBpsDevice,
		},
		{
			name: "throttle.read_iops_device",
			list: blkio.ThrottleReadIOPSDevice,
		},
		{
			name: "throttle.write_bps_device",
			list: blkio.ThrottleWriteBpsDevice,
		},
		{
			name: "throttle.write_iops_device",
			list: blkio.ThrottleWriteIOPSDevice,
		},
	} {
		for _, td := range t.list {
			settings = append(settings, blkioSettings{
				name:   t.name,
				value:  td,
				format: throttleddev,
			})
		}
	}
	return settings
}

type blkioSettings struct {
	name   string
	value  interface{}
	format func(v interface{}) []byte
}

type blkioStatSettings struct {
	name  string
	entry []BlkioEntry
}

func uintf(v interface{}) []byte {
	return []byte(strconv.FormatUint(uint64(*v.(*uint16)), 10))
}

func weightdev(v interface{}) []byte {
	wd := v.(specs.WeightDevice)
	return []byte(fmt.Sprintf("%d:%d %d", wd.Major, wd.Minor, wd.Weight))
}

func weightleafdev(v interface{}) []byte {
	wd := v.(specs.WeightDevice)
	return []byte(fmt.Sprintf("%d:%d %d", wd.Major, wd.Minor, wd.LeafWeight))
}

func throttleddev(v interface{}) []byte {
	td := v.(specs.ThrottleDevice)
	return []byte(fmt.Sprintf("%d:%d %d", td.Major, td.Minor, td.Rate))
}

func splitBlkioStatLine(r rune) bool {
	return r == ' ' || r == ':'
}
