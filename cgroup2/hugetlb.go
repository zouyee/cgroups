/*
   Copyright The containerd Authors.

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

package cgroup2

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type HugeTlb []HugeTlbEntry

type HugeTlbEntry struct {
	HugePageSize string
	Limit        uint64
}

func (r *HugeTlb) Values() (o []Value) {
	for _, e := range *r {
		o = append(o, Value{
			filename:      strings.Join([]string{"hugetlb", e.HugePageSize, "max"}, "."),
			value:         e.Limit,
			writerHandler: writerHandlerRsvd,
		})
	}

	return o
}

func writerHandlerRsvd(path, fileName string, perm os.FileMode, data []byte) error {
	err := os.WriteFile(
		filepath.Join(path, fileName),
		data,
		perm,
	)
	if err != nil {
		return err
	}
	str := strings.Split(fileName, ".")
	if len(str) != 3 {
		return fmt.Errorf("invalid file: %s", fileName)
	}
	fileName = strings.Join([]string{str[0], str[1], "rsvd", str[2]}, ".")
	err = os.WriteFile(
		filepath.Join(path, fileName),
		data,
		perm,
	)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
