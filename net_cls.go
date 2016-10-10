package cgroups

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func NewNetCls(root string) *NetCls {
	return &NetCls{
		root: filepath.Join(root, "net_cls"),
	}
}

type NetCls struct {
	root string
}

func (n *NetCls) Path(path string) string {
	return filepath.Join(n.root, path)
}

func (n *NetCls) Create(path string, resources *specs.Resources) error {
	if err := os.MkdirAll(n.Path(path), defaultDirPerm); err != nil {
		return err
	}
	if resources.Network != nil && resources.Network.ClassID != nil && *resources.Network.ClassID > 0 {
		return ioutil.WriteFile(
			filepath.Join(n.Path(path), "net_cls_classid_u"),
			[]byte(strconv.FormatUint(uint64(*resources.Network.ClassID), 10)),
			0,
		)
	}
	return nil
}
