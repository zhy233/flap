package broker

import (
	"fmt"
	"io/ioutil"
	"net"
	"strconv"

	"github.com/hashicorp/memberlist"
	"github.com/imdevlab/flap/pkg/config"
)

func (b *Broker) joinCluster() error {
	conf := memberlist.DefaultLocalConfig()
	laddr, lportS, err := net.SplitHostPort(config.Conf.Cluster.Listen)
	if err != nil {
		return err
	}
	lport, _ := strconv.Atoi(lportS)
	conf.BindAddr = laddr
	conf.BindPort = lport
	conf.AdvertiseAddr = laddr
	conf.AdvertisePort = lport
	conf.Name = config.Conf.Cluster.Listen
	conf.LogOutput = ioutil.Discard

	// conf.Logger = nil
	list, err := memberlist.Create(conf)
	if err != nil {
		return err
	}

	// list.SendBestEffort()
	_, err = list.Join(config.Conf.Cluster.Seeds)
	if err != nil {
		return err
	}

	// Ask for members of the cluster
	for _, member := range list.Members() {
		fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
	}

	return nil
}
