package service

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v1"
)

type Config struct {
	Common struct {
		Version  string
		IsDebug  bool `yaml:"debug"`
		LogPath  string
		LogLevel string
	}
	Broker struct {
		Host string
		Port string
	}

	Store struct {
		Engine string
	}

	Cluster struct {
		HwAddr    string
		Port      string
		SeedPeers []string
	}

	Admin struct {
		Addr string
	}
}

func initConfig() *Config {
	conf := &Config{}
	data, err := ioutil.ReadFile("broker.yaml")
	if err != nil {
		log.Fatal("read config error :", err)
	}

	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		log.Fatal("yaml decode error :", err)
	}

	return conf
}
