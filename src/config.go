package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type userConfig struct {
	FileRootPath string
	Oss          ossConfig
}

type ossConfig struct {
	OssKey     string
	OssSecret  string
	BucketName string
	APIPrefix  string
}

func checkConf(conf *userConfig) error {
	// fileRootPath
	stat, err := os.Stat(conf.FileRootPath)
	if err != nil {
		return errors.New("fileRootPath '" + conf.FileRootPath + "' is not available: " + err.Error())
	}
	if !stat.IsDir() {
		return errors.New("fileRootPath '" + conf.FileRootPath + "' is not a directory")
	}

	// oss
	if conf.Oss.OssKey == "" || conf.Oss.OssSecret == "" || conf.Oss.BucketName == "" || conf.Oss.APIPrefix == "" {
		return errors.New("oss config is invalid")
	}

	return nil
}

func getConfig(configFileName string) (config userConfig) {
	if configFileName == "" {
		configFileName = "config"
	}
	viper.SetConfigName(configFileName) // name of config file (without extension)
	viper.AddConfigPath(".")            // optionally look for config in the working directory

	// defaults
	viper.SetDefault("fileRootPath", "")
	viper.SetDefault("oss.ossKey", "")
	viper.SetDefault("oss.ossSecret", "")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// write a default file
			// and then still need to die..
			fmt.Println("Automatically creating a blank config file")

			if err := viper.WriteConfigAs("./config.yml"); err != nil {
				panic(err)
			} else {
				return getConfig(configFileName)
			}
		}
		panic(fmt.Errorf("Fatal error config file: \n%s", err))
	}

	// unmarshal to userConfig structure
	if err := viper.Unmarshal(&config); err != nil {
		panic(err)
	}

	// check config
	if err := checkConf(&config); err != nil {
		panic(err)
	}

	return
}
