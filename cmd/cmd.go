package cmd

import (
	"github.com/urfave/cli/v2"
)

func AppCommandLineInterface() *cli.App {
	appCli := cli.NewApp()
	//appCli.Action = ServerRun
	appCli.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "config",
			Aliases:     []string{"c"},
			DefaultText: "./config/local.yaml",
			Value:       "./config/local.yaml",
			Usage:       "Load configuration from `FILE`",
			Required:    false,
		},
	}
	appCli.Commands = []*cli.Command{
		{
			Name:   "kafka_consumer",
			Usage:  "run kafka consumer",
			Action: startKafkaConsumer,
			Flags:  []cli.Flag{},
		},
		{
			Name:   "temporal_worker",
			Usage:  "run temporal worker",
			Action: startTemporalWorker,
			Flags:  []cli.Flag{},
		},
		{
			Name:   "temporal_cron_worker",
			Usage:  "run temporal cron redrive worker",
			Action: StartTemporalCronWorker,
			Flags:  []cli.Flag{},
		},
	}
	return appCli
}
