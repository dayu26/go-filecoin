package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ipfs/go-ipfs-files"
	"github.com/pkg/errors"

	"github.com/filecoin-project/go-filecoin/address"
	"github.com/filecoin-project/go-filecoin/commands"
	"github.com/filecoin-project/go-filecoin/tools/fast"
	"github.com/filecoin-project/go-filecoin/tools/fast/series"
	lpfc "github.com/filecoin-project/go-filecoin/tools/iptb-plugins/filecoin/local"
	"github.com/filecoin-project/go-filecoin/types"
)

type PowerMinerConfig struct {
	CommonConfig
	FaucetURL        string
	AutoSealInterval int
	Collateral       int
	SectorSize       string
}

type PowerMinerProfile struct {
	config PowerMinerConfig
	runner FASTRunner
}

func NewPowerMinerProfile(configfile string) (Profile, error) {
	cf, err := os.Open(configfile)
	if err != nil {
		return nil, errors.Wrapf(err, "config file %s", configfile)
	}

	defer cf.Close()

	dec := json.NewDecoder(cf)

	var config PowerMinerConfig
	if err := dec.Decode(&config); err != nil {
		return nil, errors.Wrap(err, "config")
	}

	blocktime, err := time.ParseDuration(config.BlockTime)
	if err != nil {
		return nil, err
	}

	runner := FASTRunner{
		WorkingDir: config.WorkingDir,
		ProcessArgs: fast.FilecoinOpts{
			InitOpts: []fast.ProcessInitOption{
				fast.POGenesisFile(config.GenesisCarFile),
				NetworkPO(config.Network),
				fast.POPeerKeyFile(config.PeerkeyFile), // Needs to be last
			},
			DaemonOpts: []fast.ProcessDaemonOption{
				fast.POBlockTime(blocktime),
			},
		},
		PluginOptions: map[string]string{
			lpfc.AttrLogJSON:  config.LogJSON,
			lpfc.AttrLogLevel: config.LogLevel,
		},
	}

	return &PowerMinerProfile{config, runner}, nil
}

func (p *PowerMinerProfile) Pre() error {
	ctx := context.Background()

	node, err := GetNode(ctx, lpfc.PluginName, p.runner.WorkingDir, p.runner.PluginOptions, p.runner.ProcessArgs)
	if err != nil {
		return err
	}

	if _, err := os.Stat(p.runner.WorkingDir + "/repo"); os.IsNotExist(err) {
		if o, err := node.InitDaemon(ctx); err != nil {
			io.Copy(os.Stdout, o.Stdout())
			io.Copy(os.Stdout, o.Stderr())
			return err
		}
	} else if err != nil {
		return err
	}

	cfg, err := node.Config()
	if err != nil {
		return err
	}

	cfg.Observability.Metrics.PrometheusEnabled = true

	// IPTB changes this to loopback and a random port
	cfg.Swarm.Address = "/ip4/0.0.0.0/tcp/6000"

	if err := node.WriteConfig(cfg); err != nil {
		return err
	}

	return nil
}

func (p *PowerMinerProfile) Daemon() error {
	args := []string{}
	for _, argfn := range p.runner.ProcessArgs.DaemonOpts {
		args = append(args, argfn()...)
	}

	fmt.Println(strings.Join(args, " "))

	return nil
}

func (p *PowerMinerProfile) Post() error {
	ctx := context.Background()
	miner, err := GetNode(ctx, lpfc.PluginName, p.runner.WorkingDir, p.runner.PluginOptions, p.runner.ProcessArgs)
	if err != nil {
		return err
	}

	ctxWaitForAPI, cancel := context.WithTimeout(ctx, 10*time.Minute)
	if err := WaitForAPI(ctxWaitForAPI, miner); err != nil {
		return err
	}
	cancel()

	defer miner.DumpLastOutput(os.Stdout)

	var minerAddress address.Address
	if err := miner.ConfigGet(ctx, "mining.minerAddress", &minerAddress); err != nil {
		return err
	}

	// If the miner address is set then we are restarting
	if minerAddress == address.Undef {
		if err := FaucetRequest(ctx, miner, p.config.FaucetURL); err != nil {
			return err
		}

		collateral := big.NewInt(int64(p.config.Collateral))
		sectorSize, ok := types.NewBytesAmountFromString(p.config.SectorSize, 10)
		if !ok {
			return fmt.Errorf("Failed to parse sector size %s", p.config.SectorSize)
		}

		_, err := miner.MinerCreate(ctx, collateral, fast.AOSectorSize(sectorSize), fast.AOPrice(big.NewFloat(0.0001)), fast.AOLimit(300))
		if err != nil {
			return err
		}

		if err := miner.MiningStart(ctx); err != nil {
			return err
		}
	} else {
		if err := miner.MiningStart(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *PowerMinerProfile) Main() error {
	ctx := context.Background()
	miner, err := GetNode(ctx, lpfc.PluginName, p.runner.WorkingDir, p.runner.PluginOptions, p.runner.ProcessArgs)
	if err != nil {
		return err
	}

	ctxWaitForAPI, cancel := context.WithTimeout(ctx, 10*time.Minute)
	if err := WaitForAPI(ctxWaitForAPI, miner); err != nil {
		return err
	}
	cancel()

	defer miner.DumpLastOutput(os.Stdout)

	sectorSize, ok := types.NewBytesAmountFromString(p.config.SectorSize, 10)
	if !ok {
		return fmt.Errorf("Failed to parse sector size %s", p.config.SectorSize)
	}

	pparams, err := miner.Protocol(ctx)
	if err != nil {
		return err
	}

	maxPieceSize := types.NewBytesAmount(0)
	for _, info := range pparams.SupportedSectors {
		if sectorSize.Equal(info.Size) {
			maxPieceSize = info.MaxPieceSize
			break
		}
	}

	if maxPieceSize.IsZero() {
		return fmt.Errorf("Could not find max piece size")
	}

	miningStatus, err := miner.MiningStatus(ctx)
	if err != nil {
		return err
	}

	minerAddress := miningStatus.Miner
	minerOwner := miningStatus.Owner

	for {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-series.CtxSleepDelay(ctx):
				fmt.Println("Checking outbox...")
				var outbox commands.OutboxLsResult
				// Wait for message to show up in outbox
				err := miner.RunCmdJSONWithStdin(ctx, nil, &outbox, "go-filecoin", "outbox", "ls", minerOwner.String())
				if err != nil {
					fmt.Printf("ERROR: %s", err)
					time.Sleep(time.Minute)
					continue
				}

				if len(outbox.Messages) == 0 {
					break
				}

				foundCommitment := false
				for _, msg := range outbox.Messages {
					if msg.Msg.To == minerAddress && msg.Msg.Method == "commitSector" {
						foundCommitment = true
					}
				}

				if !foundCommitment {
					break
				}
			}
		}

		dataReader := io.LimitReader(rand.Reader, int64(maxPieceSize.Uint64()))
		_, err = miner.AddPiece(ctx, files.NewReaderFile(dataReader))
		if err != nil {
			fmt.Printf("ERROR: %s", err)
			time.Sleep(time.Minute)
			continue
		}

		if err := miner.SealNow(ctx); err != nil {
			fmt.Printf("ERROR: %s", err)
			time.Sleep(time.Minute)
			continue
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-series.CtxSleepDelay(ctx):
				fmt.Println("Checking outbox...")

				var outbox commands.OutboxLsResult
				// Wait for message to show up in outbox
				err := miner.RunCmdJSONWithStdin(ctx, nil, &outbox, "go-filecoin", "outbox", "ls", minerOwner.String())
				if err != nil {
					fmt.Printf("ERROR: %s", err)
					time.Sleep(time.Minute)
					continue
				}

				foundCommitment := false
				for _, msg := range outbox.Messages {
					if msg.Msg.To == minerAddress && msg.Msg.Method == "commitSector" {
						foundCommitment = true
					}
				}

				if foundCommitment {
					break
				}
			}
		}
	}

	return nil
}
