package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"cherry-picker/internal/app"
	"cherry-picker/internal/buildinfo"
	"cherry-picker/internal/config"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		_ = json.NewEncoder(os.Stdout).Encode(buildinfo.Current(""))
		return
	}
	logger := log.New(os.Stdout, "cherry-picker ", log.LstdFlags|log.Lmicroseconds)

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal(err)
	}
	configHash, err := buildinfo.Fingerprint(cfg)
	if err != nil {
		logger.Fatalf("fingerprint effective config: %v", err)
	}
	build := buildinfo.Current(configHash)
	logger.Printf("provenance: commit=%s dirty=%s source=%s config=%s build_time=%s go=%s",
		buildinfo.Short(build.GitCommit), build.GitDirty,
		buildinfo.Short(build.SourceHash), buildinfo.Short(build.ConfigHash),
		build.BuildTime, build.GoVersion)
	startPprof(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application := app.New(cfg, logger)
	if err := application.Run(ctx); err != nil {
		logger.Fatal(err)
	}
}

func startPprof(logger *log.Logger) {
	addr := strings.TrimSpace(os.Getenv("CHERRY_PICKER_PPROF_ADDR"))
	if addr == "" {
		return
	}
	go func() {
		logger.Printf("pprof: listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			logger.Printf("pprof: %v", err)
		}
	}()
}
