package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"cherry-picker/internal/app"
	"cherry-picker/internal/config"
)

func main() {
	logger := log.New(os.Stdout, "cherry-picker ", log.LstdFlags|log.Lmicroseconds)

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal(err)
	}
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
