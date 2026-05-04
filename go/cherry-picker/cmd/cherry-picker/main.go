package main

import (
	"context"
	"log"
	"os"
	"os/signal"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application := app.New(cfg, logger)
	if err := application.Run(ctx); err != nil {
		logger.Fatal(err)
	}
}
