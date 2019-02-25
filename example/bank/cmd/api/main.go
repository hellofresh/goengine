package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/hellofresh/goengine/example/bank/config"
	"github.com/hellofresh/goengine/example/bank/domain"
	goengineZap "github.com/hellofresh/goengine/extension/zap"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	failOnError(err)

	db, dbCloser, err := config.NewPostgresDB(logger)
	failOnError(err)
	defer dbCloser()

	manager, err := config.NewGoEngineManager(db, goengineZap.Wrap(logger))
	failOnError(err)
	failOnError(config.SetupDB(manager, db, logger))

	eventStore, err := manager.NewEventStore()
	failOnError(err)

	bankAccountRepo, err := domain.NewBankAccountRepository(eventStore, config.EventStoreStreamName)
	failOnError(err)

	router := chi.NewRouter()
	router.Get("/", listRoutes(router, logger))
	router.Get("/report/", bankReportsHandler(db, logger))
	router.Mount("/debug", middleware.Profiler())
	router.Mount("/account", newAccountHandler(db, bankAccountRepo, logger).HTTPHandler())

	serve(router, logger)
}

func serve(router http.Handler, logger *zap.Logger) {
	srv := &http.Server{Addr: ":8080", Handler: router}

	closed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint

		// We received an interrupt signal, shut down.
		if err := srv.Shutdown(context.Background()); err != nil {
			logger.With(zap.Error(err)).Error("failed to shutdown http server")
		}
		close(closed)
	}()

	logger.With(zap.String("addr", srv.Addr)).Info("Starting http.ListenAndServe")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.With(zap.Error(err)).Error("http.ListenAndServe return an error")
	}

	<-closed
}

func listRoutes(router chi.Router, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var res []string
		var appendRoutes func(prefix string, routes []chi.Route)
		appendRoutes = func(prefix string, routes []chi.Route) {
			for _, route := range routes {
				if route.SubRoutes == nil || len(route.SubRoutes.Routes()) == 0 {
					res = append(res, prefix+route.Pattern)
					continue
				}

				appendRoutes(prefix+route.Pattern[:len(route.Pattern)-2], route.SubRoutes.Routes())
			}
		}
		appendRoutes("", router.Routes())

		body, err := json.Marshal(res)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			logger.With(zap.Error(err)).Error("failed to marshal routes")
			return
		}

		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err = w.Write(body); err != nil {
			logger.With(zap.Error(err)).Error("failed to write response")
		}
	}
}

func failOnError(err error) {
	if err != nil {
		panic(err)
	}
}
