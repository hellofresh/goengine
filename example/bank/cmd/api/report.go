package main

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"go.uber.org/zap"
)

func bankReportsHandler(db *sql.DB, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.QueryContext(r.Context(), "SELECT r.name, r.amount FROM bank_reports AS r")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			logger.With(zap.Error(err)).Error("failed to query bank reports")
			return
		}

		reports := map[string]uint{}
		for rows.Next() {
			var (
				key    string
				amount uint
			)
			if err := rows.Scan(&key, &amount); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				logger.With(zap.Error(err)).Error("failed to scan bank reports")
				return
			}
			reports[key] = amount
		}

		body, err := json.Marshal(reports)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			logger.With(zap.Error(err)).Error("failed to marshal bank reports")
			return
		}

		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err = w.Write(body); err != nil {
			logger.With(zap.Error(err)).Error("failed to write response")
		}
	}
}
