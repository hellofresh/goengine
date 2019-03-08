package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/example/bank/domain"
	"go.uber.org/zap"
)

type contextKey string

const contextKeyBankAccountID contextKey = "bankAccountID"

func newAccountHandler(db *sql.DB, repo *domain.BankAccountRepository, logger *zap.Logger) *accountHandler {
	return &accountHandler{db, repo, logger}
}

type accountHandler struct {
	db              *sql.DB
	bankAccountRepo *domain.BankAccountRepository
	logger          *zap.Logger
}

func (a *accountHandler) HTTPHandler() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.NoCache)

	r.Post("/", a.CreateAccount)

	r.Route("/{account_id}", func(r chi.Router) {
		r.Use(a.accountIDFromURL)

		r.Get("/", a.ViewAccount)
		r.Get("/average", a.AccountAverages)
		r.Post("/deposit", a.AccountDeposit)
		r.Post("/withdraw", a.AccountWithdraw)
	})

	return r
}

func (a *accountHandler) CreateAccount(w http.ResponseWriter, r *http.Request) {
	bankAccount, err := domain.OpenBankAccount()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		a.logger.With(zap.Error(err)).Error("failed to open a bank account")
		return
	}

	if err := a.bankAccountRepo.Save(r.Context(), bankAccount); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		a.logger.With(zap.Error(err)).Error("failed to save a opened bank account")
		return
	}

	w.Header().Add("Location", fmt.Sprintf("/account/%s", bankAccount.AggregateID()))
	w.WriteHeader(http.StatusCreated)
}

func (a *accountHandler) ViewAccount(w http.ResponseWriter, r *http.Request) {
	accountID := r.Context().Value(contextKeyBankAccountID).(aggregate.ID)
	bankAccount, err := a.bankAccountRepo.Get(r.Context(), accountID)
	if err != nil {
		a.renderAccountError(w, accountID, err)
		return
	}

	body := []byte(
		fmt.Sprintf(`{"version":%d}`, bankAccount.AggregateVersion()),
	)

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil {
		a.logger.With(zap.Error(err)).Error("failed to write response")
	}
}

func (a *accountHandler) AccountAverages(w http.ResponseWriter, r *http.Request) {
	var debit, credit uint
	accountID := r.Context().Value(contextKeyBankAccountID).(aggregate.ID)
	err := a.db.QueryRowContext(r.Context(),
		"SELECT debit, credit FROM account_averages WHERE accountid=$1",
		accountID,
	).Scan(&debit, &credit)
	if err != nil {
		if err != sql.ErrNoRows {
			w.WriteHeader(http.StatusNotFound)
			a.logger.With(zap.Error(err)).Debug("unknown accountID provided")
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
		a.logger.Error("failed to query account averages")
		return
	}
	body := []byte(
		fmt.Sprintf(`{"average_deposit":%d,"average_credit":%d}`, debit, credit),
	)

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err = w.Write(body); err != nil {
		a.logger.With(zap.Error(err)).Error("failed to write response")
	}
}

func (a *accountHandler) AccountDeposit(w http.ResponseWriter, r *http.Request) {
	amount, err := a.loadAmountFromPost(r)
	if err != nil {
		a.renderAmountError(w, err)
		return
	}

	accountID := r.Context().Value(contextKeyBankAccountID).(aggregate.ID)
	bankAccount, err := a.bankAccountRepo.Get(r.Context(), accountID)
	if err != nil {
		a.renderAccountError(w, accountID, err)
		return
	}

	if err := bankAccount.Deposit(amount); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		a.logger.With(
			zap.Error(err),
			zap.Uint("amount", amount),
			zap.String("account_id", string(bankAccount.AggregateID())),
		).Error("failed to deposit into bank account")
		return
	}

	if err := a.bankAccountRepo.Save(r.Context(), bankAccount); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		a.logger.With(
			zap.Error(err),
			zap.String("account_id", string(bankAccount.AggregateID())),
		).Error("failed to save a bank account deposit")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (a *accountHandler) AccountWithdraw(w http.ResponseWriter, r *http.Request) {
	amount, err := a.loadAmountFromPost(r)
	if err != nil {
		a.renderAmountError(w, err)
		return
	}

	accountID := r.Context().Value(contextKeyBankAccountID).(aggregate.ID)
	bankAccount, err := a.bankAccountRepo.Get(r.Context(), accountID)
	if err != nil {
		a.renderAccountError(w, accountID, err)
		return
	}

	if err := bankAccount.Deposit(amount); err != nil {
		log := a.logger.With(
			zap.Error(err),
			zap.Uint("amount", amount),
			zap.String("account_id", string(bankAccount.AggregateID())),
		)
		if err == domain.ErrInsufficientFunds {
			w.WriteHeader(http.StatusForbidden)
			log.Debug("unable to withdraw from bank account")
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
		log.Error("failed to withdraw from bank account")
		return
	}

	if err := a.bankAccountRepo.Save(r.Context(), bankAccount); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		a.logger.With(
			zap.Error(err),
			zap.Uint("amount", amount),
			zap.String("account_id", string(bankAccount.AggregateID())),
		).Error("failed to save a bank account withdraw")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (a *accountHandler) accountIDFromURL(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accountIDStr := chi.URLParam(r, "account_id")
		accountID, err := aggregate.IDFromString(accountIDStr)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			a.logger.With(
				zap.Error(err),
				zap.String("account_id", accountIDStr),
			).Error("invalid bank account id in url")
			return
		}

		ctx := context.WithValue(r.Context(), contextKeyBankAccountID, accountID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *accountHandler) loadAmountFromPost(r *http.Request) (uint, error) {
	amountStr := r.PostFormValue("amount")
	amount64, err := strconv.ParseUint(amountStr, 10, 32)
	if err != nil {
		return 0, err
	}

	return uint(amount64), nil
}

func (a *accountHandler) renderAmountError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	a.logger.With(
		zap.Error(err),
	).Info("invalid amount")
}

func (a *accountHandler) renderAccountError(w http.ResponseWriter, accountID aggregate.ID, err error) {
	switch err {
	case aggregate.ErrEmptyEventStream:
		w.WriteHeader(http.StatusNotFound)
		a.logger.With(zap.Error(err)).Debug("unknown accountID provided")
	default:
		w.WriteHeader(http.StatusInternalServerError)
		a.logger.With(
			zap.Error(err),
			zap.String("account_id", string(accountID)),
		).Error("failed to load bank account")
	}
}
