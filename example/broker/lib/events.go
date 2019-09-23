package lib

import "github.com/hellofresh/goengine/aggregate"


const (
	// BankAccountOpenedEventName is the event name for BankAccountOpened
	BankAccountOpenedEventName = "bank_account_opened"
	// BankAccountCreditedEventName is the event name for BankAccountCredited
	BankAccountCreditedEventName = "bank_account_credited"
	// BankAccountDebitedName is the event name for BankAccountDebited
	BankAccountDebitedEventName = "bank_account_debited"
)

type (
	// BankAccountOpened a DomainEvent indicating that a bank account was opened
	BankAccountOpened struct {
		AccountID aggregate.ID `json:"account_id"`
	}

	// BankAccountCredited a DomainEvent indicating that a bank account was credited
	BankAccountCredited struct {
		Amount uint `json:"amount"`
	}

	// BankAccountDebited a DomainEvent indicating that a bank account was debited
	BankAccountDebited struct {
		Amount uint `json:"amount"`
	}
)
