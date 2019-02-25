## Bank Example

Welcome to the GoEngine Bank. 

This is a basic example of how GoEngine can be used. This application consists of the following commands:
- [API](cmd/api) The banks HTTP API, here bank account can be opened, money can be deposited and withdrawn.
- [report-bank](cmd/report-bank) The banks general reported, this will report the total balance across all accounts and the total amount of bank accounts.
- [report-account](cmd/report-account) The account reporter keeps track of the average accounts debit and credit information.  

### Running the example

In order to run the example you need a postgres DB. The provided [docker-compose.yml](docker-compose.yml) provides this. 

Once you have your DB server up and running you need to set the DSN using the `POSTGRES_DSN` environment variable.
```bash   
export POSTGRES_DSN="postgres://goengine:goengine@localhost/goengine-bank?sslmode=disable&client_encoding=UTF8"
```

Now you can start once of the applications.
- API: `go run ./cmd/api/` 
- report-bank: `go run ./cmd/report-bank/`
- report-account: `go run ./cmd/report-account/`

