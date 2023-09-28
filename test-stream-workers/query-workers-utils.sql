# ResetBalance
FOR bank IN Banks
  UPDATE bank WITH { "balance": 10000, "reserved": 0 } IN Banks


#CalTotal
LET totalBalance = SUM(
  FOR bank IN Banks
    RETURN bank.balance
)

LET totalReserved = SUM(
  FOR bank IN Banks
    RETURN bank.reserved
)

LET totalBanks = LENGTH(Banks)

RETURN {
    "totalBanks": totalBanks,
    "totalBalance": totalBalance,
    "totalReserved": totalReserved
}

#TruncateColl

FOR l IN Ledger
  REMOVE l IN Ledger

FOR p IN PaymentRequests
  REMOVE p IN PaymentRequests

FOR s IN Settlement
  REMOVE s IN Settlement

