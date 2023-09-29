#reserve-money
FOR doc IN Banks FILTER (doc._key == @source_bank )  UPDATE doc WITH { reserved: doc.reserved +@amount } IN Banks RETURN {txnID:@txnID , source_bank: @source_bank, target_bank: @target_bank, amount: @amount, currency: @currency}

#cw-source-bank
FOR doc IN Banks FILTER (doc._key == @source_bank )  
UPDATE doc WITH { balance: doc.balance -@amount, reserved: doc.reserved -@amount } IN Banks RETURN {txnID:@txnID , source_bank: @source_bank, target_bank: @target_bank, amount: @amount, currency: @currency}

#cw-target-bank
FOR doc IN Banks FILTER (doc._key == @target_bank )  
UPDATE doc WITH { balance: doc.balance +@amount } IN Banks RETURN {txnID:@txnID , source_bank: @source_bank, target_bank: @target_bank, amount: @amount, currency: @currency}