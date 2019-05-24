using System;
using System.Collections.Generic;
using System.Text;

namespace SmallBank.Interfaces
{   
    public enum BenchTxnType { Balance, DepositChecking, Transfer, TransactSaving, WriteCheck, MultiTransfer };

    public enum AllTxnTypes { Balance, DepositChecking, Transfer, TransactSaving, WriteCheck, MultiTransfer, Amalgamate, InitBankAccounts  };
}
