using System;

namespace SmallBank.Grains
{
    [Serializable]
    public class BankAccount : ICloneable
    {
        public int accountID = -1;
        public float balance = 0;

        public BankAccount()
        {
        }

        object ICloneable.Clone()
        {
            var account = new BankAccount();
            account.accountID = accountID;
            account.balance = balance;
            return account;
        }
    }
}