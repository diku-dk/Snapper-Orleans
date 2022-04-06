using System;
using System.Runtime.Serialization;
using MessagePack;

namespace SmallBank.Grains
{
    [MessagePackObject]
    public class BankAccount : ICloneable, ISerializable
    {
        [Key(0)]
        public int accountID = -1;
        [Key(1)]
        public float balance = 0;

        public BankAccount()
        {
        }

        /// <summary> This constructor is used to deserialize values. </summary>
        public BankAccount(SerializationInfo info, StreamingContext context)
        {
            accountID = (int) info.GetValue("ID", typeof(int));
            balance = (float)info.GetValue("balance", typeof(float));
        }

        /// <summary> This method is used to serialize values. </summary>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("ID", accountID, typeof(int));
            info.AddValue("balance", balance, typeof(float));
        }

        public object Clone()
        {
            var account = new BankAccount();
            account.accountID = accountID;
            account.balance = balance;
            return account;
        }
    }
}