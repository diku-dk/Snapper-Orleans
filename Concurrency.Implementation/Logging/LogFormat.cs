using System;
using System.Collections.Generic;
using System.Text;

namespace Concurrency.Implementation.Logging
{
    enum LogType { PREPARE, COMMIT, ABORT };
    
    [Serializable]
    class LogFormat<TState>
    {
        public LogType logRecordType;
        public bool onCoordinator;
        public long txn_id;
        public TState state;
        public LogFormat(LogType logRecordType, bool onCoordinator, long txn_id, TState state)
        {
            this.logRecordType = logRecordType;
            this.onCoordinator = onCoordinator;
            this.txn_id = txn_id;
            this.state = state;
        }

        public LogFormat(LogType logRecordType, bool onCoordinator, long txn_id)
        {
            this.logRecordType = logRecordType;
            this.onCoordinator = onCoordinator;
            this.txn_id = txn_id;
        }
    };
}
