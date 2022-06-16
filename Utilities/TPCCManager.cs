namespace Utilities
{
    public interface ITPCCManager
    {
        void Init(int numCPUPerSilo, int NUM_OrderGrain_PER_D);
        int GetItemGrain(int W_ID);
        int GetWarehouseGrain(int W_ID);
        int GetDistrictGrain(int W_ID, int D_ID);
        int GetCustomerGrain(int W_ID, int D_ID);
        int GetStockGrain(int W_ID, int I_ID);
        int GetOrderGrain(int W_ID, int D_ID, int C_ID);
    }

    public class TPCCManager : ITPCCManager
    {
        public int numCPUPerSilo;
        public int NUM_OrderGrain_PER_D = 1;
        public int NUM_GRAIN_PER_W;

        public void Init(int numCPUPerSilo, int NUM_OrderGrain_PER_D)
        {
            this.numCPUPerSilo = numCPUPerSilo;
            this.NUM_OrderGrain_PER_D = NUM_OrderGrain_PER_D;
            NUM_GRAIN_PER_W = 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + Constants.NUM_D_PER_W * NUM_OrderGrain_PER_D;
        }

        // 1 ItemGrain + 1 WarehouseGrain + 10 DistrictGrain + 10 CustomerGrain + xx StockGrain + yy OrderGrain

        public int GetItemGrain(int W_ID)
        {
            return W_ID * NUM_GRAIN_PER_W;
        }

        public int GetWarehouseGrain(int W_ID)
        {
            return W_ID * NUM_GRAIN_PER_W + 1;
        }

        public int GetDistrictGrain(int W_ID, int D_ID)
        {
            return W_ID * NUM_GRAIN_PER_W + 1 + 1 + D_ID;
        }

        public int GetCustomerGrain(int W_ID, int D_ID)
        {
            return W_ID * NUM_GRAIN_PER_W + 1 + 1 + 10 + D_ID;
        }

        public int GetStockGrain(int W_ID, int I_ID)
        {
            return W_ID * NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + I_ID / (Constants.NUM_I / Constants.NUM_StockGrain_PER_W);
        }

        public int GetOrderGrain(int W_ID, int D_ID, int C_ID)
        {
            return W_ID * NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * NUM_OrderGrain_PER_D + C_ID / (Constants.NUM_C_PER_D / NUM_OrderGrain_PER_D);
        }
    }
}