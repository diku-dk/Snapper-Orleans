namespace Concurrency.Interface
{
    public class CommittedState<TState>
    {
        private TState state;

        public CommittedState(TState state)
        {
            this.state = state;
        }

        public TState GetState()
        {
            return state;
        }

        public void SetState(TState state)
        {
            this.state = state;
        }
    }
}