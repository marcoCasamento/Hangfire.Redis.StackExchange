using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Redis
{
    internal class SucceededStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList("succeeded", context.BackgroundJob.Id);

            var storage = context.Storage as RedisStorage;
            if (storage != null && storage.SucceededListSize > 0)
            {
                transaction.TrimList("succeeded", 0, storage.SucceededListSize);
            }
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromList("succeeded", context.BackgroundJob.Id);
        }

        public string StateName => SucceededState.StateName;
    }
}