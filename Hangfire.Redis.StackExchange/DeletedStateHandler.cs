using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Redis
{
    internal class DeletedStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList("deleted", context.BackgroundJob.Id);
            
            var storage = context.Storage as RedisStorage;
            if (storage != null && storage.DeletedListSize > 0)
            {
                transaction.TrimList("deleted", 0, storage.DeletedListSize);
            }
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromList("deleted", context.BackgroundJob.Id);
        }

        public string StateName => DeletedState.StateName;
    }
}