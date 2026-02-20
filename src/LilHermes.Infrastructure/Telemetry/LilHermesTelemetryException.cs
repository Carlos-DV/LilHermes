using System;
using System.Diagnostics;

namespace LilHermes.Infrastructure.Telemetry
{
    internal static class LilHermesTelemetryException
    {
        public static void AddExceptionCompat(this Activity activity, Exception ex)
        {
            if (activity == null || ex == null) return;

            activity.SetStatus(ActivityStatusCode.Error, ex.Message);

            activity.AddEvent(
                new ActivityEvent(
                    "exception",
                    DateTime.Now,
                    new ActivityTagsCollection
                    {
                        { "exception.type", ex.GetType().FullName },
                        { "exception.message", ex.Message },
                        { "exception.stacktrace", ex.StackTrace }
                    }
                )
            );
        }
    }
}