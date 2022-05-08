using System;

namespace DynamoDbTransfer
{
    public static class DateTimeExtentions
    {
        public static long ToUnixEpoch(this DateTime dateTime)
        {
            TimeSpan t = dateTime - new DateTime(1970, 1, 1);
            long secondsSinceEpoch = (long)t.TotalSeconds;
            return secondsSinceEpoch;
        }
    }
}
