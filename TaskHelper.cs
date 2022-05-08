using Amazon.DynamoDBv2.Model;
using System;
using System.Threading.Tasks;

namespace DynamoDbTransfer
{
    public static class TaskHelper
    {
        public static async Task<TReturn> ExecWithThroughputControlWithResult<TReturn>(Func<Task<TReturn>> exec)
        {
            try
            {
                return await exec.Invoke();
            }
            catch (ProvisionedThroughputExceededException ex)
            {
                Console.WriteLine($"ProvisionedThroughputExceededException: {ex.Message}");

                while (true)
                {
                    await Task.Delay(5000);

                    try
                    {
                        return await exec.Invoke();
                    }
                    catch (Amazon.DynamoDBv2.Model.ProvisionedThroughputExceededException exc)
                    {
                        Console.WriteLine($"ProvisionedThroughputExceededException: {exc.Message}");
                    }
                }            
            }
        }

        public static async Task ExecWithThroughputControl(Func<Task> exec)
        {
            try
            {
                await exec.Invoke();
            }
            catch (ProvisionedThroughputExceededException ex)
            {
                Console.WriteLine($"ProvisionedThroughputExceededException: {ex.Message}");

                while (true)
                {
                    await Task.Delay(5000);

                    try
                    {
                        await exec.Invoke();

                        break;
                    }
                    catch (Amazon.DynamoDBv2.Model.ProvisionedThroughputExceededException exc)
                    {
                        Console.WriteLine($"ProvisionedThroughputExceededException: {exc.Message}");
                    }
                }
            }
        }
    }
}
