using System;
using System.Collections.Generic;
using System.Text;
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
            catch (Amazon.DynamoDBv2.Model.ProvisionedThroughputExceededException ex)
            {
                Console.WriteLine($"ProvisionedThroughputExceededException: {ex.Message}");
                int i = 0;

                while (true)
                {
                    System.Threading.Thread.Sleep(5000);

                    i++;

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
            catch (Amazon.DynamoDBv2.Model.ProvisionedThroughputExceededException ex)
            {
                Console.WriteLine($"ProvisionedThroughputExceededException: {ex.Message}");

                int i = 0;

                while (true)
                {
                    System.Threading.Thread.Sleep(5000);

                    i++;

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
