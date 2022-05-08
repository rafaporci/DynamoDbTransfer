## What is DynamoDbTransfer?

It's a simple C# console application project to transfer data between accounts in the AWS. It allows customized copy strategies for each table (e.g.: batch size, page size, pre copy actions), controls the throughput and can increase/decrease the tables capacity.

## Running the progam

You need to specify in the Program.cs:
- The credencials for source and target accounts;
- The tables specification;

## Future improvements

- Cover the project with Unit and Integration Tests;
- Move some settings to config files;
- Move some responsabilites (such as capacityUpdate) to specific classes;
- Rewrite the ExecWithThroughputControl and ExecWithThroughputControlWithResult methods with recursion limiting the retries;

## Usage

This project was created for a specific migration with several tables, you can use freely, if you have some doubt, please feel free to reach me.