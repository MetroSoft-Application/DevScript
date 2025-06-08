using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace TableAPI
{
    class Program
    {
        // Azure Cosmos DB Table API 接続文字列
        private static readonly string ConnectionString = "";
        private static readonly string TableName = "test2";

        // 並列処理設定
        private static readonly int MaxWorkers = 5; // 並列処理数（必要に応じて変更）
        private static readonly int BatchSize = 100; // 一括投入のバッチサイズ(変更しないこと)
        private static readonly int NumBatches = 100; // バッチの回数

        // スレッドセーフな CloudTableClient へのアクセス
        private static readonly ThreadLocal<CloudTable> ThreadLocalTableClient = new(() =>
        {
            var storageAccount = CloudStorageAccount.Parse(ConnectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            return tableClient.GetTableReference(TableName);
        });

        static async Task Main(string[] args)
        {
            // コマンドライン引数からパーティションキー名を取得
            string partitionKeyPrefix = args.Length > 0 ? args[0] : "BatchCS";

            // CloudStorageAccount を作成
            var storageAccount = CloudStorageAccount.Parse(ConnectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference(TableName);

            // テーブルが存在しない場合は作成
            try
            {
                bool tableCreated = await table.CreateIfNotExistsAsync();
                if (tableCreated)
                {
                    Console.WriteLine($"[{GetCurrentTime()}] テーブル '{TableName}' を作成しました。");
                }
                else
                {
                    Console.WriteLine($"[{GetCurrentTime()}] テーブル '{TableName}' は既に存在します。");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[{GetCurrentTime()}] Table creation skipped: {e.Message}");
            }

            // トータル処理予定件数を計算
            int totalExpectedRecords = BatchSize * NumBatches;

            string startProcessTime = GetCurrentTime();
            Console.WriteLine($"[{startProcessTime}] 並列処理開始 - 並列数: {MaxWorkers}, バッチ数: {NumBatches}");
            Console.WriteLine($"[{startProcessTime}] パーティションキープレフィックス: {partitionKeyPrefix}");
            Console.WriteLine($"[{startProcessTime}] 処理予定レコード数: {totalExpectedRecords:N0}件");

            // 処理結果を追跡するためのカウンター
            int completedCount = 0;
            int successCount = 0;
            int failedCount = 0;
            int successfulRecords = 0;
            int failedRecords = 0;

            // セマフォーを使用して並列実行数を制限
            SemaphoreSlim semaphore = new SemaphoreSlim(MaxWorkers);

            // タスクのリストを作成
            List<Task<(bool Success, string Message)>> tasks = new List<Task<(bool Success, string Message)>>();

            // タスクを作成して開始
            for (int batchNum = 0; batchNum < NumBatches; batchNum++)
            {
                int currentBatch = batchNum; // ラムダ式でのキャプチャ用

                tasks.Add(Task.Run(async () =>
                {
                    await semaphore.WaitAsync();
                    try
                    {
                        var result = await ProcessBatchAsync(currentBatch, BatchSize, partitionKeyPrefix);
                        return result;
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }

            // タスクが完了するたびに結果を処理
            while (tasks.Count > 0)
            {
                Task<(bool Success, string Message)> completedTask = await Task.WhenAny(tasks);
                tasks.Remove(completedTask);

                try
                {
                    var result = await completedTask;
                    bool success = result.Success;
                    string message = result.Message;

                    Interlocked.Increment(ref completedCount);

                    if (success)
                    {
                        Interlocked.Increment(ref successCount);
                        Interlocked.Add(ref successfulRecords, BatchSize);
                    }
                    else
                    {
                        Interlocked.Increment(ref failedCount);
                        Interlocked.Add(ref failedRecords, BatchSize);
                    }
                    Console.WriteLine($"[{GetCurrentTime()}] [{completedCount}/{NumBatches}] {message}");
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref completedCount);
                    Interlocked.Increment(ref failedCount);
                    Interlocked.Add(ref failedRecords, BatchSize);

                    Console.WriteLine($"[{GetCurrentTime()}] [{completedCount}/{NumBatches}] バッチ処理で例外が発生: {ex.Message}");
                }
            }

            // 処理完了時の統計情報を表示
            int totalProcessedRecords = successfulRecords + failedRecords;
            string endProcessTime = GetCurrentTime();

            Console.WriteLine($"[{endProcessTime}] Data insertion complete.");
            Console.WriteLine($"[{endProcessTime}] ===== 処理結果統計 =====");
            Console.WriteLine($"[{endProcessTime}] バッチ処理結果 - 成功: {successCount}, 失敗: {failedCount}, 合計: {completedCount}");
            Console.WriteLine($"[{endProcessTime}] レコード処理結果 - 成功: {successfulRecords:N0}件, 失敗: {failedRecords:N0}件");
            Console.WriteLine($"[{endProcessTime}] トータル処理件数: {totalProcessedRecords:N0}件 / 予定件数: {totalExpectedRecords:N0}件");
            Console.WriteLine($"[{endProcessTime}] 成功率: {(successfulRecords / (double)totalExpectedRecords * 100):F2}%");
            Console.WriteLine($"[{endProcessTime}] 処理開始時刻: {startProcessTime}");
            Console.WriteLine($"[{endProcessTime}] 処理終了時刻: {endProcessTime}");
        }

        private static async Task<(bool Success, string Message)> ProcessBatchAsync(int batchNum, int batchSize, string partitionKeyPrefix)
        {
            var tableClient = ThreadLocalTableClient.Value;
            var entities = new List<DynamicTableEntity>();
            string partitionKey = $"{partitionKeyPrefix}_{batchNum / 50}";

            string startTime = GetCurrentTime();
            for (int itemNum = 0; itemNum < batchSize; itemNum++)
            {
                // DateTime.Now の代わりに DateTime.UtcNow を使用、または DateTime.SpecifyKind で明示的に UTC に設定
                DateTime currentTime = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc);
                DateTime orderDate = DateTime.SpecifyKind(currentTime.AddDays(-Random.Shared.Next(0, 31)), DateTimeKind.Utc);

                var entity = new DynamicTableEntity(partitionKey, $"Order_{batchNum}_{itemNum}");

                // プロパティを設定
                entity.Properties["CreatedAt"] = EntityProperty.GeneratePropertyForDateTimeOffset(new DateTimeOffset(currentTime));
                entity.Properties["UserId"] = EntityProperty.GeneratePropertyForInt(Random.Shared.Next(1000, 10000));
                entity.Properties["UserName"] = EntityProperty.GeneratePropertyForString($"User_{Random.Shared.Next(1, 5001)}");
                entity.Properties["ProductId"] = EntityProperty.GeneratePropertyForInt(Random.Shared.Next(100, 1000));
                entity.Properties["ProductName"] = EntityProperty.GeneratePropertyForString($"Product_{Random.Shared.Next(1, 101)}");
                entity.Properties["Price"] = EntityProperty.GeneratePropertyForDouble(Math.Round(Random.Shared.NextDouble() * 490 + 10, 2));
                entity.Properties["Quantity"] = EntityProperty.GeneratePropertyForInt(Random.Shared.Next(1, 21));

                // 計算フィールド
                double price = entity.Properties["Price"].DoubleValue ?? 0.0;
                int quantity = entity.Properties["Quantity"].Int32Value ?? 0;
                entity.Properties["Total"] = EntityProperty.GeneratePropertyForDouble(Math.Round(price * quantity, 2));
                entity.Properties["OrderDate"] = EntityProperty.GeneratePropertyForDateTimeOffset(new DateTimeOffset(orderDate));

                entities.Add(entity);
            }

            bool success = false;
            int retries = 0;
            while (!success && retries < 30)
            {
                try
                {
                    // バッチ操作を使用（最大100エンティティずつ）
                    for (int i = 0; i < entities.Count; i += 100)
                    {
                        var batch = new TableBatchOperation();
                        foreach (var entity in entities.Skip(i).Take(Math.Min(100, entities.Count - i)))
                        {
                            batch.InsertOrReplace(entity);
                        }
                        await tableClient!.ExecuteBatchAsync(batch);
                    }

                    success = true;
                    string endTime = GetCurrentTime();
                    return (true, $"[{endTime}] Batch {batchNum + 1} inserted successfully. (開始: {startTime})");
                }
                catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 429)
                {
                    int waitTime = 2 * retries;
                    string currentTime = GetCurrentTime();
                    Console.WriteLine($"[{currentTime}] スレッド {Thread.CurrentThread.ManagedThreadId}: RU制限超過。{waitTime}秒後に再試行します。(Batch {batchNum + 1})");
                    await Task.Delay(waitTime * 1000);
                    retries++;
                }
                catch (Exception ex)
                {
                    string errorTime = GetCurrentTime();
                    Console.WriteLine($"[{errorTime}] Batch {batchNum + 1} でエラーが発生: {ex.Message}");
                    return (false, $"[{errorTime}] Batch {batchNum + 1} failed with error: {ex.Message}");
                }
            }

            string failTime = GetCurrentTime();
            return (false, $"[{failTime}] Batch {batchNum + 1} failed after {retries} retries.");
        }

        private static string GetCurrentTime()
        {
            // 現在時刻を日本語フォーマットで取得
            return DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        }
    }
}
