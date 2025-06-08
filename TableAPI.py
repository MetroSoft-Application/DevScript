from azure.data.tables import TableServiceClient, TableEntity
import random
import time
from datetime import datetime, timedelta
from azure.core.exceptions import HttpResponseError
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sys

# Cosmos DB接続文字列
connection_string = ""
table_name = ""

# コマンドライン引数からパーティションキー名を取得
partition_key_prefix = sys.argv[1] if len(sys.argv) > 1 else "Batch"

# 並列処理設定
max_workers = 5  # 並列処理数（必要に応じて変更）

# Table Service Clientを作成
table_service = TableServiceClient.from_connection_string(conn_str=connection_string)

# テーブルが存在しない場合は作成
try:
    table_service.create_table(table_name)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] テーブル '{table_name}' を作成しました。")
except Exception as e:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Table creation skipped: {e}")

# スレッドローカルストレージでテーブルクライアントを管理
thread_local = threading.local()

def get_table_client():
    """スレッドローカルなテーブルクライアントを取得"""
    if not hasattr(thread_local, 'table_client'):
        thread_local.table_client = table_service.get_table_client(table_name=table_name)
    return thread_local.table_client

def get_current_time():
    """現在時刻を日本語フォーマットで取得"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def process_batch(batch_num, batch_size):
    """単一バッチを処理する関数"""
    table_client = get_table_client()
    entities = []
    partition_key = f"{partition_key_prefix}_{batch_num // 50}"
    
    start_time = get_current_time()
    
    for item_num in range(batch_size):
        current_time = datetime.now()
        order_date = current_time - timedelta(days=random.randint(0, 30))
        entity = TableEntity()
        entity['PartitionKey'] = partition_key
        entity['RowKey'] = f"Order_{batch_num}_{item_num}"
        entity['CreatedAt'] = current_time.isoformat()
        entity['UserId'] = random.randint(1000, 9999)
        entity['UserName'] = f"User_{random.randint(1, 5000)}"
        entity['ProductId'] = random.randint(100, 999)
        entity['ProductName'] = f"Product_{random.randint(1, 100)}"
        entity['Price'] = round(random.uniform(10.0, 500.0), 2)
        entity['Quantity'] = random.randint(1, 20)
        entity['Total'] = round(entity['Price'] * entity['Quantity'], 2)
        entity['OrderDate'] = order_date.isoformat()
        entities.append(entity)

    success = False
    retries = 0
    while not success and retries < 30:
        try:
            table_client.submit_transaction([("upsert", e) for e in entities])
            success = True
            end_time = get_current_time()
            return f"[{end_time}] Batch {batch_num + 1} inserted successfully. (開始: {start_time})"
        except HttpResponseError as ex:
            if ex.status_code == 429:
                wait_time = 2 * retries
                current_time = get_current_time()
                print(f"[{current_time}] Thread {threading.current_thread().name}: RU制限超過。{wait_time}秒後に再試行します。(Batch {batch_num + 1})")
                time.sleep(wait_time)
                retries += 1
            else:
                error_time = get_current_time()
                print(f"[{error_time}] Batch {batch_num + 1} でHTTPエラーが発生: {ex}")
                raise ex
    
    fail_time = get_current_time()
    return f"[{fail_time}] Batch {batch_num + 1} failed after retries."

# 大量データ投入（並列処理版）
batch_size = 100  # 一括投入のバッチサイズ(変更しないこと)
num_batches = 100  # バッチの回数

# トータル処理予定件数を計算
total_expected_records = batch_size * num_batches

start_process_time = get_current_time()
print(f"[{start_process_time}] 並列処理開始 - 並列数: {max_workers}, バッチ数: {num_batches}")
print(f"[{start_process_time}] パーティションキープレフィックス: {partition_key_prefix}")
print(f"[{start_process_time}] 処理予定レコード数: {total_expected_records:,}件")

# ThreadPoolExecutorを使用して並列処理
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # 全バッチをサブミット
    future_to_batch = {
        executor.submit(process_batch, batch_num, batch_size): batch_num 
        for batch_num in range(num_batches)
    }
    
    completed_count = 0
    success_count = 0
    failed_count = 0
    successful_records = 0
    failed_records = 0
    
    # 完了したタスクから順次結果を取得
    for future in as_completed(future_to_batch):
        batch_num = future_to_batch[future]
        try:
            result = future.result()
            completed_count += 1
            
            # 成功・失敗の判定
            if "successfully" in result:
                success_count += 1
                successful_records += batch_size  # 成功したバッチのレコード数を加算
            else:
                failed_count += 1
                failed_records += batch_size  # 失敗したバッチのレコード数を加算
            
            current_time = get_current_time()
            print(f"[{current_time}] [{completed_count}/{num_batches}] {result}")
            
        except Exception as exc:
            completed_count += 1
            failed_count += 1
            failed_records += batch_size  # 例外が発生したバッチのレコード数を加算
            error_time = get_current_time()
            print(f"[{error_time}] [{completed_count}/{num_batches}] Batch {batch_num + 1} generated an exception: {exc}")

# 処理完了時の統計情報を表示
total_processed_records = successful_records + failed_records
end_process_time = get_current_time()

print(f"[{end_process_time}] Data insertion complete.")
print(f"[{end_process_time}] ===== 処理結果統計 =====")
print(f"[{end_process_time}] バッチ処理結果 - 成功: {success_count}, 失敗: {failed_count}, 合計: {completed_count}")
print(f"[{end_process_time}] レコード処理結果 - 成功: {successful_records:,}件, 失敗: {failed_records:,}件")
print(f"[{end_process_time}] トータル処理件数: {total_processed_records:,}件 / 予定件数: {total_expected_records:,}件")
print(f"[{end_process_time}] 成功率: {(successful_records / total_expected_records * 100):.2f}%")
print(f"[{end_process_time}] 処理開始時刻: {start_process_time}")
print(f"[{end_process_time}] 処理終了時刻: {end_process_time}")