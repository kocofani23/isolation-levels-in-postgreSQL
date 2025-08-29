import threading
import time
import psycopg2
import matplotlib.pyplot as plt

# Database connection parameters
db_config = {
    'dbname': 'odev1',
    'user': 'postgres',
    'password': 'koco',
    'host': 'localhost',
    'port': '5432'
}

# Function that performs TX-A operation for a given list of account numbers with optional retry logic
def transfer_salary(accounts, isolation_level='SERIALIZABLE', use_retry=False, max_retries=3):
    for attempt in range(max_retries if use_retry else 1):
        try:
            conn = psycopg2.connect(**db_config)
            conn.autocommit = False
            cur = conn.cursor()
            cur.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level};")

            for acc in accounts:
                cur.execute("UPDATE Accounts SET balance = balance - 1 WHERE accno = 0;")
                cur.execute("UPDATE Accounts SET balance = balance + 1 WHERE accno = %s;", (acc,))

            conn.commit()
            cur.close()
            conn.close()
            return  # Success, exit the function

        except psycopg2.Error as e:
            if 'could not serialize access' in str(e) and use_retry and attempt < max_retries - 1:
                time.sleep(0.05 * (attempt + 1))  # backoff before retry
                continue
            print(f"Error in transaction for accounts {accounts}: {e}")
            return

# Helper to divide accounts into chunks for each thread
def get_account_chunks(k, all_accounts):
    return [all_accounts[i:i+k] for i in range(0, len(all_accounts), k)]

# Reset the database to the initial state
def reset_accounts():
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute("DELETE FROM Accounts;")
        cur.execute("INSERT INTO Accounts (accno, balance) VALUES (0, 100);")
        cur.execute("INSERT INTO Accounts (accno, balance)"
                    " SELECT i, 0 FROM generate_series(1, 100) AS s(i);")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print("Could not reset database to initial value")
        print("Error:", e)

# Run tests for a given isolation level and retry policy
def run_test(isolation_level, use_retry):
    print(f"\n=== Running test for Isolation Level: {isolation_level} | Retry Enabled: {use_retry} ===")
    accounts = list(range(1, 101))
    k_values = [2, 10, 50, 100]

    # Initialize result storage
    time_taken = []
    tps_values = []
    correctness_values = []

    for k in k_values:
        reset_accounts()

        chunks = get_account_chunks(k, accounts)
        threads = []
        start_time = time.time()

        for chunk in chunks:
            t = threading.Thread(target=transfer_salary, args=(chunk, isolation_level, use_retry))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        elapsed_time = time.time() - start_time
        tps = len(chunks) / elapsed_time

        try:
            conn = psycopg2.connect(**db_config)
            cur = conn.cursor()
            cur.execute("SELECT balance FROM Accounts WHERE accno = 0;")
            final_balance = cur.fetchone()[0]
            cur.close()
            conn.close()
        except Exception as e:
            print("Error fetching final balance:", e)
            final_balance = None

        c_value = (100 - final_balance) / 100 if final_balance is not None else None

        # Store the results for graphing
        time_taken.append(elapsed_time)
        tps_values.append(tps)
        correctness_values.append(c_value)

        # Print results for each k value
        print(f"\nResults for k={k}:")
        print(f"Isolation level: {isolation_level}")
        print(f"Retry enabled: {use_retry}")
        print(f"Time taken (s): {round(elapsed_time, 4)}")
        print(f"TPS (transactions/sec): {round(tps, 4)}")
        print(f"Correctness (c-value): {round(c_value, 4) if c_value is not None else 'N/A'}")

    return k_values, time_taken, tps_values, correctness_values

def plot_results(k_values, time_taken_serializable, tps_serializable, correctness_serializable,
                 time_taken_serializable_no_retry, tps_serializable_no_retry, correctness_serializable_no_retry,
                 time_taken_read_committed, tps_read_committed, correctness_read_committed):
    # Plot time taken for different isolation levels and K values
    plt.figure(figsize=(12, 8))
    plt.subplot(3, 1, 1)
    plt.plot(k_values, time_taken_serializable, label='Serializable with Retry', marker='o')
    plt.plot(k_values, time_taken_read_committed, label='Read Committed', marker='o')
    plt.plot(k_values, time_taken_serializable_no_retry, label='Serializable without Retry', marker='o')
    plt.xlabel('K (Account Chunks)')
    plt.ylabel('Time Taken (s)')
    plt.title('Time Taken for Different Isolation Levels')
    plt.legend()

    # Plot TPS for different isolation levels and K values
    plt.subplot(3, 1, 2)
    plt.plot(k_values, tps_serializable, label='Serializable with Retry', marker='o')
    plt.plot(k_values, tps_read_committed, label='Read Committed', marker='o')
    plt.plot(k_values, tps_serializable_no_retry, label='Serializable without Retry', marker='o')
    plt.xlabel('K (Account Chunks)')
    plt.ylabel('Transactions per Second')
    plt.title('TPS for Different Isolation Levels')
    plt.legend()

    # Plot Correctness (c-value) for different isolation levels and K values
    plt.subplot(3, 1, 3)
    plt.plot(k_values, correctness_serializable, label='Serializable with Retry', marker='o')
    plt.plot(k_values, correctness_read_committed, label='Read Committed', marker='o')
    plt.plot(k_values, correctness_serializable_no_retry, label='Serializable without Retry', marker='o')
    plt.xlabel('K (Account Chunks)')
    plt.ylabel('Correctness (c-value)')
    plt.title('Correctness for Different Isolation Levels')
    plt.legend()

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Run tests for each isolation level and retry policy
    k_values = [2, 10, 50, 100]
    
    # 1. SERIALIZABLE with retry (manual optimistic retry)
    k_values_serializable, time_taken_serializable, tps_serializable, correctness_serializable = run_test('SERIALIZABLE', use_retry=True)

    # 2. SERIALIZABLE without retry (pure PostgreSQL optimistic concurrency)
    k_values_serializable_no_retry, time_taken_serializable_no_retry, tps_serializable_no_retry, correctness_serializable_no_retry = run_test('SERIALIZABLE', use_retry=False)

    # 3. READ COMMITTED (default, no retry needed)
    k_values_read_committed, time_taken_read_committed, tps_read_committed, correctness_read_committed = run_test('READ COMMITTED', use_retry=False)

    # Plot the results
    plot_results(k_values_serializable, time_taken_serializable, tps_serializable, correctness_serializable,
                 time_taken_serializable_no_retry, tps_serializable_no_retry, correctness_serializable_no_retry,
                 time_taken_read_committed, tps_read_committed, correctness_read_committed)

    # Save the plot to a file
    plt.figure(figsize=(12, 8))
    plt.subplot(3, 1, 1)
    plt.plot(k_values_serializable, time_taken_serializable, label='Serializable with Retry', marker='o')
    plt.plot(k_values_read_committed, time_taken_read_committed, label='Read Committed', marker='o')
    plt.plot(k_values_serializable_no_retry, time_taken_serializable_no_retry, label='Serializable Optimistic Approach', marker='o')
    plt.xlabel('K (Account Chunks)')
    plt.ylabel('Time Taken (s)')
    plt.title('Time Taken for Different Isolation Levels')
    plt.legend()

    plt.subplot(3, 1, 2)
    plt.plot(k_values_serializable, tps_serializable, label='Serializable with Retry', marker='o')
    plt.plot(k_values_read_committed, tps_read_committed, label='Read Committed', marker='o')
    plt.plot(k_values_serializable_no_retry, tps_serializable_no_retry, label='Serializable Optimistic Approach', marker='o')

    plt.xlabel('K (Account Chunks)')
    plt.ylabel('Transactions per Second')
    plt.title('TPS for Different Isolation Levels')
    plt.legend()

    plt.subplot(3, 1, 3)
    plt.plot(k_values_serializable, correctness_serializable, label='Serializable with Retry', marker='o')
    plt.plot(k_values_read_committed, correctness_read_committed, label='Read Committed', marker='o')
    plt.plot(k_values_serializable_no_retry, correctness_serializable_no_retry, label='Serializable Optimistic Approach', marker='o')

    plt.xlabel('K (Account Chunks)')
    plt.ylabel('Correctness (c-value)')
    plt.title('Correctness for Different Isolation Levels')
    plt.legend()

    plt.tight_layout()
    plt.savefig('transaction_isolation_results.png')  # Save the plot as a PNG file
    print("Graph saved as 'transaction_isolation_results.png'")
