-- Create the Accounts table
CREATE TABLE Accounts (
    accno INTEGER PRIMARY KEY,
    balance INTEGER NOT NULL
);

-- Insert account-0 with 100 lira
INSERT INTO Accounts (accno, balance) VALUES (0, 100);

-- Insert accounts 1 to 100 with 0 lira each.
-- In PostgreSQL, you can use generate_series:
INSERT INTO Accounts (accno, balance)
SELECT i, 0
FROM generate_series(1, 100) AS s(i);

