# isolation-levels-in-postgreSQL
Bu script yalnızca bir tablodan oluşan bir veritabanı üzerinde çalışması sağlanmıştır: Accounts(accno,balance). 
Kullanılan veritabanı programı PostgreSQL 17, arayüzü ise PGAdmin4 olarak tercih edilmiştir. 
Scriptin yazımında programlama dili olarak Python kullanılmıştır. Bu veritabanında, 0 numaralı 
hesaptan diğer 100 hesaba, 'serializable' ve 'read committed' gibi farklı izolasyon seviyelerinde, 
1 Türk lirası tutarında para transferi gerçekleştirilmiştir. Ayrıca, programda K olarak adlandırılan 
ve {2, 10, 50, 100} değerlerinden oluşan gruplar aracılığıyla bu işlem gerçekleştirilmiştir. Bu 
gruplar tarafından yapılan işlem şu şekilde uygulanmıştır: 
INSERT INTO Accounts (hesap no, bakiye) VALUES (0, 100); 
INSERT INTO Accounts (hesap no, bakiye) 
SELECT i, 0 
FROM generate_series(1, 100) AS s(i); 
Programın sonunda, her grup için gereken doğruluk, işlem/saniye (TPS) ve zaman değerleri 
ölçülmüştür ve bu sonuçlar yorumlanmıştır. 
