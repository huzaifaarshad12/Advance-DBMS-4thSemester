-- Inventory Management System Tables
CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    CostPrice DECIMAL(10, 2),
    SellingPrice DECIMAL(10, 2),
    StockQuantity INT
);

CREATE TABLE Suppliers (
    SupplierID INT PRIMARY KEY,
    SupplierName VARCHAR(100),
    ContactDetails VARCHAR(255)
);

CREATE TABLE Purchases (
    PurchaseID INT PRIMARY KEY,
    ProductID INT,
    SupplierID INT,
    Quantity INT,
    PurchaseDate DATE,
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID),
    FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID)
);

CREATE TABLE Sales (
    SaleID INT PRIMARY KEY,
    ProductID INT,
    CustomerID INT,
    QuantitySold INT,
    SaleDate DATE,
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    ContactDetails VARCHAR(255)
);

-- Payroll Management System Tables
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    FullName VARCHAR(100),
    Department VARCHAR(50),
    BaseSalary DECIMAL(10, 2),
    HireDate DATE
);

CREATE TABLE Payroll (
    PayrollID INT PRIMARY KEY,
    EmployeeID INT,
    PayPeriodStart DATE,
    PayPeriodEnd DATE,
    GrossPay DECIMAL(10, 2),
    TaxDeductions DECIMAL(10, 2),
    NetPay DECIMAL(10, 2),
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
);

CREATE TABLE Bonuses (
    BonusID INT PRIMARY KEY,
    EmployeeID INT,
    BonusAmount DECIMAL(10, 2),
    BonusDate DATE,
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
);

CREATE TABLE Overtime (
    OvertimeID INT PRIMARY KEY,
    EmployeeID INT,
    HoursWorked INT,
    OvertimePay DECIMAL(10, 2),
    OvertimeDate DATE,
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
);



-- Inventory Management System Queries
-- 1. Current Stock Levels
SELECT ProductName, StockQuantity
FROM Products;

-- 2. Low Stock Alert
SELECT ProductName, StockQuantity
FROM Products
WHERE StockQuantity < 10;

-- 3. Top-Selling Products
SELECT ProductName, SUM(QuantitySold) AS TotalSold
FROM Sales
JOIN Products ON Sales.ProductID = Products.ProductID
GROUP BY ProductName
ORDER BY TotalSold DESC;

-- 4. Total Inventory Value
SELECT SUM(StockQuantity * CostPrice) AS TotalInventoryValue
FROM Products;

-- 5. Suppliers of a Specific Product
SELECT DISTINCT SupplierName
FROM Suppliers
JOIN Purchases ON Suppliers.SupplierID = Purchases.SupplierID
WHERE ProductID = 1;

-- 6. Monthly Revenue Trends
SELECT MONTH(SaleDate) AS Month, YEAR(SaleDate) AS Year, SUM(QuantitySold * SellingPrice) AS TotalRevenue
FROM Sales
JOIN Products ON Sales.ProductID = Products.ProductID
GROUP BY YEAR(SaleDate), MONTH(SaleDate)
ORDER BY Year, Month;

-- 7. Customer Purchase History
SELECT Sales.SaleDate, Products.ProductName, Sales.QuantitySold
FROM Sales
JOIN Products ON Sales.ProductID = Products.ProductID
WHERE Sales.CustomerID = 1;

-- 8. Unsold Products
SELECT ProductName
FROM Products
WHERE ProductID NOT IN (SELECT DISTINCT ProductID FROM Sales);

-- 9. Pending Deliveries
SELECT CustomerID, ProductID, QuantitySold, SaleDate
FROM Sales
WHERE SaleDate > GETDATE();

-- 10. Most Reliable Supplier
SELECT TOP 1 SupplierName, COUNT(PurchaseID) AS TotalPurchases
FROM Suppliers
JOIN Purchases ON Suppliers.SupplierID = Purchases.SupplierID
GROUP BY SupplierName
ORDER BY TotalPurchases DESC;

-- 11. Delivery Efficiency
SELECT AVG(DATEDIFF(DAY, PurchaseDate, GETDATE())) AS AverageDeliveryTime
FROM Purchases;

-- 12. Most Expensive Purchases
SELECT ProductName, MAX(CostPrice) AS HighestCost
FROM Products;

-- 13. Top Customers
SELECT Customers.CustomerName, SUM(Sales.QuantitySold * Products.SellingPrice) AS TotalSpent
FROM Sales
JOIN Customers ON Sales.CustomerID = Customers.CustomerID
JOIN Products ON Sales.ProductID = Products.ProductID
GROUP BY Customers.CustomerName
ORDER BY TotalSpent DESC;

-- 14. Category-Wise Stock Levels
SELECT Category, SUM(StockQuantity) AS TotalStock
FROM Products
GROUP BY Category;

-- 15. Profitability Analysis
SELECT ProductName, SUM(QuantitySold * SellingPrice) - SUM(QuantitySold * CostPrice) AS Profit
FROM Sales
JOIN Products ON Sales.ProductID = Products.ProductID
GROUP BY ProductName
ORDER BY Profit DESC;



-- Payroll Management System Queries--
-- 1. Employee Salary Slips
SELECT Employees.FullName, Payroll.PayPeriodStart, Payroll.PayPeriodEnd, Payroll.GrossPay, Payroll.TaxDeductions, Payroll.NetPay
FROM Payroll
JOIN Employees ON Payroll.EmployeeID = Employees.EmployeeID;

-- 2. Monthly Payroll Summary
SELECT MONTH(PayPeriodStart) AS Month, YEAR(PayPeriodStart) AS Year, SUM(NetPay) AS TotalPayroll
FROM Payroll
GROUP BY YEAR(PayPeriodStart), MONTH(PayPeriodStart);

-- 3. Tax Deductions Report
SELECT EmployeeID, SUM(TaxDeductions) AS TotalTax
FROM Payroll
GROUP BY EmployeeID;

-- 4. Overtime Payments
SELECT Employees.FullName, SUM(Overtime.OvertimePay) AS TotalOvertimePay
FROM Overtime
JOIN Employees ON Overtime.EmployeeID = Employees.EmployeeID
GROUP BY Employees.FullName;

-- 5. Employee Pay History
SELECT Payroll.PayPeriodStart, Payroll.PayPeriodEnd, Payroll.GrossPay, Payroll.TaxDeductions, Payroll.NetPay
FROM Payroll
WHERE EmployeeID = 1;

-- 6. Bonuses Distributed
SELECT Employees.FullName, SUM(Bonuses.BonusAmount) AS TotalBonuses
FROM Bonuses
JOIN Employees ON Bonuses.EmployeeID = Employees.EmployeeID
GROUP BY Employees.FullName;

-- 7. Unpaid Employees
SELECT Employees.FullName
FROM Employees
WHERE EmployeeID NOT IN (SELECT EmployeeID FROM Payroll);

-- 8. Deductions Breakdown
SELECT SUM(TaxDeductions) AS TotalTax, SUM(GrossPay - NetPay - TaxDeductions) AS OtherDeductions
FROM Payroll;

-- 9. Highest Earners
SELECT TOP 1 FullName, GrossPay AS HighestEarner
FROM Payroll
JOIN Employees ON Payroll.EmployeeID = Employees.EmployeeID
ORDER BY GrossPay DESC;

-- 10. Payroll Errors
SELECT PayrollID, EmployeeID, GrossPay, NetPay, GrossPay - TaxDeductions - NetPay AS Error
FROM Payroll
WHERE GrossPay - TaxDeductions <> NetPay;

-- 11. Department-Wise Payroll
SELECT Department, SUM(NetPay) AS TotalPayroll
FROM Employees
JOIN Payroll ON Employees.EmployeeID = Payroll.EmployeeID
GROUP BY Department;

-- 12. Annual Tax Reports
SELECT YEAR(PayPeriodStart) AS Year, SUM(TaxDeductions) AS TotalTax
FROM Payroll
GROUP BY YEAR(PayPeriodStart);

-- 13. Employee Loan Repayments
SELECT Employees.FullName, Payroll.PayPeriodStart, Payroll.NetPay
FROM Payroll
JOIN Employees ON Payroll.EmployeeID = Employees.EmployeeID
WHERE Payroll.NetPay < Payroll.GrossPay;

-- 14. New Hires Salary Costs
SELECT HireDate, SUM(BaseSalary) AS TotalCost
FROM Employees
WHERE HireDate > '2024-01-01'
GROUP BY HireDate;

-- 15. Employee Benefit Costs
SELECT SUM(GrossPay - NetPay - TaxDeductions) AS BenefitCosts
FROM Payroll;