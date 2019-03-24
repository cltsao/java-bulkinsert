DROP TABLE [TestDatabase].[dbo].[inventory];
DROP TABLE [TestDatabase].[dbo].[products];

CREATE TABLE [TestDatabase].[dbo].[products]
( product_id INT IDENTITY(1,1) PRIMARY KEY,
  product_name VARCHAR(50) NOT NULL
);

CREATE TABLE [TestDatabase].[dbo].[inventory]
( inventory_id INT IDENTITY(1,1) PRIMARY KEY,
  product_id INT NOT NULL,
  quantity INT,
  CONSTRAINT fk_inv_product_id
    FOREIGN KEY (product_id)
    REFERENCES products (product_id)
);

SELECT name, is_not_trusted FROM [TestDatabase].[sys].[foreign_keys];