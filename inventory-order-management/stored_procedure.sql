-- Creating a stored procedure to automate the entire process of ordering and logging
CREATE PROCEDURE Placeorder
    @customer_id INT,
    @product_id INT,
    @quantity INT
AS
BEGIN
    DECLARE @price DECIMAL(10,2), @total_amount DECIMAL(10,2), @order_id INT;

    -- Get product price
    SELECT @price = price FROM products WHERE product_id = @product_id;

    -- Calculate total
    SET @total_amount = @price * @quantity;

    -- Insert into orders
    INSERT INTO orders (customer_id, order_date, total_amount)
    VALUES (@customer_id, GETDATE(), @total_amount);

    SET @order_id = SCOPE_IDENTITY();  -- Get the generated order_id

    -- Insert into orderdetails
    INSERT INTO orderdetails (order_id, product_id, quantity, price)
    VALUES (@order_id, @product_id, @quantity, @price);

    -- Update stock
    UPDATE products
    SET stock_quantity = stock_quantity - @quantity
    WHERE product_id = @product_id;

    -- Log inventory change
    INSERT INTO inventoryLogs (product_id, changeType, quantity_changed)
    VALUES (@product_id, 'order', -@quantity);
END;

-- Creating a stored procedure to replenish stocks that are running out
CREATE PROCEDURE ReplenishStock
AS
BEGIN
    DECLARE @product_id INT;

    -- Cursor to loop through all low-stock products
    DECLARE low_stock_cursor CURSOR FOR
    SELECT product_id
    FROM products
    WHERE stock_quantity <= reorder_level;

    OPEN low_stock_cursor;
    FETCH NEXT FROM low_stock_cursor INTO @product_id;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Update stock
        UPDATE products
        SET stock_quantity = stock_quantity + 10
        WHERE product_id = @product_id;

        -- Log the replenishment
        INSERT INTO inventoryLogs (product_id, changeType, quantity_changed)
        VALUES (@product_id, 'Replenish', 10);

        FETCH NEXT FROM low_stock_cursor INTO @product_id;
    END;

    CLOSE low_stock_cursor;
    DEALLOCATE low_stock_cursor;
END;