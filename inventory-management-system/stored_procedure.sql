-- Creating a stored procedure to automate the entire process of ordering and logging
CREATE PROCEDURE PlaceMultiItemOrder
    @customer_id INT,
    @items ProductOrderType READONLY
AS
BEGIN
    DECLARE @order_id INT, @price DECIMAL(10,2), @total_amount DECIMAL(10,2) = 0;

    -- Create the order first (we'll calculate total_amount after adding details)
    INSERT INTO Orders (customer_id, order_date, total_amount)
    VALUES (@customer_id, GETDATE(), 0);

    SET @order_id = SCOPE_IDENTITY();

    -- Loop through each item in the table
    DECLARE @product_id INT, @quantity INT;

    DECLARE item_cursor CURSOR FOR
    SELECT ProductID, Quantity FROM @items;

    OPEN item_cursor;
    FETCH NEXT FROM item_cursor INTO @product_id, @quantity;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Get the price
        SELECT @price = price FROM Products WHERE product_id = @product_id;

        -- Insert into OrderDetails
        INSERT INTO OrderDetails (order_id, product_id, quantity, price)
        VALUES (@order_id, @product_id, @quantity, @price);

        -- Update total amount
        SET @total_amount = @total_amount + (@price * @quantity);

        -- Update stock
        UPDATE Products
        SET stock_quantity = stock_quantity - @quantity
        WHERE product_id = @product_id;

        -- Log inventory change
        INSERT INTO InventoryLogs (product_id, ChangeType, QuantityChanged)
        VALUES (@product_id, 'Order', -@quantity);

        FETCH NEXT FROM item_cursor INTO @product_id, @quantity;
    END;

    CLOSE item_cursor;
    DEALLOCATE item_cursor;

    -- Update total amount in Orders
    UPDATE Orders
    SET total_amount = @total_amount
    WHERE order_id = @order_id;
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