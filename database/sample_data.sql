INSERT INTO customers (name, email, phone)
VALUES
('Aarav Sharma', 'aarav.sharma@gmail.com', '9876543210'),
('Priya Mehta', 'priya.mehta@gmail.com', '9876543211'),
('Rohan Verma', 'rohan.verma@gmail.com', '9876543212'),
('Ananya Gupta', 'ananya.gupta@gmail.com', '9876543213'),
('Kabir Malhotra', 'kabir.malhotra@gmail.com', '9876543214'),
('Ishita Kapoor', 'ishita.kapoor@gmail.com', '9876543215'),
('Arjun Singh', 'arjun.singh@gmail.com', '9876543216'),
('Meera Joshi', 'meera.joshi@gmail.com', '9876543217'),
('Aditya Rao', 'aditya.rao@gmail.com', '9876543218'),
('Sneha Iyer', 'sneha.iyer@gmail.com', '9876543219');


INSERT INTO products (name, price, stock, status)
VALUES
('Wireless Mouse', 799.00, 150, 'AVAILABLE'),
('Mechanical Keyboard', 2499.00, 80, 'AVAILABLE'),
('USB-C Cable', 499.00, 300, 'AVAILABLE'),
('Laptop Stand', 1299.00, 100, 'AVAILABLE'),
('Webcam', 3499.00, 60, 'AVAILABLE'),
('Bluetooth Headphones', 2999.00, 75, 'AVAILABLE'),
('Power Bank', 1799.00, 120, 'AVAILABLE'),
('Smart Watch', 4999.00, 50, 'AVAILABLE'),
('External SSD 1TB', 6999.00, 40, 'AVAILABLE'),
('Gaming Mouse', 1599.00, 90, 'AVAILABLE'),
('Monitor 24 Inch', 8999.00, 35, 'AVAILABLE'),
('Laptop Backpack', 1999.00, 110, 'AVAILABLE'),
('Wireless Charger', 1499.00, 130, 'AVAILABLE'),
('Desk Lamp', 999.00, 200, 'AVAILABLE'),
('Portable Speaker', 2299.00, 65, 'AVAILABLE');


INSERT INTO orders (customer_id, order_date, status, total_amount)
VALUES
(1, '2026-09-01 10:15:00', 'CONFIRMED', 3298.00),
(2, '2026-09-01 11:30:00', 'SHIPPED', 4999.00),
(3, '2026-09-02 09:45:00', 'DELIVERED', 7997.00),
(4, '2026-09-02 14:20:00', 'CONFIRMED', 2798.00),
(5, '2026-09-03 16:10:00', 'PENDING', 6999.00),
(6, '2026-09-04 12:00:00', 'SHIPPED', 4798.00),
(7, '2026-09-05 15:45:00', 'DELIVERED', 999.00),
(8, '2026-09-06 10:30:00', 'CONFIRMED', 8999.00),
(9, '2026-09-07 13:15:00', 'PENDING', 3396.00),
(10, '2026-09-08 18:20:00', 'CONFIRMED', 10998.00),
(1, '2026-09-09 09:10:00', 'SHIPPED', 1999.00),
(3, '2026-09-09 17:30:00', 'CONFIRMED', 5798.00);


INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES
(1, 1, 1, 799.00),
(1, 2, 1, 2499.00),
(2, 8, 1, 4999.00),
(3, 9, 1, 6999.00),
(3, 3, 2, 499.00),
(4, 4, 1, 1299.00),
(4, 13, 1, 1499.00),
(5, 9, 1, 6999.00),
(6, 6, 1, 2999.00),
(6, 7, 1, 1799.00),
(7, 14, 1, 999.00),
(8, 11, 1, 8999.00),
(9, 10, 1, 1599.00),
(9, 1, 1, 799.00),
(9, 3, 2, 499.00),
(10, 11, 1, 8999.00),
(10, 12, 1, 1999.00),
(11, 12, 1, 1999.00),
(12, 5, 1, 3499.00),
(12, 15, 1, 2299.00);


INSERT INTO payments (order_id, amount, payment_status, payment_date)
VALUES
(1, 3298.00, 'PAID', '2026-09-01 10:20:00'),
(2, 4999.00, 'PAID', '2026-09-01 11:35:00'),
(3, 7997.00, 'PAID', '2026-09-02 09:50:00'),
(4, 2798.00, 'PAID', '2026-09-02 14:25:00'),
(5, 6999.00, 'PENDING', '2026-09-03 16:15:00'),
(6, 4798.00, 'PAID', '2026-09-04 12:05:00'),
(7, 999.00, 'PAID', '2026-09-05 15:50:00'),
(8, 8999.00, 'PAID', '2026-09-06 10:35:00'),
(9, 3396.00, 'PENDING', '2026-09-07 13:20:00'),
(10, 10998.00, 'PAID', '2026-09-08 18:25:00'),
(11, 1999.00, 'PAID', '2026-09-09 09:15:00'),
(12, 5798.00, 'PENDING', '2026-09-09 17:35:00');

SELECT * FROM payments;