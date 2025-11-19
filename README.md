Dưới đây là nội dung file `README.md` đầy đủ và chi tiết, mô tả cách sử dụng mã nguồn và các câu lệnh Hive đã được cung cấp trong bước trước để xử lý dữ liệu `amazon.csv`.

Bạn có thể lưu nội dung này vào file có tên **`README.md`**.

````markdown
# Phân tích Dữ liệu Sản phẩm Amazon (Big Data Analysis)

Dự án này cung cấp giải pháp xử lý và phân tích tập dữ liệu `amazon.csv` sử dụng **Hadoop MapReduce (Java)** và **Apache Hive**.

## Mục lục
1. [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
2. [Mô tả dữ liệu](#mô-tả-dữ-liệu)
3. [Cài đặt và Chạy (Hive)](#cài-đặt-và-chạy-hive)
4. [Cài đặt và Chạy (MapReduce)](#cài-đặt-và-chạy-mapreduce)
5. [Kết quả phân tích](#kết-quả-phân-tích)

---

## Yêu cầu hệ thống
* **Hadoop**: v2.7 trở lên (HDFS & YARN).
* **Hive**: v2.x hoặc v3.x.
* **Java JDK**: v1.8.
* **IDE**: IntelliJ IDEA hoặc Eclipse (để build code Java).

---

## Mô tả dữ liệu
Tập dữ liệu `amazon.csv` chứa thông tin về các sản phẩm bán trên Amazon.
* **Nguồn dữ liệu**: Uploaded (`/user/data/amazon/amazon.csv`).
* **Các trường chính**:
    * `product_id`: Mã sản phẩm.
    * `category`: Danh mục (phân cách bởi `|`).
    * `discounted_price`: Giá đã giảm (có ký tự tiền tệ).
    * `rating`: Điểm đánh giá.
    * `review_content`: Nội dung đánh giá.

---

## Cài đặt và Chạy (Hive)
Phương pháp này sử dụng HiveQL để làm sạch và truy vấn dữ liệu nhanh chóng.

### Bước 1: Chuẩn bị dữ liệu trên HDFS
Mở terminal và chạy các lệnh sau:
```bash
# Tạo thư mục chứa dữ liệu
hdfs dfs -mkdir -p /user/data/amazon

# Upload file csv lên HDFS
hdfs dfs -put amazon.csv /user/data/amazon/
````

### Bước 2: Tạo bảng và Làm sạch dữ liệu

Mở Hive shell (`hive`) hoặc Beeline và chạy:

1.  **Tạo bảng thô (Raw Table)**:

    ```sql
    CREATE EXTERNAL TABLE IF NOT EXISTS amazon_raw (
        product_id STRING, product_name STRING, category STRING, 
        discounted_price STRING, actual_price STRING, discount_percentage STRING, 
        rating STRING, rating_count STRING, about_product STRING, 
        user_id STRING, user_name STRING, review_id STRING, 
        review_title STRING, review_content STRING, img_link STRING, product_link STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
    STORED AS TEXTFILE
    LOCATION '/user/data/amazon';
    ```

2.  **Tạo View đã làm sạch (Cleaned View)**:

    ```sql
    CREATE VIEW amazon_clean AS
    SELECT
        product_id,
        SPLIT(category, '\\|')[0] AS main_category,
        CAST(REGEXP_REPLACE(discounted_price, '[₹,]', '') AS DOUBLE) AS price,
        CAST(REGEXP_REPLACE(discount_percentage, '[%]', '') AS INT) AS discount_percent,
        CAST(rating AS DOUBLE) AS rating
    FROM amazon_raw
    WHERE product_id != 'product_id';
    ```

### Bước 3: Thực thi các câu truy vấn

  * **Câu 1: Đếm sản phẩm theo danh mục**
    ```sql
    SELECT main_category, COUNT(*) FROM amazon_clean GROUP BY main_category;
    ```
  * **Câu 2: Top 5 sản phẩm giảm giá sâu nhất**
    ```sql
    SELECT product_id, discount_percent FROM amazon_clean ORDER BY discount_percent DESC LIMIT 5;
    ```

-----

## Cài đặt và Chạy (MapReduce)

Phương pháp này sử dụng mã nguồn Java để thực hiện đếm số lượng sản phẩm theo danh mục.

### Cấu trúc thư mục

```
src/
├── AmazonMapper.java   # Logic tách từ và xử lý dòng CSV
├── AmazonReducer.java  # Logic cộng tổng (Aggregation)
└── AmazonDriver.java   # Cấu hình Job
```

### Hướng dẫn Compile & Run

1.  **Compile code**:
    Tạo file `.jar` từ source code (ví dụ: `amazon-analytics.jar`). Đảm bảo đã add thư viện `hadoop-common` và `hadoop-mapreduce-client-core`.

2.  **Chạy Job trên Hadoop**:

    ```bash
    # Cú pháp: hadoop jar <jar_file> <main_class> <input_path> <output_path>

    hadoop jar amazon-analytics.jar AmazonDriver /user/data/amazon/amazon.csv /user/data/amazon_output
    ```

3.  **Kiểm tra kết quả**:

    ```bash
    hdfs dfs -cat /user/data/amazon_output/part-r-00000
    ```

-----

## Kết quả phân tích

Sau khi chạy, hệ thống sẽ trả về:

1.  Danh sách các danh mục sản phẩm và số lượng tương ứng (thông qua MapReduce hoặc Hive Query 1).
2.  Danh sách các sản phẩm có phần trăm giảm giá cao nhất (thông qua Hive Query 2).
3.  Giá trung bình của các sản phẩm chất lượng cao (thông qua Hive Query 3).

<!-- end list -->

```

### Cách sử dụng:
1.  Copy nội dung trên.
2.  Tạo một file mới tên là `README.md` trong thư mục dự án của bạn.
3.  Paste nội dung vào và lưu lại.
```
