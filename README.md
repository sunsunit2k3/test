```python?code_reference&code_event_index=2
import pandas as pd

# Load the dataset to inspect columns and data types
df = pd.read_csv('Supplement_Sales_Weekly_Expanded.csv')
print(df.info())
print(df.head())

```

```text?code_stdout&code_event_index=2
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 4384 entries, 0 to 4383
Data columns (total 10 columns):
 #   Column          Non-Null Count  Dtype  
---  ------          --------------  -----  
 0   Date            4384 non-null   object 
 1   Product Name    4384 non-null   object 
 2   Category        4384 non-null   object 
 3   Units Sold      4384 non-null   int64  
 4   Price           4384 non-null   float64
 5   Revenue         4384 non-null   float64
 6   Discount        4384 non-null   float64
 7   Units Returned  4384 non-null   int64  
 8   Location        4384 non-null   object 
 9   Platform        4384 non-null   object 
dtypes: float64(3), int64(2), object(5)
memory usage: 342.6+ KB
None
         Date  Product Name     Category  Units Sold  Price  Revenue  Discount  Units Returned Location Platform
0  2020-01-06  Whey Protein      Protein         143  31.98  4573.14      0.03               2   Canada  Walmart
1  2020-01-06     Vitamin C      Vitamin         139  42.51  5908.89      0.04               0       UK   Amazon
2  2020-01-06      Fish Oil        Omega         161  12.91  2078.51      0.25               0   Canada   Amazon
3  2020-01-06  Multivitamin      Vitamin         140  16.07  2249.80      0.08               0   Canada  Walmart
4  2020-01-06   Pre-Workout  Performance         157  35.47  5568.79      0.25               3   Canada    iHerb

```

Chào bạn, tôi đã cập nhật lại toàn bộ giải pháp Hadoop MapReduce và Hive để phù hợp với bộ dữ liệu mới `Supplement_Sales_Weekly_Expanded.csv` của bạn.

Dữ liệu này có cấu trúc đơn giản hơn (không bị lỗi dấu phẩy trong nội dung text như file cũ), nên việc xử lý sẽ dễ dàng hơn.

-----

### Phần 1: Cấu trúc dữ liệu mới

Dựa trên phân tích file CSV mới, các cột dữ liệu là:
0\. `Date`

1.  `Product Name`
2.  `Category`
3.  `Units Sold`
4.  `Price`
5.  `Revenue`
6.  `Discount`
7.  `Units Returned`
8.  `Location`
9.  `Platform`

-----

### Phần 2: Giải pháp với Apache Hive

Chúng ta sẽ tạo bảng và thực hiện 3 câu truy vấn mẫu tương tự: Thống kê doanh thu theo danh mục, Top sản phẩm bán chạy, và Phân tích theo Platform.

#### 1\. Tạo bảng Hive

```sql

CREATE EXTERNAL TABLE IF NOT EXISTS sales_raw (
    sale_date STRING,       -- Đổi thành STRING để tránh lỗi NULL date
    product_name STRING,
    category STRING,
    units_sold STRING,      -- Đổi thành STRING
    price STRING,           -- Đổi thành STRING
    revenue STRING,         -- Đổi thành STRING
    discount STRING,        -- Đổi thành STRING
    units_returned STRING,  -- Đổi thành STRING
    location STRING,
    platform STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' -- Dùng thư viện này xử lý tốt hơn dấu phẩy và ngoặc kép
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/user/data/sales'
TBLPROPERTIES ("skip.header.line.count"="1");
-- Lưu ý: Cần bỏ dòng header trong file CSV trước khi upload hoặc dùng tblproperties("skip.header.line.count"="1")
```

#### 2\. Các câu truy vấn phân tích (Queries)

**Yêu cầu 1: Tổng doanh thu và số lượng bán theo từng Danh mục (Category).**

```sql
SELECT 
    category, 
    SUM(units_sold) as total_units, 
    SUM(revenue) as total_revenue 
FROM sales_raw 
GROUP BY category
ORDER BY total_revenue DESC;
```

**Yêu cầu 2: Top 5 sản phẩm có tỷ lệ hoàn trả (Units Returned) cao nhất.**

```sql
SELECT 
    product_name, 
    SUM(units_returned) as total_returned
FROM sales_raw
GROUP BY product_name
ORDER BY total_returned DESC
LIMIT 5;
```

**Yêu cầu 3: Doanh thu trung bình trên mỗi đơn hàng theo từng Nền tảng (Platform).**

```sql
SELECT 
    platform, 
    AVG(revenue) as avg_revenue_per_order
FROM sales_raw
GROUP BY platform;
```

-----

### Phần 3: Giải pháp với Hadoop MapReduce (Java)

Bài toán MapReduce: **"Tính tổng doanh thu (Revenue) cho từng Danh mục (Category)"**.

#### 1\. Mapper Class (`SalesMapper.java`)

Đọc từng dòng, lấy `Category` (cột 2) làm Key và `Revenue` (cột 5) làm Value.

```java
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text category = new Text();
    private DoubleWritable revenue = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // Bỏ qua header nếu dòng bắt đầu bằng "Date"
        if (line.startsWith("Date")) return;

        // Tách dấu phẩy
        String[] fields = line.split(",");

        // Đảm bảo đủ số cột để tránh lỗi IndexOutOfBounds
        if (fields.length > 5) {
            try {
                String catName = fields[2].trim(); // Cột Category
                double rev = Double.parseDouble(fields[5].trim()); // Cột Revenue

                category.set(catName);
                revenue.set(rev);
                
                context.write(category, revenue);
            } catch (NumberFormatException e) {
                // Bỏ qua dòng lỗi định dạng số
            }
        }
    }
}
```

#### 2\. Reducer Class (`SalesReducer.java`)

Cộng dồn các giá trị Revenue cho cùng một Category.

```java
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable totalRevenue = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        totalRevenue.set(sum);
        context.write(key, totalRevenue);
    }
}
```

#### 3\. Driver Class (`SalesDriver.java`)

Cấu hình Job.

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SalesDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sales Revenue Per Category");
        
        job.setJarByClass(SalesDriver.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

-----

### Phần 4: File README.md cập nhật

Bạn có thể dùng nội dung dưới đây cho file `README.md`.

````markdown
# Phân tích Doanh số Bán hàng (Supplement Sales Analysis)

Dự án xử lý tập dữ liệu `Supplement_Sales_Weekly_Expanded.csv` để tính toán doanh thu và hiệu suất bán hàng.

## 1. Setup Dữ liệu
```bash
# Tạo thư mục
hdfs dfs -mkdir -p /user/data/sales

# Upload file
hdfs dfs -put Supplement_Sales_Weekly_Expanded.csv /user/data/sales/
````

## 2\. Chạy với Hive

Dùng Hive để truy vấn SQL nhanh.

**Tạo bảng:**

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS sales_raw (
    date STRING, product_name STRING, category STRING, 
    units_sold INT, price DOUBLE, revenue DOUBLE, 
    discount DOUBLE, units_returned INT, location STRING, platform STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/data/sales'
TBLPROPERTIES ("skip.header.line.count"="1");
```

**Ví dụ truy vấn: Tổng doanh thu theo Category**

```sql
SELECT category, SUM(revenue) FROM sales_raw GROUP BY category;
```

## 3\. Chạy với MapReduce (Java)

Dùng để tính tổng doanh thu theo Category (Batch Processing).

**Build & Run:**

1.  Compile code Java ra file `sales-analytics.jar`.
2.  Chạy lệnh:
    ```bash
    hadoop jar sales-analytics.jar SalesDriver /user/data/sales/Supplement_Sales_Weekly_Expanded.csv /user/data/sales_output
    ```
3.  Xem kết quả:
    ```bash
    hdfs dfs -cat /user/data/sales_output/*
    ```

<!-- end list -->

```
```
