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
// File: com/example/hadoop/common/CSVUtils.java
package com.example.hadoop.common;

import java.util.ArrayList;
import java.util.List;

public class CSVUtils {
    // Simple CSV parser supporting quoted fields
    public static List<String> parseLine(String line) {
        List<String> result = new ArrayList<>();
        if (line == null || line.isEmpty()) return result;
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"'); // escaped quote
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                result.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        result.add(cur.toString());
        return result;
    }
}

// ======================================================
// File: com/example/hadoop/filter/FilterMapper.java
package com.example.hadoop.filter;

import com.example.hadoop.common.CSVUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String[] headers = null;
    private String filterColumn = null;
    private String filterValue = null;
    private int filterIdx = -1;
    private boolean skipHeader = true;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        filterColumn = context.getConfiguration().get("filter.column");
        filterValue = context.getConfiguration().get("filter.value");
        String headerLine = context.getConfiguration().get("input.header.line");
        if (headerLine != null) {
            headers = CSVUtils.parseLine(headerLine).toArray(new String[0]);
            for (int i = 0; i < headers.length; i++) {
                if (headers[i].trim().equalsIgnoreCase(filterColumn)) {
                    filterIdx = i;
                    break;
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Skip header if configured
        if (skipHeader && key.get() == 0 && line.toLowerCase().contains("date") && line.toLowerCase().contains("product")) {
            return;
        }
        List<String> cols = CSVUtils.parseLine(line);
        if (filterIdx >= 0 && filterIdx < cols.size()) {
            String v = cols.get(filterIdx);
            if (v != null && v.equalsIgnoreCase(filterValue)) {
                context.write(new Text("match"), new Text(line));
            }
        }
    }
}

// ======================================================
// File: com/example/hadoop/filter/FilterDriver.java
package com.example.hadoop.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FilterDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: FilterDriver <input> <output> <filterColumn> <filterValue> <headerLine>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        conf.set("filter.column", args[2]);
        conf.set("filter.value", args[3]);
        conf.set("input.header.line", args[4]);

        Job job = Job.getInstance(conf, "CSV Filter Job");
        job.setJarByClass(FilterDriver.class);

        job.setMapperClass(FilterMapper.class);
        job.setNumReduceTasks(0); // mapper-only job outputs matching lines

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem.get(conf).delete(new Path(args[1]), true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

// ======================================================
// File: com/example/hadoop/revenue/RevenueMapper.java
package com.example.hadoop.revenue;

import com.example.hadoop.common.CSVUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class RevenueMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private String[] headers = null;
    private int productIdx = -1;
    private int revenueIdx = -1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String headerLine = context.getConfiguration().get("input.header.line");
        if (headerLine != null) {
            headers = CSVUtils.parseLine(headerLine).toArray(new String[0]);
            for (int i = 0; i < headers.length; i++) {
                String h = headers[i].trim().toLowerCase();
                if (h.equals("product name") || h.equals("product_name") || h.equals("product")) productIdx = i;
                if (h.equals("revenue")) revenueIdx = i;
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.toLowerCase().startsWith("date,")) return; // skip header
        List<String> cols = CSVUtils.parseLine(line);
        if (productIdx >= 0 && revenueIdx >= 0 && productIdx < cols.size() && revenueIdx < cols.size()) {
            String product = cols.get(productIdx);
            String revenueStr = cols.get(revenueIdx).replaceAll("[^"]+" , "");
            // try parsing revenue safely
            double revenue = 0.0;
            try {
                revenue = Double.parseDouble(cols.get(revenueIdx));
            } catch (Exception e) {
                // try removing currency symbols
                String cleaned = cols.get(revenueIdx).replaceAll("[^0-9.\\-]", "");
                if (!cleaned.isEmpty()) {
                    try { revenue = Double.parseDouble(cleaned); } catch (Exception ex) { revenue = 0.0; }
                }
            }
            context.write(new Text(product), new DoubleWritable(revenue));
        }
    }
}

// ======================================================
// File: com/example/hadoop/revenue/RevenueReducer.java
package com.example.hadoop.revenue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RevenueReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable v : values) sum += v.get();
        context.write(key, new DoubleWritable(sum));
    }
}

// ======================================================
// File: com/example/hadoop/revenue/RevenueDriver.java
package com.example.hadoop.revenue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RevenueDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: RevenueDriver <input> <output> <headerLine>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        conf.set("input.header.line", args[2]);

        Job job = Job.getInstance(conf, "Revenue Sum per Product");
        job.setJarByClass(RevenueDriver.class);

        job.setMapperClass(RevenueMapper.class);
        job.setReducerClass(RevenueReducer.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileSystem.get(conf).delete(new Path(args[1]), true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

// ======================================================
// File: com/example/hadoop/returns/ReturnsMapper.java
package com.example.hadoop.returns;

import com.example.hadoop.common.CSVUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class ReturnsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private int productIdx = -1;
    private int unitsReturnedIdx = -1;
    private String[] headers = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String headerLine = context.getConfiguration().get("input.header.line");
        if (headerLine != null) {
            headers = CSVUtils.parseLine(headerLine).toArray(new String[0]);
            for (int i = 0; i < headers.length; i++) {
                String h = headers[i].trim().toLowerCase();
                if (h.equals("product name") || h.equals("product_name") || h.equals("product")) productIdx = i;
                if (h.equals("units returned") || h.equals("units_returned")) unitsReturnedIdx = i;
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.toLowerCase().startsWith("date,")) return;
        List<String> cols = CSVUtils.parseLine(line);
        if (productIdx >= 0 && unitsReturnedIdx >= 0 && productIdx < cols.size() && unitsReturnedIdx < cols.size()) {
            String product = cols.get(productIdx);
            int returned = 0;
            try { returned = Integer.parseInt(cols.get(unitsReturnedIdx)); } catch (Exception e) { returned = 0; }
            context.write(new Text(product), new IntWritable(returned));
        }
    }
}

// ======================================================
// File: com/example/hadoop/returns/ReturnsReducer.java
package com.example.hadoop.returns;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReturnsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values) sum += v.get();
        context.write(key, new IntWritable(sum));
    }
}

// ======================================================
// File: README_RUN.txt
/*
Usage examples (after building a JAR with these classes):

1) Filter rows where Category == "Supplements":
   hadoop jar yourjar.jar com.example.hadoop.filter.FilterDriver \
      /input/path /output/filter Category Supplements "Date,Product Name,Category,Units Sold,Price,Revenue,Discount,Units Returned,Location,Platform"

2) Sum revenue per product:
   hadoop jar yourjar.jar com.example.hadoop.revenue.RevenueDriver \
      /input/path /output/revenue "Date,Product Name,Category,Units Sold,Price,Revenue,Discount,Units Returned,Location,Platform"

3) Count returned units per product:
   hadoop jar yourjar.jar com.example.hadoop.returns.ReturnsDriver \
      /input/path /output/returns "Date,Product Name,Category,Units Sold,Price,Revenue,Discount,Units Returned,Location,Platform"

Notes:
- Each driver expects the CSV header line as an argument so the mapper can locate fields by name.
- You can extend the parsers to support different delimiters or additional cleanup of numeric fields.
- Consider adding a Combiner for revenue and returns to reduce network traffic.
- For performance, convert CSV to SequenceFile or Parquet and use Hadoop MapReduce or Spark for larger datasets.
*/


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
