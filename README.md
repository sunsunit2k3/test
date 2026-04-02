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
    sale_date STRING,
    product_name STRING,
    category STRING,
    units_sold STRING,
    price STRING,
    revenue STRING,
    discount STRING,
    units_returned STRING,
    location STRING,
    platform STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/user/data/Processfile1.txt'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Lưu ý: Cần bỏ dòng header trong file CSV trước khi upload hoặc dùng tblproperties("skip.header.line.count"="1")
```

package com.example.hadoop;

import com.example.hadoop.CSVUtils;
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

            String product = cols.get(productIdx).trim();

            // Clean revenue (remove $, commas, spaces…)
            String revenueClean = cols.get(revenueIdx).replaceAll("[^0-9.\\-]", "");

            double revenue = 0.0;

            try {
                if (!revenueClean.isEmpty()) {
                    revenue = Double.parseDouble(revenueClean);
                }
            } catch (Exception e) {
                revenue = 0.0;
            }

            context.write(new Text(product), new DoubleWritable(revenue));
        }
    }
}
