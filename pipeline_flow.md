# Ngữ cảnh : 
Xây dựng 1 app xử lý dữ liệu từ file excel báo cáo quảng cáo Amazon Ads, mục đích hướng tới là đưa file vào input, output sẽ có đủ gợi ý chỉnh sửa hoặc chỉnh sửa rồi, đưa vào template của amz ads seller center để tải lên. Xa hơn nữa thì kết nối trực tiếp các API để xử lý
Hiện tại : Đang trong giai đoạn xử lý và kết nối dữ liệu, đồng thời test các tính năng của app.
Cần test thêm các luật của hiện tại, và tối ưu hóa các tính năng của app.
# Các folder hiện có:
1. data/input_queue : Nơi chứa các file excel báo cáo quảng cáo Amazon Ads
2. data/processed : Nơi chứa các file excel báo cáo quảng cáo Amazon Ads đã được đưa vào thư viện lịch sử
3. data/historical : Nơi chứa các file excel báo cáo quảng cáo Amazon Ads đã được lưu vào lịch sử
4. data/output : Nơi chứa các file excel báo cáo quảng cáo Amazon Ads đã được xử lý

# Các định nghĩa thông số hiện tại:
Bên dưới là bộ quy tắc và ngưỡng (Thresholds) đang được áp dụng trực tiếp trong mã nguồn (`polars_executor.py` & `db_engine.py`):

### 1. Công thức Chỉ số Cơ bản (Metrics)
* **Impressions** : Số lần hiển thị quảng cáo
* Impressions được coi là thấp khi < 1000, trung bình khi 1000 <= Impressions < 3000, cao khi >= 3000
* **Clicks** : Số lần nhấp vào quảng cáo
* **Spend** : Số tiền đã chi tiêu cho quảng cáo
* **Sales** : Số tiền đã thu được từ quảng cáo
* **Orders** : Số đơn hàng đã thu được từ quảng cáo
* **Units** : Số sản phẩm đã bán được từ quảng cáo
* **Conversion Rate** : Tỷ lệ chuyển đổi từ quảng cáo
* CR được coi là thấp khi < 5%, trung bình khi 5% <= CR < 12%, cao khi >= 12%
* **ACoS (Advertising Cost of Sales)** : Tỷ lệ chi phí quảng cáo trên doanh thu
* ACoS được coi là thấp khi < 30%, trung bình khi 30% <= ACoS < 40%, cao khi >= 40%
* **CPC (Cost Per Click)** : Chi phí trung bình cho mỗi lần nhấp vào quảng cáo
* **ROAS (Return On Ad Spend)** : Tỷ lệ doanh thu trên chi phí quảng cáo
* **CTR (Click-Through Rate):** `Clicks / Impressions` (Chỉ số thu hút của hình ảnh/tiêu đề sản phẩm).
* CTR được coi là thấp khi < 0.3%, trung bình khi 0.3% <= CTR < 1%, cao khi >= 1%
*   **CR (Conversion Rate):** `Orders / Clicks` (Chỉ số sức mạnh chốt đơn của nội dung mô tả, A+ Content, hoặc Giá).
*   **ACoS (Advertising Cost of Sales):** `Spend / Sales` (Hệ số cảnh báo lỗ/lãi của quảng cáo).

### 2. Bộ Luật Đánh Giá Từ Khóa (Keyword / Product Targeting)
Hệ thống sử dụng **Cây quyết định (Decision Tree)** để gán nhãn theo thứ tự ưu tiên:
*   **Luật Tàng Hình:** `Impressions == 0`. Từ khóa đang chạy nhưng Amazon không thèm phân phối hiển thị.
*   **Luật Thiếu Dữ Liệu:** `0 < Impressions < 1000`. Chưa đủ mẫu số khách hàng để kết luận là tốt hay xấu, cần theo dõi thêm.
*   **Luật CTR Thấp:** `Impressions >= 1000` VÀ `CTR < 0.3%`. Khách thấy nhiều nhưng lướt qua, có thể ảnh xấu hoặc thứ hạng tự nhiên đã đứng top không cần click quảng cáo.
*   **Luật CR Thấp:** `CTR >= 0.3%` VÀ `CR < 5%`. Khách vào xem rất nhiều nhưng thoát ra, cảnh báo giá đắt hoặc kém đối thủ.
*   **Luật Lỗ Nặng:** `ACoS > 50%`. Quảng cáo nuốt hết lợi nhuận, bắt buộc giảm Bid.

### 3. Luật Phong "SIÊU SAO" (Winner Keywords)
Từ khóa được gợi ý TĂNG BID (Vít Bid) khi thỏa mãn ĐỒNG THỜI 4 điều kiện cực gắt:
*   Độ phủ tốt: `Impressions >= 3000`
*   Thu hút tốt: `CTR >= 1%`
*   Chốt đơn tốt: `CR >= 8%`
*   Chi phí rẻ: `ACoS <= 40%`

### 4. Luật Đánh Giá Chiến Dịch (Campaign)
*   **Chiến dịch Thảm Họa (Cần Cách Ly/Làm lại):** `Clicks > 20` VÀ `Orders == 0`. Tiêu tiền vào 20 khách rồi nhưng không thu về được lệnh mua nào.
*   **Chiến dịch Yếu:** Chiến dịch không chứa bất kỳ một từ khóa "Siêu sao" nào gánh team.
*   **Chiến dịch Tuyệt Đối (Tốt):** Chiến dịch sạch bóng không chứa bất kỳ cụm từ khóa mục tiêu nào vi phạm các luật "Yếu" ở trên.

# 🚀 Luồng Hoạt Động (Pipeline) Hệ Thống Amazon Ads

Quy trình này được vận hành hoàn toàn tự động thông qua file "nhạc trưởng" là `pipeline.py`. 
Dưới đây là **5 bước** mà hệ thống tự động chạy từ lúc bạn ném file vào cho đến lúc ra báo cáo:

## Bước 1: Quét và Thu thập dữ liệu (`bulk_import.py`)
Khi bạn chạy `chay_pipeline.bat`, việc đầu tiên hệ thống làm là nhìn vào thư mục 📂 `data/import_queue`:
*   Nó sẽ gom tất cả các file Excel bạn vừa thả vào đó.
*   Trích xuất ngày tháng từ tên file (ví dụ: `20240401_ads.xlsx`).
*   **Lưu trữ:** Gửi dữ liệu vào kho Parquet (Bước 2) để làm "tài sản lịch sử".
*   **Dọn dẹp:** Chuyển file gốc sang 📂 `data/processed` để hệ thống gọn gàng.
*   **Chọn Lọc:** Tìm ra file có ngày **mới nhất** (ví dụ file báo cáo của ngày gần đây nhất) để mang đi phân tích chiến lược. Nếu thư mục hàng đợi trống, nó sẽ đi tìm file `report.xlsx` trong 📂 `data/input` để dự phòng.

## Bước 2: Chuẩn hóa & Nén rạc thành Lịch sử (`ingest_engine.py` & `storage_engine.py`)
*   File báo cáo mới nhất được nạp vào bộ nhớ siêu tốc bằng Engine Polars tích hợp Calamine.
*   Đi qua màng lọc y tế **`pandera_schema.py`** để chữa bệnh: 
    *   Sửa tên cột về chuẩn quốc tế thống nhất (ví dụ: `Campaign Id` -> `Campaign ID`).
    *   Phòng vệ vững chắc cho các dãy số ID dài để không bị Excel bóp méo thành mã số thập phân khoa học (Ví dụ số sẽ không thành `1.23E+10`).
    *   Tự động phát hiện và bù đắp các cột bị thiếu bằng giá trị ô trống (Null).
*   Sau khi "sạch sẽ", dữ liệu đó được nén thu nhỏ bằng công nghệ Snappy thành đuôi `.parquet`. Các file file nén này được phân loại cất kỹ theo cấu trúc cây thư mục `Năm/Tháng/Ngày` trong phân khu 📂 `data/historical`.

## Bước 3: Đánh giá Lịch sử & Vẽ Bức tranh toàn cảnh (`db_engine.py` cùng DuckDB)
Đây là bước hệ thống tự tổng hợp lại lịch sử để theo dõi phong độ tráng thái chung. Cỗ máy siêu tốc DuckDB sẽ gộp tất cả các mảnh `.parquet` kể trên để tạo một bản báo cáo Đa chiều:
*   Kẻ bảng thống kê xu hướng Chi tiêu (Spend) vs Tổng Doanh thu (Sales) qua từng chu kỳ.
*   Thực hiện phân loại Tốt, Trung Bình, Yếu để đếm tính phần trăm dành vẽ kết cấu Biểu đồ Tròn (Pie chart).
*   Sản xuất ra các sheet dữ liệu chi tiết đặc biệt (Ví dụ "Từ khóa Khỏe", "Campaign Tốt") kết hợp "Dò tên" (ID Lookup) nên trong kết quả ID đi đôi với các đoạn Tên thật của chiến dịch mà sẽ không bị rỗng ở cột `Campaign Name` được nữa.

## Bước 4: Khám bệnh & Đưa Cảnh Báo (`polars_executor.py` cùng Polars)
Đây là "bộ não AI đề xuất" phân tích chi tiết cấp tiến của hệ thống. Máy sẽ bóc tách riêng cái file chứa ngày giờ gần nhất để xử lý:
*   Tính ngay các chỉ số sinh tồn và phái sinh: **CTR** (tỷ lệ nhấp hiển thị), **CR** (tỷ lệ chốt/mua), **ACoS** (chi phí trừ hao / tỷ lệ đốt tiền).
*   Thực thi bộ máy **Điều Phối Các Luật Phân Nhóm Cảnh Báo** để dán nhãn chiến lược vào file:
    *   Chiến dịch nếu đã ngốn vượt 20 Clicks nhưng doanh số không bứt được = 0 đơn -> gắn nhãn 🛑 **Cần Làm Lại (Campaign lỗi)**.
    *   Từ khóa hoàn hảo đạt `Imps >= 3000`, `CTR >= 1%`, `CR >= 8%`, `ACoS <= 40%` -> gắn phong vàng 💎 **Từ Khóa Siêu Sao (Tăng Bid ngay)**.
    *   Từ khóa đạt ACoS quá dở tệ > 50% -> gắn nhãn ⚠️ **Lỗ Nặng**.
*   Sàng lọc triệt để các list từ khoá cần can thiệp để xếp vào từng sheet nhỏ.

## Bước 5: Đóng gói & Xuất xưởng File Đặc Tuyến (`output_engine.py`)
Mọi thành quả thu thập từ "Bức tranh toàn cảnh" (Bước 3) và "Kê đơn bệnh chiến lược" (Bước 4) sẽ được đúc thành thành phẩm duy nhất:
*   Gọi robot định dạng thư viện Excel `xlsxwriter` để xử lý ra file **`Final_Analyzed_Report.xlsx`** đặt trong ngăn 📂 `data/output`.
*   Tự động lên màu nổi, kẻ ô đẹp mắt, và vẽ cả sơ đồ Đường, sơ đồ Cột kép và sơ đồ Tròn thể hiện tất cả trong duy nhất một tờ "Dashboard Lịch sử".

---

💡 **LUỒNG THAO TÁC CỦA BẠN (NGƯỜI DÙNG) ĐƠN GIẢN HÓA VỀ 3 BƯỚC:**
1. Rải tất cả file báo cáo mới tải từ tài khoản Amazon Ads vào 📂 `data/import_queue`.
2. Bấm Click đúp vào file kịch bản `chay_pipeline.bat`.
3. Đi mở tủ lạnh lấy nước uống 5 giây sau quay lại, lấy file bản vẽ chiến lược và list từ khóa điều chỉnh giá ở trong 📂 `data/output`. Chấm hết!
