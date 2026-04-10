import duckdb                     # Thư viện SQL OLAP siêu nhanh để truy vấn file Parquet
import os                         # Thư viện thao tác hệ thống tệp
import shutil                     # Thư viện thao tác file/thư mục (dùng để xóa toàn bộ DB)
import polars as pl                # Thư viện xử lý DataFrame (vật chứa dữ liệu sau khi truy vấn)
import sys                        # Thư viện hệ thống

# Cấu hình Terminal để hiển thị đúng tiếng Việt và Emoji
sys.stdout.reconfigure(encoding='utf-8')

# Import đường dẫn kho dữ liệu từ storage_engine
from storage_engine import HISTORICAL_DB_PATH

class DuckDBEngine:
    """
    Cỗ máy DuckDB: Đóng vai trò là 'Bộ não' truy vấn dữ liệu lịch sử.
    Nhiệm vụ: Gộp hàng trăm file Parquet lại và trích xuất xu hướng, phân loại.
    """
    def __init__(self):
        # Khởi tạo kết nối DuckDB chạy trên RAM (In-memory) để đảm bảo tốc độ tối đa
        self.con = duckdb.connect(database=':memory:')
        
    def create_unified_view(self) -> bool:
        """
        Kỹ thuật 'Read-Virtualization':
        Gộp tất cả các file Parquet nằm rải rác trong các thư mục Năm/Tháng/Ngày thành 1 View duy nhất.
        """
        # Tạo đường dẫn quét mọi file đuôi .parquet trong toàn bộ cây thư mục historical
        search_path = os.path.join(HISTORICAL_DB_PATH, "**", "*.parquet")
        # Chuẩn hóa dấu gạch chéo cho DuckDB (quy chuẩn SQL)
        search_path = search_path.replace("\\", "/")
        
        try:
            # Lệnh SQL mạnh mẽ: read_parquet với tham số union_by_name=true
            # Giúp nạp dữ liệu ngay cả khi các file cũ thiếu cột so với file mới (Schema Evolution)
            self.con.execute(f"""
                CREATE OR REPLACE VIEW unified_view AS 
                SELECT * FROM read_parquet('{search_path}', union_by_name=true)
            """)
            print(f"✅ [DUCKDB] Đã móc nối thành công View Lịch sử (Unified View).")
            return True
        except Exception as e:
            # Nếu folder trống (chưa có file nào), sẽ báo lỗi ở đây
            print(f"⚠️ [DUCKDB] Không thể khởi tạo View lịch sử. Lỗi: {e}")
            return False

    def get_latest_metrics(self) -> pl.DataFrame:
        """
        Lấy trạng thái 'Sống' mới nhất của từng thực thể (Campaign/Keyword).
        Đây là cách giải quyết triệt để vấn đề 'Tử huyệt 1' (Dữ liệu bị lặp do nạp nhiều lần).
        """
        # Kiểm tra nếu chưa có View thì phải tạo trước khi truy vấn
        if 'unified_view' not in [x[0] for x in self.con.execute("SHOW TABLES").fetchall()]:
             if not self.create_unified_view():
                  return pl.DataFrame() # Trả về bảng rỗng nếu ko có dữ liệu
        
        print("⏳ [DUCKDB] Đang nội soi dòng thời gian, chiết xuất dữ liệu Mới Nhất...")
        
        # Dùng kỹ thuật Window Function (ROW_NUMBER) để tóm lấy dòng có ngày mới nhất (rn=1)
        # của từng bộ ID (Campaign Id, Ad Group Id, Keyword Id...)
        query = """
        WITH RankedData AS (
            SELECT *,
                   ROW_NUMBER() OVER(
                       PARTITION BY "Entity", "Campaign ID", "Ad Group ID", "Keyword ID", "Product Targeting ID" 
                       ORDER BY "Report Date" DESC
                   ) as rn
            FROM unified_view
        )
        SELECT * EXCLUDE(rn)
        FROM RankedData
        WHERE rn = 1
        """
        
        try:
             # Bắn kết quả trực tiếp ra Polars DataFrame
             df_latest = self.con.execute(query).pl()
             print(f"✅ [DUCKDB] Trích xuất thành công {df_latest.height} bản ghi mới nhất.")
             return df_latest
        except Exception as e:
             print(f"❌ [DUCKDB] Lỗi truy xuất dữ liệu: {e}")
             return pl.DataFrame()
             
    def reset_database(self) -> bool:
        """Hàm dọn dẹp: Xóa sạch toàn bộ thư mục chứa file Parquet."""
        if os.path.exists(HISTORICAL_DB_PATH):
            try:
                shutil.rmtree(HISTORICAL_DB_PATH) # Xóa toàn bộ cây thư mục
                print("🚨 [DUCKDB] Đã dọn dẹp sạch sẽ kho dữ liệu Lịch Sử!")
                return True
            except Exception as e:
                print(f"❌ [DUCKDB] Lỗi xóa dữ liệu Lịch sử: {e}")
                return False
        return True

    def get_historical_summary(self) -> pl.DataFrame:
        """Tính toán tổng số (Spend, Sales, Clicks) theo từng Ngày báo cáo để vẽ biểu đồ Trend."""
        if 'unified_view' not in [x[0] for x in self.con.execute("SHOW TABLES").fetchall()]:
             if not self.create_unified_view():
                  return pl.DataFrame()
                  
        # SQL: Gộp nhóm theo ngày (Group By) và tính tổng các chỉ số cốt lõi
        # Ép kiểu 'Report Date' về DATE để đồng nhất định dạng trên Dashboard Excel
        query = """
        SELECT CAST("Report Date" AS DATE) as "Report Date", 
               SUM(CAST(COALESCE(NULLIF(CAST("Spend" AS VARCHAR), 'nan'), '0') AS FLOAT)) as "Total Spend", 
               SUM(CAST(COALESCE(NULLIF(CAST("Sales" AS VARCHAR), 'nan'), '0') AS FLOAT)) as "Total Sales", 
               SUM(CAST(COALESCE(NULLIF(CAST("Clicks" AS VARCHAR), 'nan'), '0') AS INT)) as "Total Clicks",
               SUM(CAST(COALESCE(NULLIF(CAST("Orders" AS VARCHAR), 'nan'), '0') AS INT)) as "Total Orders"
        FROM unified_view
        WHERE "Entity" = 'Campaign'
        GROUP BY CAST("Report Date" AS DATE)
        ORDER BY "Report Date" ASC
        """
        try:
             df_summary = self.con.execute(query).pl()
             print(f"✅ [DUCKDB] Đã lấy thành công xu hướng thời gian ({df_summary.height} mốc thời gian).")
             return df_summary
        except Exception as e:
             print(f"❌ [DUCKDB] Lỗi truy xuất summary: {e}")
             return pl.DataFrame()

    def get_keyword_campaign_classification(self) -> dict:
        """
        Phân loại Phức hợp: Phân nhóm KW/Camp theo tiêu chuẩn nghiệp vụ (Tốt/Yếu/TB).
        Đây là logic quan trọng để vẽ 2 Biểu Đồ Tròn trên Dashboard Excel.
        """
        if 'unified_view' not in [x[0] for x in self.con.execute("SHOW TABLES").fetchall()]:
            if not self.create_unified_view():
                return {"keywords": pl.DataFrame(), "campaigns": pl.DataFrame()}

        # CTE SQL phức tạp để phân loại Keywords dựa trên 4 chỉ số (Imps, CTR, CR, ACoS)
        query = """
        WITH LatestKeywords AS (
            SELECT *,
                   ROW_NUMBER() OVER(
                       PARTITION BY "Campaign ID", "Ad Group ID", "Keyword ID"
                       ORDER BY "Report Date" DESC
                   ) as rn
            FROM unified_view
            WHERE "Entity" IN ('Keyword', 'Product Targeting')
              AND "Keyword ID" IS NOT NULL
        ),
        KW AS (
            -- Ép kiểu dữ liệu an toàn để tránh lỗi tính toán
            SELECT 
                "Campaign ID", "Keyword ID",
                TRY_CAST("Impressions" AS INTEGER)  AS imps,
                TRY_CAST("Clicks"      AS INTEGER)  AS clicks,
                TRY_CAST("Orders"      AS INTEGER)  AS orders,
                TRY_CAST("Spend"       AS FLOAT)    AS spend,
                TRY_CAST("Sales"       AS FLOAT)    AS sales,
                -- Tính CTR, CR, ACoS tại chỗ bằng SQL
                CASE WHEN TRY_CAST("Impressions" AS INTEGER) > 0 THEN TRY_CAST("Clicks" AS INTEGER) * 1.0 / TRY_CAST("Impressions" AS INTEGER) ELSE NULL END AS ctr,
                CASE WHEN TRY_CAST("Clicks" AS INTEGER) > 0 THEN TRY_CAST("Orders" AS INTEGER) * 1.0 / TRY_CAST("Clicks" AS INTEGER) ELSE NULL END AS cr,
                CASE WHEN TRY_CAST("Sales" AS FLOAT) > 0 THEN TRY_CAST("Spend" AS FLOAT) / TRY_CAST("Sales" AS FLOAT) ELSE NULL END AS acos
            FROM LatestKeywords WHERE rn = 1
        ),
        KW_Classified AS (
            -- Áp dụng luật IF/ELSE để gán nhãn Yếu/Khỏe/Trung Bình
            SELECT
                "Campaign ID", "Keyword ID",
                CASE
                    -- Luật KW YẾU: Thất bại ở cả 4 mặt trận chỉ số
                    WHEN (ctr IS NULL OR ctr < 0.003) AND (cr IS NULL OR cr < 0.05) AND (imps IS NULL OR imps < 1000) AND (acos IS NULL OR acos > 0.40) THEN 'Yếu'
                    -- Luật KW KHỎE: Thành công rực rỡ ở cả 4 mặt trận
                    WHEN ctr >= 0.006 AND cr >= 0.15 AND imps >= 3000 AND acos IS NOT NULL AND acos < 0.30 THEN 'Khỏe'
                    ELSE 'Trung bình'
                END AS kw_class
            FROM KW
        ),
        KW_Summary AS (
            -- Đếm tổng số từ khóa của mỗi nhóm để vẽ biểu đồ tròn KW
            SELECT kw_class AS "Phân loại", COUNT(*) AS "Số lượng" FROM KW_Classified GROUP BY kw_class
        ),
        Camp_Has AS (
            -- Kiểm tra xem trong mỗi Campaign có chứa hầm 'Yếu' hay mỏ 'Vàng' (Khỏe) nào ko
            SELECT "Campaign ID", MAX(CASE WHEN kw_class = 'Yếu' THEN 1 ELSE 0 END) AS has_weak, MAX(CASE WHEN kw_class = 'Khỏe' THEN 1 ELSE 0 END) AS has_strong
            FROM KW_Classified GROUP BY "Campaign ID"
        ),
        Camp_Classified AS (
            -- Phân loại Campaign dựa trên chất lượng 'Quân lính' (Keyword) bên trong
            SELECT
                CASE
                    WHEN has_weak = 0 THEN 'Tốt'        -- Chiến dịch sạch bóng từ khóa yếu
                    WHEN has_strong = 0 THEN 'Yếu'      -- Chiến dịch ko có lấy 1 từ khóa khỏe
                    ELSE 'Trung bình'                    -- Có cả trung bình, khỏe, yếu lẫn lộn
                END AS camp_class
            FROM Camp_Has
        ),
        Camp_Summary AS (
            -- Đếm tổng số Campaign mỗi nhóm để vẽ biểu đồ tròn Camp
            SELECT camp_class AS "Phân loại", COUNT(*) AS "Số lượng" FROM Camp_Classified GROUP BY camp_class
        )
        -- Trả về 2 bảng kết quả cùng lúc bằng UNION
        SELECT 'keyword' AS type, * FROM KW_Summary
        UNION ALL
        SELECT 'campaign' AS type, * FROM Camp_Summary
        """

        try:
            df_all = self.con.execute(query).pl()
            # Tách bảng tổng thành 2 bảng riêng cho nghiệp vụ
            df_keywords  = df_all.filter(pl.col("type") == "keyword").drop("type")
            df_campaigns = df_all.filter(pl.col("type") == "campaign").drop("type")
            print(f"✅ [DUCKDB] Phân loại xong: Keywords {df_keywords.to_dict(as_series=False)} | Campaigns {df_campaigns.to_dict(as_series=False)}")
            return {"keywords": df_keywords, "campaigns": df_campaigns}
        except Exception as e:
            print(f"❌ [DUCKDB] Lỗi phân loại keyword/campaign: {e}")
            return {"keywords": pl.DataFrame(), "campaigns": pl.DataFrame()}

    def get_classified_detail_sheets(self) -> dict:
        """
        Hàm cung cấp dữ liệu chi tiết (tên Campaign, nội dung Keyword...) cho các sheet phụ ở cuối file Excel.
        Dành cho việc xử lý thủ công của trader (Ví dụ: Mở sheet 'Từ khóa yếu' để giảm bid hàng loạt).
        """
        if 'unified_view' not in [x[0] for x in self.con.execute("SHOW TABLES").fetchall()]:
            if not self.create_unified_view():
                return {}

        # SQL Query tổng để lấy list Keyword sạch (ngày mới nhất) kèm dán nhãn
        # KỸ THUẬT QUAN TRỌNG: Dùng Cross-Lookup để lấy Tên (Name) từ ID nếu bị thiếu
        base_query = """
        WITH CampNames AS (
            -- Lấy tên mới nhất cho Camp ID
            SELECT "Campaign ID", "Campaign Name", ROW_NUMBER() OVER(PARTITION BY "Campaign ID" ORDER BY "Report Date" DESC) as rn
            FROM unified_view WHERE "Campaign Name" IS NOT NULL AND "Campaign Name" != ''
        ),
        AdGNames AS (
            -- Lấy tên mới nhất cho Ad Group ID
            SELECT "Ad Group ID", "Ad Group Name", ROW_NUMBER() OVER(PARTITION BY "Ad Group ID" ORDER BY "Report Date" DESC) as rn
            FROM unified_view WHERE "Ad Group Name" IS NOT NULL AND "Ad Group Name" != ''
        ),
        LatestKW AS (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY "Campaign ID", "Ad Group ID", "Keyword ID" ORDER BY "Report Date" DESC) as rn
            FROM unified_view
            WHERE "Entity" IN ('Keyword', 'Product Targeting') AND "Keyword ID" IS NOT NULL
        ),
        KW AS (
            SELECT
                COALESCE(L."Campaign Name", C."Campaign Name") AS "Campaign Name", 
                L."Campaign ID", 
                COALESCE(L."Ad Group Name", A."Ad Group Name") AS "Ad Group Name", 
                L."Ad Group ID", 
                L."Keyword ID", L."Keyword Text", L."Match Type", L."State", 
                CAST(L."Report Date" AS DATE) AS "Report Date",
                TRY_CAST(L."Impressions" AS INTEGER)  AS "Impressions",
                TRY_CAST(L."Clicks"      AS INTEGER)  AS "Clicks",
                TRY_CAST(L."Orders"      AS INTEGER)  AS "Orders",
                ROUND(TRY_CAST(L."Spend" AS FLOAT), 2) AS "Spend ($)",
                ROUND(TRY_CAST(L."Sales" AS FLOAT), 2) AS "Sales ($)",
                -- Tính toán lại tỷ lệ % (Làm tròn 1 chữ số theo phản hồi người dùng)
                CASE WHEN TRY_CAST(L."Impressions" AS INTEGER) > 0 THEN ROUND(TRY_CAST(L."Clicks" AS INTEGER) * 100.0 / TRY_CAST(L."Impressions" AS INTEGER), 1) ELSE NULL END AS "CTR (%)",
                CASE WHEN TRY_CAST(L."Clicks" AS INTEGER) > 0 THEN ROUND(TRY_CAST(L."Orders" AS INTEGER) * 100.0 / TRY_CAST(L."Clicks" AS INTEGER), 1) ELSE NULL END AS "CR (%)",
                CASE WHEN TRY_CAST(L."Sales" AS FLOAT) > 0 THEN ROUND(TRY_CAST(L."Spend" AS FLOAT) * 100.0 / TRY_CAST(L."Sales" AS FLOAT), 1) ELSE NULL END AS "ACoS (%)",
                -- Logic phân loại
                CASE
                    WHEN (TRY_CAST(L."Impressions" AS INTEGER) < 1000) AND (TRY_CAST(L."Clicks" AS INTEGER)*1.0/NULLIF(TRY_CAST(L."Impressions" AS INTEGER),0) < 0.003) 
                         AND (TRY_CAST(L."Orders" AS INTEGER)*1.0/NULLIF(TRY_CAST(L."Clicks" AS INTEGER),0) < 0.05) AND (TRY_CAST(L."Spend" AS FLOAT)/NULLIF(TRY_CAST(L."Sales" AS FLOAT),0) > 0.40) THEN 'Yeu'
                    WHEN TRY_CAST(L."Impressions" AS INTEGER) >= 3000 AND TRY_CAST(L."Clicks" AS INTEGER)*1.0/NULLIF(TRY_CAST(L."Impressions" AS INTEGER),0) >= 0.006 
                         AND TRY_CAST(L."Orders" AS INTEGER)*1.0/NULLIF(TRY_CAST(L."Clicks" AS INTEGER),0) >= 0.15 AND TRY_CAST(L."Spend" AS FLOAT)/NULLIF(TRY_CAST(L."Sales" AS FLOAT),0) < 0.30 THEN 'Khoe'
                    ELSE 'Trung binh'
                END AS kw_class
            FROM LatestKW L
            LEFT JOIN CampNames C ON L."Campaign ID" = C."Campaign ID" AND C.rn = 1
            LEFT JOIN AdGNames A ON L."Ad Group ID" = A."Ad Group ID" AND A.rn = 1
            WHERE L.rn = 1
        )
        SELECT * FROM KW
        """

        try:
            # Lấy toàn bộ từ khóa đã phân loại bằng SQL
            df_all_kw = self.con.execute(base_query).pl()

            # Lọc danh sách Từ khóa Yếu (Dành cho sheet '⚠️ Từ Khóa Yếu')
            df_kw_weak   = df_all_kw.filter(pl.col("kw_class") == "Yeu").drop("kw_class").sort(["Campaign Name", "Ad Group Name"])
            # Lọc danh sách Từ khóa Khỏe (Dành cho sheet '✨ Từ Khóa Khỏe')
            df_kw_strong = df_all_kw.filter(pl.col("kw_class") == "Khoe").drop("kw_class").sort("ACoS (%)")

            # Sử tại Polars để tính toán Aggregation cấp Campaign dựa trên dữ liệu Keyword (nhanh hơn SQL lồng nhau)
            df_camp_agg = (
                df_all_kw.group_by(["Campaign Name", "Campaign ID"])
                .agg([
                    pl.col("kw_class").count().alias("So KW"),
                    (pl.col("kw_class") == "Yeu").sum().alias("KW Yeu"),
                    (pl.col("kw_class") == "Khoe").sum().alias("KW Khoe"),
                    pl.col("Impressions").sum().alias("Total Impressions"),
                    pl.col("Clicks").sum().alias("Total Clicks"),
                    pl.col("Orders").sum().alias("Total Orders"),
                    pl.col("Spend ($)").sum().round(2).alias("Total Spend ($)"),
                    pl.col("Sales ($)").sum().round(2).alias("Total Sales ($)"),
                ])
                .with_columns([
                    # Tính ACoS trung bình toàn chiến dịch (Làm tròn 1 chữ số)
                    pl.when(pl.col("Total Sales ($)") > 0).then((pl.col("Total Spend ($)") * 100 / pl.col("Total Sales ($)")).round(1)).alias("ACoS TB (%)"),
                    # Phân loại Campaign dựa trên sự hiện diện của KW yếu/khỏe
                    pl.when(pl.col("KW Yeu") == 0).then(pl.lit("Tot")).when(pl.col("KW Khoe") == 0).then(pl.lit("Yeu")).otherwise(pl.lit("Trung binh")).alias("camp_class"),
                ])
            )

            # Tách ra 2 bảng Campaign chuyên dụng
            df_camp_good = df_camp_agg.filter(pl.col("camp_class") == "Tot").drop("camp_class").sort("ACoS TB (%)")
            df_camp_weak = df_camp_agg.filter(pl.col("camp_class") == "Yeu").drop("camp_class").sort("ACoS TB (%)", descending=True)

            print(f"✅ [DUCKDB] Sheet chi tiết: KW Yếu={df_kw_weak.height} | KW Khỏe={df_kw_strong.height} | Camp Tốt={df_camp_good.height} | Camp Yếu={df_camp_weak.height}")
            return {"kw_weak": df_kw_weak, "kw_strong": df_kw_strong, "camp_good": df_camp_good, "camp_weak": df_camp_weak}
        except Exception as e:
            print(f"❌ [DUCKDB] Lỗi lấy chi tiết phân loại: {e}")
            return {}

    def close(self):
        """Đóng kết nối DuckDB gác lại mọi tài nguyên RAM."""
        self.con.close()

# Đoạn code kiểm tra DuckDB khi chạy riêng file db_engine.py
if __name__ == "__main__":
    db = DuckDBEngine()
    latest_df = db.get_latest_metrics()
    if not latest_df.is_empty():
         print("--- Mẫu dữ liệu lịch sử mới nhất (Top 5 dòng) ---")
         print(latest_df.head())
    db.close()
