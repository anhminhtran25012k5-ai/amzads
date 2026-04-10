import polars as pl # Import thư viện lõi Polars để xử lý data tốc độ cao (Engine chính của dự án)
import sys        # Import thư viện hệ thống để can thiệp cấu hình dòng lệnh
from typing import Tuple # Thư viện hỗ trợ định nghĩa kiểu dữ liệu trả về

# Đảm bảo Terminal (CMD/PowerShell) hiển thị đúng các biểu tượng Emoji và tiếng Việt (UTF-8)
sys.stdout.reconfigure(encoding='utf-8')

def calculate_metrics_and_apply_rules(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]: 
    """
    Hàm 'Bộ não' của dự án: Tính toán các chỉ số quảng cáo và áp dụng 4 Điều Luật để dán nhãn gợi ý.
    Trả về 4 bảng: [Bảng Tổng hợp], [Bảng Camp lỗi], [Bảng Camp tốt], [Bảng Từ khóa siêu sao]
    """
    print("⏳ Bắt đầu tính toán rải Metrics (CTR, CR, ACoS)...")
    
    # [1] TÍNH TOÁN METRICS PHÁI SINH: Sử dụng Polars để tính toán song song trên CPU
    df_calc = df.with_columns([ 
        
        # LOGIC TÍNH CTR (Tỷ lệ nhấp): CTR = Clicks / Impressions
        # Điều kiện: Chỉ tính toán khi Impressions > 0 để tránh lỗi Toán học "Chia cho 0"
        (pl.when(pl.col("Impressions") > 0)          # Nếu lượt hiển thị lớn hơn 0
           .then(pl.col("Clicks") / pl.col("Impressions")) # Thì thực hiện phép chia
           .otherwise(None)).alias("CTR_Calc"),      # Nếu không, gán giá trị Trống (None)
           
        # LOGIC TÍNH CR (Tỷ lệ chuyển đổi đơn hàng): CR = Orders / Clicks   
        # Điều kiện: Chỉ tính toán khi có ít nhất 1 người Click (>0)
        (pl.when(pl.col("Clicks") > 0)              # Nếu có lượt nhấp chuột
           .then(pl.col("Orders") / pl.col("Clicks"))      # Lấy số đơn hàng chia cho số lượt nhấp
           .otherwise(None)).alias("CR_Calc"),       # Nếu ko có click, trả về None
           
        # LOGIC TÍNH ACoS (Chi phí quảng cáo / Doanh thu): ACoS = Spend / Sales
        # Điều kiện: Chỉ tính toán khi đã có Doanh thu (Sales > 0)
        (pl.when(pl.col("Sales") > 0)               # Nếu phát sinh doanh thu từ quảng cáo
           .then(pl.col("Spend") / pl.col("Sales"))        # Lấy số tiền đã tiêu chia cho doanh thu mang về
           .otherwise(None)).alias("ACoS_Calc")      # Nếu chưa có sales, trả về None
    ])
    
    print("🎯 Bắt đầu quét 4 ĐIỀU LUẬT CƠ BẢN vào toàn bộ file...")
    
    # [2] ĐỊNH NGHĨA CÁC ĐIỀU LUẬT (Dịch từ tiêu chuẩn nghiệp vụ)
    # Lưu ý quy đổi: 5% = 0.05 | 0.3% = 0.003
    
    # LUẬT 0: Hoàn toàn không có hiển thị (Tàng hình - Impression = 0 hoặc rỗng)
    rule_0 = (pl.col("Impressions").is_null()) | (pl.col("Impressions") == 0)
    
    # LUẬT 1: Impressions Thấp (Dưới 1000 lượt - Chưa đủ mẫu số để đánh giá)
    rule_1 = (pl.col("Impressions") > 0) & (pl.col("Impressions") < 1000) 
    
    # LUẬT 2: CTR Thấp (Khách nhìn thấy nhưng không thèm nhấp chuột - < 0.3%)
    rule_2 = (pl.col("Impressions") >= 1000) & (pl.col("CTR_Calc") < 0.003) 
    
    # LUẬT 3: CR Thấp (Khách nhấp vào xem nhưng không mua hàng - CR < 5%)
    # Điều kiện đi kèm: CTR phải ổn (>= 0.3%) thì mới xét đến CR
    rule_3 = (pl.col("CTR_Calc") >= 0.003) & (pl.col("CR_Calc") < 0.05)
    
    # LUẬT 4: ACoS Cao (Bán được hàng nhưng chi phí quá đắt, lỗ vốn - ACoS >= 40%)
    rule_4 = pl.col("ACoS_Calc") >= 0.40 
    
    # LUẬT ĐẶC BIỆT CHO CAMPAIGN: Trên 20 Click mà vẫn chưa có đơn nào (Thảm họa)
    rule_camp_worse = (pl.col("Entity") == "Campaign") & (pl.col("Clicks") > 20) & ((pl.col("Orders").is_null()) | (pl.col("Orders") == 0))
    
    # LUẬT PHONG "SIÊU SAO": Các từ khóa cực phẩm (Imps>=3K, CTR>=1%, CR>=12%, ACoS<30%)
    rule_superstar = (
        pl.col("Entity").is_in(["Keyword", "Product Targeting"]) &
        (pl.col("Impressions") >= 3000) &
        (pl.col("CTR_Calc") >= 0.01) &   # Bấm chuột >= 1% (Mức Cao)
        (pl.col("CR_Calc") >= 0.12)  &   # Mua hàng >= 12% (Mức Cao)
        (pl.col("ACoS_Calc") < 0.30) &   # Lỗ quảng cáo < 30% (Mức Thấp)
        pl.col("ACoS_Calc").is_not_null()
    )

    # [3] Luồng quyết định (DECISION TREE): Dán nhãn AI Recommendation theo thứ tự ưu tiên
    df_result = df_calc.with_columns(
        # Nhãn 1: Phát hiện Campaign lỗi nặng cần làm lại ngay
        pl.when(rule_camp_worse).then(pl.lit("CẦN LÀM LẠI CHIẾN DỊCH (Thảm họa: >20 Click mà 0 Đơn)"))
        
        # Nhãn 2: Bỏ qua các dòng dữ liệu không phải từ khóa trực tiếp (như dòng Ad Group, v.v.)
        .when(~pl.col("Entity").is_in(["Keyword", "Product Targeting"])).then(pl.lit("BỎ QUA (Không phải cấp đấu thầu trực tiếp)"))
        
        # Nhãn 3: Gán nhãn cho các Từ khóa Siêu Sao
        .when(rule_superstar).then(pl.lit("🏆 TỪ KHÓA SIÊU SAO (Imps>=3K, CTR>=1%, CR>=12%, ACoS<30%) -> Vít Bid ngay!"))
        
        # Nhãn 4: Cảnh báo lỗ nặng dựa trên ACoS
        .when(rule_4).then(pl.lit("LỖ NẶNG (ACoS >= 40%) -> Cần Tối Ưu Giảm Bid"))
        
        # Nhãn 5: Cảnh báo tỷ lệ mua hàng (CR) thấp
        .when(rule_3).then(pl.lit("CR THẤP (Click nhiều mà không mua) -> Khảo sát lại Giá hoặc A+ Content"))
        
        # Nhãn 6: Cảnh báo tỷ lệ nhấp (CTR) thấp - Do hình ảnh không hút khách
        .when(rule_2).then(pl.lit("CTR THẤP (Thấy nhiều mà không Click) -> Đổi/Tối ưu Hình ảnh Main"))
        
        # Nhãn 7: Cảnh báo Tàng Hình - Impression bằng 0
        .when(rule_0).then(pl.lit("BỊ TÀNG HÌNH (0 Impression) -> Cần điều chỉnh Keyword ngay"))
        
        # Nhãn 8: Impression thấp (Lượng tiếp cận khách hàng quá ít)
        .when(rule_1).then(pl.lit("IMPRESSION THẤP (<1000) -> Chỉnh từ khóa (phạm vi quá rộng/chung chung)"))
        
        # Mặc định: Nếu không vi phạm bất cứ luật nào ở trên -> Gán nhãn Bình Thường
        .otherwise(pl.lit("BÌNH THƯỜNG / THEO DÕI THÊM"))
          .alias("AI_Recommendation") # Lưu vào cột mới tên là 'AI_Recommendation'
    )
    
    # -------------------------------------------------------------------------
    # TRÍCH XUẤT CÁC NHÓM CAMPAIGN/TỪ KHÓA ĐỂ TẠO SHEET RIÊNG
    # -------------------------------------------------------------------------

    # 1. Nhóm: Phả hệ của Campaign thảm họa (Lấy tất cả các dòng thuộc campaign đó)
    bad_campaign_ids = (
        df_result.filter(pl.col("AI_Recommendation").str.contains("CẦN LÀM LẠI CHIẾN DỊCH"))
                 .get_column("Campaign ID")
                 .unique()
                 .to_list()
    )
    if bad_campaign_ids:
        print(f"⚠️ Phát hiện {len(bad_campaign_ids)} Campaign Thảm Họa! Rút trích ổ lỗi...")
        df_bad_family = df_result.filter(pl.col("Campaign ID").is_in(bad_campaign_ids))
    else:
        df_bad_family = df_result.clear() # Nếu ko có thì trả bảng trống

    # 2. Nhóm: Campaign Xuất Sắc (Là những campaign KHÔNG có bất kỳ từ khóa nào bị cảnh báo)
    # Lấy danh sách các Campaign "bị nhúng chàm" (có ít nhất 1 cảnh báo)
    tainted_campaign_ids = (
        df_result.filter(
            ~pl.col("AI_Recommendation").is_in([
                "BÌNH THƯỜNG / THEO DÕI THÊM",
                "BỎ QUA (Không phải cấp đấu thầu trực tiếp)"
            ])
        )
        .get_column("Campaign ID")
        .unique()
        .to_list()
    )
    
    # Lấy toàn bộ danh sách Campaign ID duy nhất có trong file
    all_campaign_ids = df_result.get_column("Campaign ID").drop_nulls().unique().to_list()
    
    # Loại trừ những campaign "bị nhúng chàm" ra khỏi danh sách tổng
    excellent_campaign_ids = [cid for cid in all_campaign_ids if cid not in tainted_campaign_ids]
    
    if excellent_campaign_ids:
        print(f"🌟 Phát hiện {len(excellent_campaign_ids)} Campaign Xuất Sắc (100% tỷ lệ Tốt)! Rút trích ổ Vàng...")
        df_good_family = df_result.filter(pl.col("Campaign ID").is_in(excellent_campaign_ids))
    else:
        df_good_family = df_result.clear()
        
    # 3. Nhóm: Danh sách các Từ Khóa Siêu Sao
    df_superstars = df_result.filter(rule_superstar)
    print(f"💎 Phát hiện {df_superstars.height} Từ khóa Siêu Sao cá nhân độc lập!")

    # Trả về kết quả cuối cùng gồm 4 DataFrame
    return df_result, df_bad_family, df_good_family, df_superstars 

# Chạy thử nghiệm file độc lập
if __name__ == "__main__":
    from ingest_engine import load_sponsored_products
    # Thử nạp file report mặc định
    df = load_sponsored_products("data/input/report.xlsx")
    if df is not None:
        # Chạy logic phân tích
        df_analyzed, df_bad_isolated, df_good_isolated, df_stars = calculate_metrics_and_apply_rules(df)
        
        print("\n📊 THỐNG KÊ KẾT QUẢ ĐỀ XUẤT:")
        # Đếm xem mỗi loại nhãn Suggest có bao nhiêu từ khóa/thực thể
        print(df_analyzed.group_by("AI_Recommendation").count().sort("count", descending=True))
        
        # In ra số lượng tóm tắt các nhóm đặc biệt
        print(f"\n🚨 Số Campaign Thảm Họa: {df_bad_isolated.height}")
        print(f"🌟 Số Campaign Tốt: {df_good_isolated.height}")
        print(f"💎 Số Từ khóa Siêu Sao Tách Lẻ: {df_stars.height}")
