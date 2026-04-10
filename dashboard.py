import streamlit as st          # Thư viện xây dựng giao diện Web Dashboard nhanh chóng
import plotly.express as px      # Thư viện vẽ biểu đồ tương tác cao cấp (dễ dùng)
import plotly.graph_objects as go # Thư viện vẽ biểu đồ tùy biến sâu (cho biểu đồ cột kết hợp đường)
from db_engine import DuckDBEngine # Import bộ não truy vấn dữ liệu từ DuckDB
import pandas as pd              # Thư viện xử lý bảng dữ liệu (vật chứa trung gian cho Plotly)

# Cấu hình trang Dashboard: Tiêu đề trình duyệt (Tab name), Bố cục rộng (Wide), Biểu tượng đồ thị
st.set_page_config(page_title="Amazon Ads Analytics", layout="wide", page_icon="📈")

# Tiêu đề chính hiển thị trên trang Dashboard
st.title("📈 Amazon Ads Dashboard (Hệ thống Kho Dữ liệu Time-Series)")

# --- THIẾT LẬP THANH ĐIỀU KHIỂN BÊN TRÁI (SIDEBAR) ---
st.sidebar.header("🕹️ Bộ điều khiển")
# Nút bấm quan trọng: Dùng để xóa sạch dữ liệu cũ nếu muốn làm lại từ đầu
if st.sidebar.button("🚨 Xóa toàn bộ Dữ liệu Lịch sử", type="primary", use_container_width=True):
    db = DuckDBEngine()         # Kết nối tới DuckDB
    success = db.reset_database() # Gọi lệnh xóa thư mục Parquet
    db.close()                  # Đóng kết nối
    if success:
        st.sidebar.success("✅ Đã dọn dẹp sạch Database!")
        st.rerun()              # Tải lại trang để cập nhật giao diện trống
    else:
        st.sidebar.error("❌ Xóa thất bại, vui lòng kiểm tra Console.")

# --- HÀM TẢI DỮ LIỆU TỪ DUCKDB ---
@st.cache_data(ttl=1)  # Kỹ thuật Cache giúp Dashboard mượt mà, tự động làm mới sau 1 giây nếu có file mới
def load_data():
    """Truy vấn DuckDB để lấy bảng tổng hợp dữ liệu theo từng ngày báo cáo."""
    db = DuckDBEngine()
    df_pl = db.get_historical_summary() # Lấy Spend, Sales, Clicks, Orders theo Ngày
    db.close()
    if not df_pl.is_empty():
        # Chuyển từ Polars (siêu nhanh) sang Pandas (Plotly yêu cầu Pandas để vẽ biểu đồ)
        return df_pl.to_pandas()
    return pd.DataFrame() # Trả về bảng rỗng nếu ko có dữ liệu

# Thực hiện nạp dữ liệu vào biến 'df'
df = load_data()

# KIỂM TRA: Nếu không có dữ liệu thì hiện thông báo hướng dẫn
if df.empty:
    st.info("📂 Kho dữ liệu lịch sử hiện đang trống. Hãy chạy file `pipeline.py` để nạp báo cáo ngày đầu tiên.")
else:
    # HIỂN THỊ KPI: Lấy ngày báo cáo mới nhất trong kho
    latest_date = df['Report Date'].max()
    st.markdown(f"**📅 Dữ liệu cập nhật mới nhất tính đến ngày:** {latest_date}")
    
    # --- BIỂU ĐỒ SỐ 1: CHI PHÍ VÀ DOANH THU (SPEND VS SALES) ---
    st.subheader("📊 1. Xu hướng Chi phí và Doanh thu (Spend vs Sales)")
    fig_spend_sales = go.Figure()
    # Biểu đồ Cột: Doanh thu (Sales) màu xanh lá
    fig_spend_sales.add_trace(go.Bar(
        x=df['Report Date'], y=df['Total Sales'], name='Total Sales', marker_color='green'
    ))
    # Biểu đồ Đường: Chi phí (Spend) màu đỏ đậm (để thấy rõ tỷ lệ tiêu tiền so với doanh thu)
    fig_spend_sales.add_trace(go.Scatter(
        x=df['Report Date'], y=df['Total Spend'], name='Total Spend', 
        mode='lines+markers', line=dict(color='red', width=3)
    ))
    # Cấu hình trục và tiêu đề
    fig_spend_sales.update_layout(barmode='group', xaxis_title="Ngày báo cáo", yaxis_title="Giá trị ($)")
    st.plotly_chart(fig_spend_sales, use_container_width=True)

    # --- BIỂU ĐỒ SỐ 2 & 3: LƯỢT CLICK VÀ ĐƠN HÀNG (CHIA LÀM 2 CỘT) ---
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🖱️ 2. Xu hướng Lượt Click")
        # Vẽ biểu đồ đường cong (spline) cho lượt Click màu xanh biển
        fig_clicks = px.line(df, x='Report Date', y='Total Clicks', markers=True, 
                             line_shape='spline', color_discrete_sequence=['blue'])
        st.plotly_chart(fig_clicks, use_container_width=True)

    with col2:
        st.subheader("🛒 3. Xu hướng Lượt Mua (Orders)")
        # Vẽ biểu đồ đường cong cho Đơn hàng (Orders) màu cam
        fig_orders = px.line(df, x='Report Date', y='Total Orders', markers=True, 
                             line_shape='spline', color_discrete_sequence=['orange'])
        st.plotly_chart(fig_orders, use_container_width=True)
    
    # --- HIỂN THỊ BẢNG SỐ LIỆU ĐỊNH DẠNG ĐẸP ---
    st.subheader("🗂️ Bảng Số liệu Thống kê Chi tiết")
    # Sử dụng Format để hiển thị dấu $ cho tiền tệ và làm tròn số
    st.dataframe(df.style.format({
        'Total Spend': '${:.2f}',
        'Total Sales': '${:.2f}',
        'Total Clicks': '{:.0f}',
        'Total Orders': '{:.0f}'
    }), use_container_width=True)
