import polars as pl       # Thư viện xử lý bảng dữ liệu (DataFrame)
import os                # Thư viện thao tác đường dẫn tệp tin


def _write_dashboard_sheet(workbook, df_summary, df_classification=None):
    """
    Hàm nội bộ (private): Xây dựng Sheet 'Dashboard Lịch sử' chuyên nghiệp.
    Bao gồm: Bảng số liệu tổng hợp và 4 loại biểu đồ (Xu hướng & Phân loại).
    """
    import xlsxwriter     # Thư viện ghi file Excel chuyên sâu (hỗ trợ vẽ đồ thị)

    # Thêm một trang tính mới vào file Excel
    ws = workbook.add_worksheet("Dashboard Lich su")

    # --- KHỞI TẠO CÁC ĐỊNH DẠNG (STYLE) CHO EXCEL ---
    # Định dạng tiêu đề chính (Chữ trắng, nền đậm, cỡ to)
    fmt_title = workbook.add_format({
        "bold": True, "font_size": 16, "font_color": "#FFFFFF",
        "bg_color": "#1F3864", "align": "center", "valign": "vcenter", "border": 0
    })
    # Định dạng tiêu đề cột (Header)
    fmt_header = workbook.add_format({
        "bold": True, "font_color": "#FFFFFF", "bg_color": "#2E75B6",
        "align": "center", "valign": "vcenter", "border": 1
    })
    # Định dạng ngày tháng
    fmt_date = workbook.add_format({
        "num_format": "yyyy-mm-dd", "align": "center", "border": 1
    })
    # Định dạng số thập phân (có dấu phẩy ngăn cách hàng nghìn)
    fmt_number = workbook.add_format({
        "num_format": "#,##0.00", "border": 1
    })
    # Định dạng số nguyên
    fmt_int = workbook.add_format({
        "num_format": "#,##0", "border": 1
    })
    # Định dạng ghi chú (Chữ nghiêng, màu xám)
    fmt_note = workbook.add_format({
        "italic": True, "font_color": "#888888", "font_size": 9
    })

    # --- THIẾT KẾ GIAO DIỆN TRANG DASHBOARD ---
    # Gộp các ô từ A1 đến E1 để tạo thanh tiêu đề rộng
    ws.merge_range("A1:E1", "📊 DASHBOARD LỊCH SỬ AMAZON ADS", fmt_title)
    ws.set_row(0, 30) # Chỉnh độ cao hàng tiêu đề

    # Ghi chú nguồn dữ liệu
    ws.write("A2", "Dữ liệu tổng hợp tự động từ kho Time-Series Parquet (DuckDB)", fmt_note)
    ws.set_row(1, 14)

    # --- XÂY DỰNG BẢNG DỮ LIỆU TỔNG HỢP (XU HƯỚNG THEO NGÀY) ---
    headers = ["Report Date", "Total Spend ($)", "Total Sales ($)", "Total Clicks", "Total Orders"]
    col_widths = [14, 18, 18, 14, 14] # Độ rộng cố định cho các cột
    for col_i, (h, w) in enumerate(zip(headers, col_widths)):
        ws.write(3, col_i, h, fmt_header) # Ghi header vào hàng thứ 4 (index 3)
        ws.set_column(col_i, col_i, w)   # Áp dụng độ rộng cột

    # Kiểm tra nếu có dữ liệu tổng hợp từ Database thì mới ghi vào
    if df_summary is not None and df_summary.height > 0:
        data = df_summary.to_pandas() # Chuyển sang Pandas để dễ dàng duyệt từng dòng (itertuples)
        for row_i, row in enumerate(data.itertuples(index=False), start=4):
            # Ghi từng giá trị vào ô tương ứng kèm theo Style đã định nghĩa
            # Quan trọng: Không dùng str() cho ngày tháng để Excel nhận diện đúng kiểu DATE
            ws.write(row_i, 0, row[0], fmt_date)                             # Cột Ngày
            ws.write(row_i, 1, float(row[1]) if row[1] else 0, fmt_number)  # Cột Chi phí
            ws.write(row_i, 2, float(row[2]) if row[2] else 0, fmt_number)  # Cột Doanh thu
            ws.write(row_i, 3, int(row[3]) if row[3] else 0, fmt_int)       # Cột Click
            ws.write(row_i, 4, int(row[4]) if row[4] else 0, fmt_int)       # Cột Đơn hàng

        n_rows = df_summary.height # Số lượng dòng dữ liệu hiện có

        # --- BIỂU ĐỒ 1: BIỂU ĐỒ ĐƯỜNG (TREND SPEND VS SALES) ---
        chart1 = workbook.add_chart({"type": "line"})
        # Cấu hình chuỗi dữ liệu Doanh thu (Sales)
        chart1.add_series({
            "name": "Total Sales ($)",
            "categories": ["Dashboard Lich su", 4, 0, 3 + n_rows, 0], # Vùng chứa tên Ngày
            "values":     ["Dashboard Lich su", 4, 2, 3 + n_rows, 2], # Vùng chứa giá trị Sales
            "line": {"color": "#00B050", "width": 2.5}, # Màu xanh lá
            "marker": {"type": "circle", "size": 5, "fill": {"color": "#00B050"}},
        })
        # Cấu hình chuỗi dữ liệu Chi phí (Spend)
        chart1.add_series({
            "name": "Total Spend ($)",
            "categories": ["Dashboard Lich su", 4, 0, 3 + n_rows, 0],
            "values":     ["Dashboard Lich su", 4, 1, 3 + n_rows, 1], # Vùng chứa giá trị Spend
            "line": {"color": "#FF0000", "width": 2.5, "dash_type": "dash"}, # Đường đứt nét màu đỏ
            "marker": {"type": "square", "size": 5, "fill": {"color": "#FF0000"}},
        })
        # Cài đặt tiêu đề và tên trục
        chart1.set_title({"name": "📈 Xu hướng Doanh thu vs Chi phí"})
        chart1.set_x_axis({"name": "Ngày báo cáo", "text_axis": True})
        chart1.set_y_axis({"name": "Giá trị ($)"})
        chart1.set_legend({"position": "bottom"}) # Đưa chú thích xuống dưới
        chart1.set_size({"width": 480, "height": 288}) # Kích thước biểu đồ
        ws.insert_chart(f"G4", chart1) # Chèn biểu đồ vào ô G4

        # --- BIỂU ĐỒ 2: BIỂU ĐỒ KẾT HỢP (CLICKS VS ORDERS) ---
        # Clicks dùng biểu đồ cột (Column), Orders dùng biểu đồ đường (Line)
        chart2 = workbook.add_chart({"type": "column"})
        chart2.add_series({
            "name": "Total Clicks",
            "categories": ["Dashboard Lich su", 4, 0, 3 + n_rows, 0],
            "values":     ["Dashboard Lich su", 4, 3, 3 + n_rows, 3],
            "fill": {"color": "#4472C4"}, # Màu xanh biển cho cột Click
        })
        # Tạo biểu đồ đường cho Orders
        chart_line = workbook.add_chart({"type": "line"})
        chart_line.add_series({
            "name": "Total Orders",
            "categories": ["Dashboard Lich su", 4, 0, 3 + n_rows, 0],
            "values":     ["Dashboard Lich su", 4, 4, 3 + n_rows, 4],
            "line": {"color": "#ED7D31", "width": 2.5}, # Màu cam cho đường Orders
            "marker": {"type": "diamond", "size": 6, "fill": {"color": "#ED7D31"}},
            "y2_axis": True, # Dùng trục tung bên phải (Y2) vì đơn hàng thường nhỏ hơn Click rất nhiều
        })
        chart2.combine(chart_line) # Kết hợp 2 biểu đồ vào làm 1
        chart2.set_title({"name": "🖱️ Clicks (cột) & Orders (đường)"})
        chart2.set_x_axis({"name": "Ngày báo cáo", "text_axis": True})
        chart2.set_y_axis({"name": "Số lượt Click"})
        chart2.set_y2_axis({"name": "Số đơn hàng"}) # Tiêu đề cho trục tung thứ 2
        chart2.set_legend({"position": "bottom"})
        chart2.set_size({"width": 480, "height": 288})
        ws.insert_chart("G22", chart2) # Chèn vào ô G22 (dưới biểu đồ 1)

    # --- KHU VỰC VẼ BIỂU ĐỒ TRÒN (PIE CHARTS) PHÂN LOẠI ---
    # Tự động tính toán vị trí bắt đầu của Pie chart để không đè lên bảng dữ liệu phía trên
    pie_start_row = (4 + (df_summary.height if df_summary is not None and df_summary.height > 0 else 0) + 3)

    if df_classification is not None:
        df_kw   = df_classification.get("keywords",  pl.DataFrame())
        df_camp = df_classification.get("campaigns", pl.DataFrame())

        # Nếu một trong hai bảng (Từ khóa/Campaign) có dữ liệu thì tiến hành vẽ
        if not df_kw.is_empty() or not df_camp.is_empty():
            # Style riêng cho khu vực Pie Chart
            fmt_section = workbook.add_format({
                "bold": True, "font_size": 13, "font_color": "#FFFFFF",
                "bg_color": "#1F3864", "align": "center", "valign": "vcenter"
            })
            fmt_pie_header = workbook.add_format({
                "bold": True, "font_color": "#FFFFFF", "bg_color": "#375623",
                "align": "center", "border": 1
            })
            fmt_pie_label = workbook.add_format({"align": "center", "border": 1})
            fmt_pie_num   = workbook.add_format({"num_format": "#,##0", "align": "center", "border": 1})

            # Tiêu đề thanh ngang phân tách khu vực
            ws.merge_range(pie_start_row, 0, pie_start_row, 4,
                           "PHÂN LOẠI TỪ KHÓA & CAMPAIGN (Dữ liệu mới nhất)", fmt_section)
            ws.set_row(pie_start_row, 24)

            # --- VÙNG DỮ LIỆU TỪ KHÓA (Ghi ra bảng nhỏ để Chart lấy dữ liệu) ---
            kw_label_row = pie_start_row + 1
            ws.write(kw_label_row, 0, "Phân loại Từ Khóa", fmt_pie_header)
            ws.write(kw_label_row, 1, "Số lượng",         fmt_pie_header)

            # Quy định màu xanh-vàng-đỏ cho 3 nhóm chất lượng
            kw_colors = {"Khỏe": "#00B050", "Trung bình": "#FFC000", "Yếu": "#FF0000"}
            kw_rows_written = 0
            kw_data = df_kw.to_pandas() if not df_kw.is_empty() else None
            if kw_data is not None:
                for ri, row in enumerate(kw_data.itertuples(index=False)):
                    ws.write(kw_label_row + 1 + ri, 0, str(row[0]), fmt_pie_label)
                    ws.write(kw_label_row + 1 + ri, 1, int(row[1]),  fmt_pie_num)
                    kw_rows_written += 1

            # --- VÙNG DỮ LIỆU CAMPAIGN ---
            ws.write(kw_label_row, 3, "Phân loại Campaign", fmt_pie_header)
            ws.write(kw_label_row, 4, "Số lượng",           fmt_pie_header)

            camp_data = df_camp.to_pandas() if not df_camp.is_empty() else None
            camp_rows_written = 0
            if camp_data is not None:
                for ri, row in enumerate(camp_data.itertuples(index=False)):
                    ws.write(kw_label_row + 1 + ri, 3, str(row[0]), fmt_pie_label)
                    ws.write(kw_label_row + 1 + ri, 4, int(row[1]),  fmt_pie_num)
                    camp_rows_written += 1

            # --- VẼ PIE CHART 1: PHÂN LOẠI TỪ KHÓA ---
            if kw_rows_written > 0:
                pie_kw = workbook.add_chart({"type": "pie"})
                pie_kw.add_series({
                    "name":       "Phân loại Từ Khóa",
                    "categories": ["Dashboard Lich su", kw_label_row + 1, 0, kw_label_row + kw_rows_written, 0],
                    "values":     ["Dashboard Lich su", kw_label_row + 1, 1, kw_label_row + kw_rows_written, 1],
                    "data_labels": {"percentage": True, "category": True, "separator": "\n"},
                    "points": [
                        # Áp dụng màu sắc đúng theo nhãn (Tốt=Xanh, Xấu=Đỏ)
                        {"fill": {"color": kw_colors.get(r, "#808080")}}
                        for r in (kw_data["Phân loại"].tolist() if kw_data is not None else [])
                    ],
                })
                pie_kw.set_title({"name": "🔑 Phân loại Từ Khóa"})
                pie_kw.set_legend({"position": "bottom"})
                pie_kw.set_size({"width": 360, "height": 288})
                # Chèn biểu đồ nằm lùi xuống dưới bảng dữ liệu của chính nó
                ws.insert_chart(kw_label_row + 1, 0, pie_kw, {"x_offset": 0, "y_offset": 20 * (kw_rows_written + 2)})

            # --- VẼ PIE CHART 2: PHÂN LOẠI CAMPAIGN ---
            if camp_rows_written > 0:
                camp_colors = {"Tốt": "#00B050", "Trung bình": "#FFC000", "Yếu": "#FF0000"}
                pie_camp = workbook.add_chart({"type": "pie"})
                pie_camp.add_series({
                    "name":       "Phân loại Campaign",
                    "categories": ["Dashboard Lich su", kw_label_row + 1, 3, kw_label_row + camp_rows_written, 3],
                    "values":     ["Dashboard Lich su", kw_label_row + 1, 4, kw_label_row + camp_rows_written, 4],
                    "data_labels": {"percentage": True, "category": True, "separator": "\n"},
                    "points": [
                        {"fill": {"color": camp_colors.get(r, "#808080")}}
                        for r in (camp_data["Phân loại"].tolist() if camp_data is not None else [])
                    ],
                })
                pie_camp.set_title({"name": "📢 Phân loại Campaign"})
                pie_camp.set_legend({"position": "bottom"})
                pie_camp.set_size({"width": 360, "height": 288})
                ws.insert_chart(kw_label_row + 1, 3, pie_camp, {"x_offset": 0, "y_offset": 20 * (camp_rows_written + 2)})

    else:
        # Nếu chưa nạp lịch sử lần nào, in thông báo trống
        ws.write(pie_start_row + 1, 0, "Chưa có dữ liệu phân loại để vẽ Biểu đồ Tròn.",
                 workbook.add_format({"italic": True, "font_color": "#888888"}))


def export_to_excel(df_dict: dict, output_path: str, df_history=None, df_classification=None):
    """
    Hàm chủ lực xuất file Excel hoàn thiện với nhiều trang (Multi-Sheet).
    - df_dict: Danh sách các DataFrame phân tách (KW Siêu sao, Campaign lỗi, v.v...)
    - df_history: Dữ liệu xu hướng từ DuckDB dành cho Dashboard.
    - df_classification: Dữ liệu phân loại phần trăm cho Dashboard.
    """
    print("⏳ [OUTPUT] Bắt đầu kết xuất dữ liệu ra file Excel đa Sheet...")

    # Đảm bảo thư mục đích (data/output) tồn tại trước khi ghi file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    try:
        import xlsxwriter
        # Sử dụng Workbook context manager để đảm bảo đóng file đúng cách ngay cả khi gặp lỗi
        with xlsxwriter.Workbook(output_path) as workbook:

            # DUYỆT QUA CÁC SHEET PHÁP ĐỊNH (Sheet 1 đến 4, và các sheet chi tiết từ DuckDB)
            for sheet_name, df_sheet in df_dict.items():
                if df_sheet.height > 0:
                    # Polars hỗ trợ ghi trực tiếp vào Workbook đang mở của xlsxwriter
                    df_sheet.write_excel(
                        workbook=workbook,
                        worksheet=sheet_name,
                        autofit=True, # Tự động giãn độ rộng ô theo nội dung
                        header_format={"bold": True, "bg_color": "#D3D3D3"} # Định dạng Header màu xám
                    )

            # TIẾN HÀNH XÂY DỰNG SHEET DASHBOARD (Sheet Lịch sử nằm ở cuối file)
            _write_dashboard_sheet(workbook, df_history, df_classification)

        # Tính tổng số sheet đã xuất
        n_sheets = len(df_dict) + 1  # +1 là sheet Dashboard
        print(f"✅ ĐÃ LƯU THÀNH CÔNG TẠI: {output_path} (Đã bung {n_sheets} Sheet)")

    except Exception as e:
        # Trường hợp file đang mở bởi Excel hoặc bị lỗi quyền ghi đĩa
        print(f"❌ Xảy ra lỗi khi xuất file: {e}")


if __name__ == "__main__":
    # File này chỉ là engine phục vụ, không nên chạy độc lập
    print("Vui lòng chạy script thông qua pipeline.py để thấy kết xuất Excel.")
