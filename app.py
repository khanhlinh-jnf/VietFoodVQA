import streamlit as st
import pandas as pd
from supabase import create_client, Client

st.set_page_config(layout="wide")
st.title("Vietnamese Food VQA - Tool Gán Nhãn Đồ Ăn")

# ==========================================
# 0. KHỞI TẠO KẾT NỐI SUPABASE
# ==========================================
SUPABASE_URL = "https://cvdoasxazyruytejluvv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN2ZG9hc3hhenlydXl0ZWpsdXZ2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMyMTM3NzEsImV4cCI6MjA4ODc4OTc3MX0.jWnKQXoKlXOJXua-Q0Z5Dcqq5kLhXD7rmIA2w7FogSg" # <-- Dán key ẩn danh vào đây

@st.cache_resource
def init_connection():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = init_connection()

# ==========================================
# 1. SIDEBAR: LỌC & CHẾ ĐỘ XEM
# ==========================================
st.sidebar.header("Chọn ảnh")
start_id = st.sidebar.text_input("Từ ID (VD: image000000):", value="image000000")
end_id = st.sidebar.text_input("Đến ID (VD: image000100):", value="image000100")

st.sidebar.markdown("---")
view_mode = st.sidebar.radio(
    "Chế độ xem:",
    ("Chỉ hiện ảnh chưa duyệt", "Hiện toàn bộ")
)

# ==========================================
# 2. QUERY KÉO DATA TỪ SUPABASE
# ==========================================
query = supabase.table("image").select("image_id").gte("image_id", start_id).lte("image_id", end_id).order("image_id")

if view_mode == "Chỉ hiện ảnh chưa duyệt":
    query = query.is_("is_checked", False)

list_response = query.execute()

if not list_response.data:
    if view_mode == "Chỉ hiện ảnh chưa duyệt":
        st.success("Đã hoàn thành hết (hoặc không tìm thấy) ảnh cần duyệt trong dải ID này!")
    else:
        st.warning("Không có ảnh nào trong dải ID này!")
    st.stop() 

all_ids = [row["image_id"] for row in list_response.data]

# --- FIX LỖI Ở ĐÂY: Đồng bộ biến trung gian vào Selectbox ---
if "next_img_id" in st.session_state:
    if st.session_state.next_img_id in all_ids:
        st.session_state.selected_img = st.session_state.next_img_id
    # Xóa biến trung gian đi để tránh kẹt trạng thái
    del st.session_state.next_img_id

selected_id = st.sidebar.selectbox(
    "Chọn ảnh để xem/sửa:", 
    all_ids,
    key="selected_img"
)
# -------------------------------------------------------------

detail_response = (supabase.table("image")
                   .select("*")
                   .eq("image_id", selected_id)
                   .limit(1)
                   .execute())

current_row = detail_response.data[0]

img_id = current_row['image_id']
img_url = current_row['image_url']
is_checked_status = current_row.get('is_checked')

status_text = "🟢 Đã duyệt" if is_checked_status else "🔴 Chưa duyệt"
st.write(f"**Đang xử lý ảnh ID:** `{img_id}` | Trạng thái: {status_text}")

# ==========================================
# 3. GIAO DIỆN SPLIT-VIEW (TRÁI/PHẢI)
# ==========================================
col1, col2 = st.columns([1, 1])

with col1:
    st.image(img_url, use_container_width=True)

    st.markdown("---")
    old_drop_status = current_row.get('is_drop')
    default_radio_index = 1 if old_drop_status is True else 0 
    
    keep_image = st.radio(
        "Có nên giữ lại ảnh này không? (Chọn Không nếu ảnh mờ, sai chủ đề)",
        ("Có", "Không"),
        index=default_radio_index,
        horizontal=True
    )

with col2:
    st.subheader("Danh sách món ăn")
    st.write("Nhập tên món ăn, **mỗi món trên 1 dòng**. Bấm Enter để xuống dòng gõ tiếp.")
    
    existing_foods = current_row.get('food_items') or []
    foods_str_default = "\n".join(existing_foods)
    
    edited_foods_str = st.text_area(
        "Danh sách món (Gõ vào đây):", 
        value=foods_str_default, 
        height=250, 
        key=f"text_area_{img_id}"
    )

    st.markdown("---")
    img_desc_input = st.text_area(
        "Mô tả/Ghi chú thêm về ảnh (Tùy chọn):", 
        value=current_row.get('image_desc') or "",
        height=100
    )

# ==========================================
# 4. LOGIC LƯU DATABASE & AUTO-ADVANCE
# ==========================================
st.markdown("---")
if st.button("Lưu", type="primary", use_container_width=True):
    raw_foods = edited_foods_str.split('\n')
    final_foods = [f.strip() for f in raw_foods if f.strip() != ""]
    
    db_food_items = final_foods if len(final_foods) > 0 else None
    db_image_desc = img_desc_input.strip() if img_desc_input.strip() else None
    is_drop_val = True if keep_image == "Không" else False

    supabase.table("image").update({
        "food_items": db_food_items,
        "image_desc": db_image_desc,
        "is_drop": is_drop_val,
        "is_checked": True
    }).eq("image_id", img_id).execute()
    
    # --- FIX LỖI Ở ĐÂY: Lưu ID ảnh kế tiếp vào biến trung gian ---
    if view_mode == "Hiện toàn bộ":
        current_idx = all_ids.index(selected_id)
        if current_idx + 1 < len(all_ids):
            st.session_state.next_img_id = all_ids[current_idx + 1]
    
    st.rerun()