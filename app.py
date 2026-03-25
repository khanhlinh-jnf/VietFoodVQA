from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import streamlit as st
from supabase import create_client

st.set_page_config(layout="wide")
st.title("Vietnamese Food VQA - Annotation & Verify Tool")

SUPABASE_URL = "https://cvdoasxazyruytejluvv.supabase.co"
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
PAGE_SIZE = 1000
PROJECT_ROOT = Path(__file__).resolve().parent
QUESTION_TYPES_CSV = PROJECT_ROOT / "data" / "question_types.csv"

VERIFY_FIELD_CANDIDATES = {
    "q0": ["q0_score", "verify_q0", "score_q0"],
    "q1": ["q1_score", "verify_q1", "score_q1"],
    "q2": ["q2_score", "verify_q2", "score_q2"],
    "q3": ["q3_score", "verify_q3", "score_q3"],
    "decision": ["verify_decision", "review_decision", "decision"],
    "notes": ["verify_notes", "review_notes", "notes", "reviewer_note"],
    "rule": ["verify_rule", "review_rule"],
}

VERIFY_OPTIONS: dict[str, dict[int, str]] = {
    "q0": {
        1: "1 — Dùng sai triple / nhắc tới thứ không có trong ảnh (DROP)",
        2: "2 — Không dùng triple",
        3: "3 — Dùng triple 1-hop",
        4: "4 — Suy luận 2-hop tốt",
    },
    "q1": {
        1: "1 — Sai cả vị trí và màu sắc (DROP)",
        2: "2 — Sai 1 trong 2 (DROP)",
        3: "3 — Đúng nhưng khó hiểu",
        4: "4 — Hoàn hảo",
    },
    "q2": {
        1: "1 — Đáp án sai",
        2: "2 — Nhiều đáp án đúng",
        3: "3 — Đúng nhưng nhiễu yếu",
        4: "4 — Đúng và nhiễu tốt",
    },
    "q3": {
        1: "1 — Rationale bịa đặt",
        2: "2 — Rationale chung chung",
        3: "3 — Rationale đúng logic",
        4: "4 — Kết nối ảnh - tri thức tường minh",
    },
}

VERIFY_TITLES = {
    "q0": "Q0: KG Alignment",
    "q1": "Q1: Visual Accuracy",
    "q2": "Q2: Choice Logic",
    "q3": "Q3: Rationale",
}


@st.cache_resource
def init_connection():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


supabase = init_connection()


def apply_bool_filter(query, column_name: str, filter_value: str):
    if filter_value == "True":
        return query.is_(column_name, True)
    if filter_value == "False":
        return query.is_(column_name, False)
    return query


def norm_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def format_choices_block(row: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"A. {norm_text(row.get('choice_a'))}",
            f"B. {norm_text(row.get('choice_b'))}",
            f"C. {norm_text(row.get('choice_c'))}",
            f"D. {norm_text(row.get('choice_d'))}",
        ]
    ).strip()


def parse_choices_block(raw_text: str) -> tuple[dict[str, str], list[str]]:
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    parsed: dict[str, str] = {}
    fallback_values: list[str] = []

    for line in lines:
        if len(line) >= 2 and line[0].upper() in {'A', 'B', 'C', 'D'} and line[1] in {'.', ')', ':', '-'}:
            key = line[0].upper()
            value = line[2:].strip(' .):-\t')
            if value:
                parsed[key] = value
        else:
            fallback_values.append(line)

    if not parsed and len(fallback_values) == 4:
        parsed = dict(zip(['A', 'B', 'C', 'D'], fallback_values))

    missing = [key for key in ['A', 'B', 'C', 'D'] if not parsed.get(key)]
    return parsed, missing


def render_image_metadata_block(image_row: dict[str, Any]) -> None:
    with st.expander("Thông tin ảnh", expanded=False):
        st.write(f"**image_id:** `{image_row['image_id']}`")
        foods = image_row.get("food_items") or []
        st.write("**food_items:**", ", ".join(foods) if foods else "(trống)")
        st.write("**image_desc:**")
        st.write(image_row.get("image_desc") or "(trống)")


def fetch_all_rows(query_builder, page_size: int = PAGE_SIZE) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    start = 0
    while True:
        response = query_builder.range(start, start + page_size - 1).execute()
        batch = response.data or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size
    return rows


def fetch_image_ids_for_filter(start_id: str, end_id: str) -> list[str]:
    query = (
        supabase.table("image")
        .select("image_id")
        .gte("image_id", start_id)
        .lte("image_id", end_id)
        .eq("is_checked", True)
        .eq("is_drop", False)
        .order("image_id")
    )
    rows = fetch_all_rows(query)
    return [row["image_id"] for row in rows if row.get("image_id")]


@st.cache_data
def fetch_question_types() -> list[str]:
    if not QUESTION_TYPES_CSV.exists():
        return []

    values: list[str] = []
    seen: set[str] = set()
    with QUESTION_TYPES_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            value = norm_text(row.get("canonical_qtype") or row.get("question_type"))
            if value and value not in seen:
                values.append(value)
                seen.add(value)
    return values


def find_existing_column(row: dict[str, Any], logical_name: str) -> str | None:
    for candidate in VERIFY_FIELD_CANDIDATES.get(logical_name, []):
        if candidate in row:
            return candidate
    return None


def get_existing_verify_value(row: dict[str, Any], logical_name: str, default: Any = None) -> Any:
    column = find_existing_column(row, logical_name)
    if column is None:
        return default
    value = row.get(column)
    return default if value is None else value


def build_verify_payload(row: dict[str, Any], scores: dict[str, int], decision: str, notes: str, rule_text: str) -> dict[str, Any]:
    payload: dict[str, Any] = {}

    mapping = {
        "q0": scores["q0"],
        "q1": scores["q1"],
        "q2": scores["q2"],
        "q3": scores["q3"],
        "decision": decision,
        "notes": notes.strip() or None,
        "rule": rule_text or None,
    }
    for logical_name, value in mapping.items():
        column = find_existing_column(row, logical_name)
        if column is not None:
            payload[column] = value
    return payload


def evaluate_verify(scores: dict[str, int]) -> tuple[str, str, list[str]]:
    reasons: list[str] = []
    rule = "PASS"
    decision = "KEEP"

    if scores["q0"] == 1:
        decision = "DROP"
        rule = "Q0=1"
        reasons.append("Q0 = 1: câu hỏi dùng sai triple hoặc nhắc tới đối tượng không có trong ảnh.")

    if scores["q1"] <= 2:
        decision = "DROP"
        if rule == "PASS":
            rule = "Q1<=2"
        else:
            rule = f"{rule} + Q1<=2"
        reasons.append("Q1 ≤ 2: thông tin thị giác (vị trí / màu sắc) không đạt guideline.")

    if decision == "KEEP":
        reasons.append("Không kích hoạt luật auto-drop từ guideline hiện tại.")

    if scores["q2"] <= 2:
        reasons.append("Q2 thấp: nên kiểm tra lại đáp án đúng và chất lượng distractor.")
    if scores["q3"] <= 2:
        reasons.append("Q3 thấp: rationale còn yếu hoặc chưa bám sát triple.")

    return decision, rule, reasons


def render_verify_summary(scores: dict[str, int]) -> tuple[str, str]:
    auto_decision, auto_rule, reasons = evaluate_verify(scores)
    avg_score = sum(scores.values()) / 4

    metric_cols = st.columns(5)
    metric_cols[0].metric("Q0", scores["q0"])
    metric_cols[1].metric("Q1", scores["q1"])
    metric_cols[2].metric("Q2", scores["q2"])
    metric_cols[3].metric("Q3", scores["q3"])
    metric_cols[4].metric("Avg", f"{avg_score:.2f}")

    if auto_decision == "DROP":
        st.error(f"Khuyến nghị theo guideline: DROP ({auto_rule})")
    else:
        st.success("Khuyến nghị theo guideline: KEEP")

    for reason in reasons:
        st.write(f"- {reason}")

    return auto_decision, auto_rule


def load_image_annotation_page() -> None:
    st.sidebar.header("Chọn ảnh")
    start_id = st.sidebar.text_input("Từ ID (VD: image000000):", value="image000000", key="img_start")
    end_id = st.sidebar.text_input("Đến ID (VD: image001000):", value="image001000", key="img_end")

    st.sidebar.markdown("---")
    filter_is_drop = st.sidebar.selectbox(
        "Lọc theo is_drop:",
        ["Tất cả", "True", "False"],
        index=2,
        key="img_filter_drop",
    )

    filter_is_checked = st.sidebar.selectbox(
        "Lọc theo is_checked:",
        ["Tất cả", "True", "False"],
        index=2,
        key="img_filter_checked",
    )

    query = (
        supabase.table("image")
        .select("image_id")
        .gte("image_id", start_id)
        .lte("image_id", end_id)
        .order("image_id")
    )
    query = apply_bool_filter(query, "is_drop", filter_is_drop)
    query = apply_bool_filter(query, "is_checked", filter_is_checked)
    list_response = query.execute()

    if not list_response.data:
        st.warning("Không có ảnh nào khớp với điều kiện lọc hiện tại!")
        return

    all_ids = [row["image_id"] for row in list_response.data]

    if "next_img_id" in st.session_state:
        if st.session_state.next_img_id in all_ids:
            st.session_state.selected_img = st.session_state.next_img_id
        del st.session_state.next_img_id

    selected_id = st.sidebar.selectbox(
        "Chọn ảnh để xem/sửa:",
        all_ids,
        key="selected_img"
    )

    detail_response = (
        supabase.table("image")
        .select("*")
        .eq("image_id", selected_id)
        .limit(1)
        .execute()
    )

    current_row = detail_response.data[0]

    img_id = current_row["image_id"]
    img_url = current_row["image_url"]
    is_checked_status = current_row.get("is_checked")
    is_drop_status = current_row.get("is_drop")

    current_idx = all_ids.index(img_id) + 1
    total_filtered = len(all_ids)

    checked_text = "🟢 Đã duyệt" if is_checked_status else "🔴 Chưa duyệt"
    drop_text = "🗑️ Drop" if is_drop_status else "✅ Giữ lại"
    st.write(
        f"**Đang xử lý ảnh ID:** `{img_id}` | **Vị trí:** {current_idx}/{total_filtered} | {checked_text} | {drop_text}"
    )

    col1, col2 = st.columns([1, 1])

    with col1:
        st.image(img_url, use_container_width=True)

        st.markdown("---")
        old_drop_status = current_row.get("is_drop")
        default_radio_index = 1 if old_drop_status is True else 0

        keep_image = st.radio(
            "Có nên giữ lại ảnh này không? (Chọn Không nếu ảnh mờ, sai chủ đề)",
            ("Có", "Không"),
            index=default_radio_index,
            horizontal=True,
        )

    with col2:
        st.subheader("Danh sách món ăn")
        st.write("Nhập tên món ăn, **mỗi món trên 1 dòng**. Bấm Enter để xuống dòng gõ tiếp.")

        existing_foods = current_row.get("food_items") or []
        foods_str_default = "\n".join(existing_foods)

        edited_foods_str = st.text_area(
            "Danh sách món (Gõ vào đây):",
            value=foods_str_default,
            height=250,
            key=f"text_area_{img_id}",
        )

        st.markdown("---")
        img_desc_input = st.text_area(
            "Mô tả/Ghi chú thêm về ảnh (Tùy chọn):",
            value=current_row.get("image_desc") or "",
            height=100,
        )

    st.markdown("---")
    if st.button("Lưu", type="primary", use_container_width=True, key="save_image_page"):
        raw_foods = edited_foods_str.split("\n")
        final_foods = [f.strip() for f in raw_foods if f.strip() != ""]

        db_food_items = final_foods if len(final_foods) > 0 else None
        db_image_desc = img_desc_input.strip() if img_desc_input.strip() else None
        is_drop_val = True if keep_image == "Không" else False

        supabase.table("image").update(
            {
                "food_items": db_food_items,
                "image_desc": db_image_desc,
                "is_drop": is_drop_val,
                "is_checked": True,
            }
        ).eq("image_id", img_id).execute()

        current_idx = all_ids.index(selected_id)
        if current_idx + 1 < len(all_ids):
            st.session_state.next_img_id = all_ids[current_idx + 1]

        st.rerun()


def fetch_vqa_rows(
    start_id: str,
    end_id: str,
    vqa_is_drop: str,
    vqa_is_checked: str,
    qtype_filter: str,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    eligible_image_ids = fetch_image_ids_for_filter(start_id, end_id)
    if not eligible_image_ids:
        return [], {}

    image_rows = (
        supabase.table("image")
        .select("image_id,image_url,food_items,image_desc,is_checked,is_drop")
        .in_("image_id", eligible_image_ids)
        .execute()
        .data
        or []
    )
    image_map = {row["image_id"]: row for row in image_rows if row.get("image_id")}

    vqa_rows: list[dict[str, Any]] = []
    chunk_size = 200
    for i in range(0, len(eligible_image_ids), chunk_size):
        chunk_ids = eligible_image_ids[i:i + chunk_size]
        query = (
            supabase.table("vqa")
            .select("vqa_id,image_id,qtype,question,is_checked,is_drop")
            .in_("image_id", chunk_ids)
            .order("image_id")
            .order("vqa_id")
        )
        query = apply_bool_filter(query, "is_drop", vqa_is_drop)
        query = apply_bool_filter(query, "is_checked", vqa_is_checked)
        if qtype_filter != "Tất cả":
            query = query.eq("qtype", qtype_filter)
        resp = query.execute()
        vqa_rows.extend(resp.data or [])

    vqa_rows = [row for row in vqa_rows if row.get("image_id") in image_map]
    return vqa_rows, image_map


def load_vqa_detail(vqa_id: int) -> dict[str, Any] | None:
    resp = (
        supabase.table("vqa")
        .select("*")
        .eq("vqa_id", vqa_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    return rows[0] if rows else None


def fetch_triple_catalog_entries(triples_used: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results = []
    for triple in triples_used or []:
        subject = norm_text(triple.get("subject"))
        relation = norm_text(triple.get("relation"))
        target = norm_text(triple.get("target"))
        if not subject or not relation or not target:
            continue
        resp = (
            supabase.table("kg_triple_catalog")
            .select("subject,relation,target,evidence,source_url")
            .eq("subject", subject)
            .eq("relation", relation)
            .eq("target", target)
            .limit(1)
            .execute()
        )
        row = (resp.data or [{}])[0]
        results.append(
            {
                "subject": subject,
                "relation": relation,
                "target": target,
                "evidence": row.get("evidence") if row else None,
                "source_url": row.get("source_url") if row else None,
            }
        )
    return results


def render_evidence_block(triples_used: list[dict[str, Any]]) -> None:
    st.subheader("Evidence từ Knowledge Graph")
    triple_entries = fetch_triple_catalog_entries(triples_used)
    if not triple_entries:
        st.info("Sample này chưa có triple/evidence để hiển thị.")
        return

    for idx, item in enumerate(triple_entries, start=1):
        triple_text = f"{item['subject']} — {item['relation']} — {item['target']}"
        with st.expander(f"Triple {idx}: {triple_text}", expanded=(idx == 1)):
            evidence = norm_text(item.get("evidence"))
            source_url = norm_text(item.get("source_url"))
            if evidence:
                st.write(evidence)
            else:
                st.caption("Không có evidence.")

            if source_url and source_url != "LLM_Knowledge":
                st.markdown(f"[Mở nguồn]({source_url})")
            elif source_url:
                st.caption(source_url)
            else:
                st.caption("Không có source_url.")


def load_vqa_verify_page() -> None:
    st.sidebar.header("Verify VQA")
    start_id = st.sidebar.text_input("Từ ID ảnh (VD: image000000):", value="image000000", key="vqa_start")
    end_id = st.sidebar.text_input("Đến ID ảnh (VD: image001000):", value="image001000", key="vqa_end")

    st.sidebar.markdown("---")
    filter_is_drop = st.sidebar.selectbox(
        "Lọc theo vqa.is_drop:",
        ["Tất cả", "True", "False"],
        index=2,
        key="vqa_filter_drop",
    )
    filter_is_checked = st.sidebar.selectbox(
        "Lọc theo vqa.is_checked:",
        ["Tất cả", "True", "False"],
        index=2,
        key="vqa_filter_checked",
    )

    qtypes = fetch_question_types()
    qtype_filter = st.sidebar.selectbox(
        "Lọc theo qtype:",
        ["Tất cả", *qtypes],
        index=0,
        key="vqa_qtype_filter",
    )

    with st.expander("Rubric verify", expanded=False):
        st.markdown(
            """
**Q0 - KG Alignment**  
1: dùng sai / nhắc tới thứ không có trong ảnh; 2: không dùng triple; 3: dùng 1-hop; 4: suy luận 2-hop.  
**Luật:** Q0 = 1 → DROP

**Q1 - Visual Accuracy**  
1: sai cả 2; 2: sai 1 trong 2; 3: đúng nhưng khó hiểu; 4: hoàn hảo.  
**Luật:** Q1 ≤ 2 → DROP

**Q2 - Choice Logic**  
1: đáp án sai; 2: nhiều đáp án đúng; 3: đúng nhưng nhiễu yếu; 4: đúng + nhiễu tốt.

**Q3 - Rationale**  
1: bịa đặt; 2: chung chung; 3: đúng logic; 4: kết nối ảnh-tri thức tường minh.
            """
        )

    vqa_rows, image_map = fetch_vqa_rows(start_id, end_id, filter_is_drop, filter_is_checked, qtype_filter)
    if not vqa_rows:
        st.warning("Không có VQA nào khớp với điều kiện lọc hiện tại!")
        return

    vqa_ids = [row["vqa_id"] for row in vqa_rows]
    vqa_meta = {row["vqa_id"]: row for row in vqa_rows}

    if "next_vqa_id" in st.session_state:
        if st.session_state.next_vqa_id in vqa_ids:
            st.session_state.selected_vqa_id = st.session_state.next_vqa_id
        del st.session_state.next_vqa_id

    def format_vqa_option(vqa_id: int) -> str:
        row = vqa_meta[vqa_id]
        question = norm_text(row.get("question"))
        preview = (question[:55] + "...") if len(question) > 58 else question
        return f"{row['image_id']} | {row.get('qtype') or '-'} | #{vqa_id} | {preview}"

    selected_vqa_id = st.sidebar.selectbox(
        "Chọn VQA để verify:",
        vqa_ids,
        format_func=format_vqa_option,
        key="selected_vqa_id",
    )

    vqa_row = load_vqa_detail(selected_vqa_id)
    if not vqa_row:
        st.error("Không tải được chi tiết VQA.")
        return

    image_row = image_map.get(vqa_row["image_id"])
    if not image_row:
        st.error("Không tìm thấy ảnh nguồn tương ứng.")
        return

    current_idx = vqa_ids.index(selected_vqa_id) + 1
    total_filtered = len(vqa_ids)
    checked_text = "🟢 Đã duyệt" if vqa_row.get("is_checked") else "🔴 Chưa duyệt"
    drop_text = "🗑️ Drop" if vqa_row.get("is_drop") else "✅ Giữ lại"
    st.write(
        f"**Đang xử lý VQA ID:** `{selected_vqa_id}` | **Ảnh:** `{vqa_row['image_id']}` | **Vị trí:** {current_idx}/{total_filtered} | {checked_text} | {drop_text}"
    )

    main_left, main_right = st.columns([1.05, 1.15])

    default_scores = {
        "q0": safe_int(get_existing_verify_value(vqa_row, "q0", 3), 3),
        "q1": safe_int(get_existing_verify_value(vqa_row, "q1", 4), 4),
        "q2": safe_int(get_existing_verify_value(vqa_row, "q2", 3), 3),
        "q3": safe_int(get_existing_verify_value(vqa_row, "q3", 3), 3),
    }

    with main_left:
        st.image(image_row["image_url"], use_container_width=True)
        render_image_metadata_block(image_row)
        with st.expander("Evidence từ Knowledge Graph", expanded=False):
            render_evidence_block(vqa_row.get("triples_used") or [])

    with main_right:
        st.subheader("Verify VQA")
        tab_content, tab_verify = st.tabs(["Nội dung câu hỏi", "Phiếu verify"])

        with tab_content:
            valid_qtypes = fetch_question_types()
            current_qtype = norm_text(vqa_row.get("qtype"))
            if not valid_qtypes:
                st.error("Không đọc được danh sách question type hợp lệ từ data/question_types.csv")
                st.stop()
            if current_qtype not in valid_qtypes and current_qtype:
                st.warning(f"qtype hiện tại không nằm trong question_types.csv: {current_qtype}")
            qtype_index = valid_qtypes.index(current_qtype) if current_qtype in valid_qtypes else 0

            top_row_left, top_row_right = st.columns([1.2, 0.8])
            with top_row_left:
                qtype_input = st.selectbox(
                    "Question type",
                    valid_qtypes,
                    index=qtype_index,
                    key=f"qtype_{selected_vqa_id}",
                )
            with top_row_right:
                answer_letters = ["A", "B", "C", "D"]
                answer_default = vqa_row.get("answer") if vqa_row.get("answer") in answer_letters else "A"
                answer_input = st.selectbox(
                    "Đáp án đúng",
                    answer_letters,
                    index=answer_letters.index(answer_default),
                    key=f"answer_{selected_vqa_id}",
                )

            question_input = st.text_area(
                "Question",
                value=vqa_row.get("question") or "",
                height=110,
                key=f"question_{selected_vqa_id}",
            )

            choices_block_input = st.text_area(
                "Choices (mỗi lựa chọn một dòng, theo dạng A./B./C./D.)",
                value=format_choices_block(vqa_row),
                height=130,
                help="Ví dụ:\nA. Phương pháp hấp\nB. Phương pháp kho\nC. Phương pháp luộc\nD. Phương pháp nướng",
                key=f"choices_block_{selected_vqa_id}",
            )
            parsed_choices, missing_choice_labels = parse_choices_block(choices_block_input)
            if missing_choice_labels:
                st.warning("Choices chưa đúng định dạng hoặc còn thiếu: " + ", ".join(missing_choice_labels))

            with st.expander("Rationale", expanded=False):
                rationale_input = st.text_area(
                    "Rationale",
                    value=vqa_row.get("rationale") or "",
                    height=140,
                    key=f"rationale_{selected_vqa_id}",
                )

        with tab_verify:
            st.subheader("Phiếu verify theo guideline")
            score_cols = st.columns(2)
            score_keys = ["q0", "q1", "q2", "q3"]
            score_inputs: dict[str, int] = {}
            for idx, score_key in enumerate(score_keys):
                target_col = score_cols[idx % 2]
                with target_col:
                    score_inputs[score_key] = st.selectbox(
                        VERIFY_TITLES[score_key],
                        options=[1, 2, 3, 4],
                        index=max(0, min(3, default_scores[score_key] - 1)),
                        format_func=lambda value, sk=score_key: VERIFY_OPTIONS[sk][value],
                        key=f"{score_key}_{selected_vqa_id}",
                    )

            auto_decision, auto_rule = render_verify_summary(score_inputs)

            existing_decision = norm_text(get_existing_verify_value(vqa_row, "decision", auto_decision)).upper()
            decision_options = ["AUTO", "KEEP", "DROP"]
            if existing_decision not in {"KEEP", "DROP"}:
                decision_index = 0
            else:
                decision_index = decision_options.index(existing_decision)

            final_decision_mode = st.radio(
                "Quyết định cuối cùng",
                decision_options,
                index=decision_index,
                horizontal=True,
                captions=["Dùng khuyến nghị từ rubric", "Giữ lại", "Drop"],
                key=f"decision_mode_{selected_vqa_id}",
            )
            final_decision = auto_decision if final_decision_mode == "AUTO" else final_decision_mode
            st.info(f"Kết quả sẽ lưu: **{final_decision}**")

            default_notes = norm_text(get_existing_verify_value(vqa_row, "notes", ""))
            verify_notes_input = st.text_area(
                "Ghi chú verify",
                value=default_notes,
                height=100,
                placeholder="Ví dụ: distractor C quá yếu; rationale chưa dẫn chiếu rõ triple 2...",
                key=f"verify_notes_{selected_vqa_id}",
            )

    st.markdown("---")
    if st.button("Lưu VQA", type="primary", use_container_width=True, key="save_vqa_page"):
        parsed_choices, missing_choice_labels = parse_choices_block(choices_block_input)

        payload = {
            "qtype": qtype_input.strip(),
            "question": question_input.strip(),
            "choice_a": parsed_choices.get("A", "").strip(),
            "choice_b": parsed_choices.get("B", "").strip(),
            "choice_c": parsed_choices.get("C", "").strip(),
            "choice_d": parsed_choices.get("D", "").strip(),
            "answer": answer_input,
            "rationale": rationale_input.strip() or None,
            "is_drop": final_decision == "DROP",
            "is_checked": True,
        }

        required_errors = []
        for key, label in [
            ("qtype", "Question type"),
            ("question", "Question"),
            ("choice_a", "Choice A"),
            ("choice_b", "Choice B"),
            ("choice_c", "Choice C"),
            ("choice_d", "Choice D"),
        ]:
            if not payload[key]:
                required_errors.append(label)
        if missing_choice_labels:
            required_errors.append("Choices block phải đủ A/B/C/D")

        if required_errors:
            st.error("Các trường bắt buộc còn trống: " + ", ".join(required_errors))
            st.stop()

        payload.update(
            build_verify_payload(
                row=vqa_row,
                scores=score_inputs,
                decision=final_decision,
                notes=verify_notes_input,
                rule_text=auto_rule,
            )
        )

        try:
            supabase.table("vqa").update(payload).eq("vqa_id", selected_vqa_id).execute()
        except Exception as exc:  # noqa: BLE001
            st.error(f"Không lưu được VQA: {exc}")
            st.stop()

        current_idx_zero = vqa_ids.index(selected_vqa_id)
        if current_idx_zero + 1 < len(vqa_ids):
            st.session_state.next_vqa_id = vqa_ids[current_idx_zero + 1]
        st.rerun()


page = st.sidebar.radio(
    "Chế độ",
    ["Verify Food Items", "Verify VQA"],
    index=1,
)

st.sidebar.markdown("---")

if page == "Verify Food Items":
    load_image_annotation_page()
else:
    load_vqa_verify_page()
