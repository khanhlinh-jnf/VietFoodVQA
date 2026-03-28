from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any

import streamlit as st
from supabase import create_client

st.set_page_config(layout="wide")
st.title("Vietnamese Food VQA - Annotation & Verify Tool")

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://cvdoasxazyruytejluvv.supabase.co")
SUPABASE_KEY = st.secrets.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")
PAGE_SIZE = 1000
PROJECT_ROOT = Path(__file__).resolve().parent
QUESTION_TYPES_CSV = PROJECT_ROOT / "data" / "question_types.csv"

VERIFY_FIELD_CANDIDATES = {
    "q0": ["q0_score", "verify_q0", "score_q0"],
    "q1": ["q1_score", "verify_q1", "score_q1"],
    "q2": ["q2_score", "verify_q2", "score_q2"],
    "decision": ["verify_decision", "review_decision", "decision"],
    "notes": ["verify_notes", "review_notes", "notes", "reviewer_note"],
    "rule": ["verify_rule", "review_rule"],
}

VERIFY_OPTIONS: dict[str, dict[int, str]] = {
    "q0": {
        1: "1 — Triple sai hoặc không liên quan tới ảnh (DROP)",
        2: "2 — Triple yếu / thiếu / chưa đủ tin cậy",
        3: "3 — Triple đúng và hỗ trợ câu hỏi",
        4: "4 — Triple đúng và hỗ trợ suy luận rõ ràng",
    },
    "q1": {
        1: "1 — Câu hỏi sai bản chất / hỏi nhầm đối tượng (DROP)",
        2: "2 — Câu hỏi mơ hồ / diễn đạt lỗi (DROP)",
        3: "3 — Câu hỏi đúng nhưng còn chưa gọn",
        4: "4 — Câu hỏi rõ ràng, đúng và tốt",
    },
    "q2": {
        1: "1 — Đáp án đúng bị sai",
        2: "2 — Nhiều đáp án đúng hoặc distractor lệch loại",
        3: "3 — Đúng nhưng distractor còn yếu",
        4: "4 — Đúng và distractor tốt",
    },
}

VERIFY_TITLES = {
    "q0": "Q0: Triple Used Validity",
    "q1": "Q1: Question Validity",
    "q2": "Q2: Choice Quality",
}

TRIPLE_REVIEW_OPTIONS = {
    "valid": "Valid",
    "invalid": "Invalid",
    "needs_edit": "Needs edit",
    "unsure": "Unsure",
}

TRIPLE_REVIEW_CAPTIONS = {
    "valid": "Triple đúng và tiếp tục dùng cho VQA này.",
    "invalid": "Triple sai, không nên dùng cho VQA này.",
    "needs_edit": "Triple còn liên quan nhưng cần sửa lại fact.",
    "unsure": "Chưa đủ chắc để kết luận.",
}

OPTIONAL_SCHEMA_HELP = {
    "triples_retrieved": (
        "App sẽ hiển thị tab `Triples retrieved` khi bảng `vqa` có cột `triples_retrieved jsonb`."
    ),
    "vqa_kg_triple_map": (
        "Để lưu trace VQA ↔ triple, nên có bảng `vqa_kg_triple_map`. Nếu bảng này chưa tồn tại, app vẫn chạy nhưng"
        " sẽ không lưu mapping chi tiết."
    ),
    "kg_triple_edit_log": (
        "Để audit việc sửa triple inline, nên có bảng `kg_triple_edit_log`. Nếu chưa có, app vẫn remap nhưng không lưu log."
    ),
    "kg_review_columns": (
        "Để Verify KG Triples hoạt động đầy đủ, nên có `is_checked` và `is_drop` trên `kg_triple_catalog`."
    ),
}


@st.cache_resource
def init_connection():
    if not SUPABASE_KEY:
        raise RuntimeError("Thiếu SUPABASE_KEY trong Streamlit secrets hoặc environment.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


supabase = init_connection()
_SCHEMA_CACHE: dict[tuple[str, str], bool] = {}
_TABLE_CACHE: dict[str, bool] = {}


def execute_query(query):
    try:
        return query.execute(), None
    except Exception as exc:  # noqa: BLE001
        return None, exc


@st.cache_data(show_spinner=False, ttl=120)
def table_exists(table_name: str) -> bool:
    _, err = execute_query(supabase.table(table_name).select("*").limit(1))
    return err is None


@st.cache_data(show_spinner=False, ttl=120)
def column_exists(table_name: str, column_name: str) -> bool:
    _, err = execute_query(supabase.table(table_name).select(column_name).limit(1))
    return err is None


@st.cache_data(show_spinner=False, ttl=120)
def list_table_columns(table_name: str) -> list[str]:
    response, err = execute_query(supabase.table(table_name).select("*").limit(1))
    if err is not None:
        return []
    rows = response.data or []
    if rows:
        return list(rows[0].keys())
    return []


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


def now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def parse_jsonish(value: Any) -> Any:
    if value is None:
        return []
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return []
    return []


def canonicalize_triple(item: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None

    subject = norm_text(item.get("subject"))
    relation = norm_text(item.get("relation"))
    target = norm_text(item.get("target"))
    if not subject or not relation or not target:
        return None

    return {
        "subject": subject,
        "relation": relation,
        "target": target,
        "evidence": norm_text(item.get("evidence")) or None,
        "source_url": norm_text(item.get("source_url")) or None,
    }


def triple_key(subject: str, relation: str, target: str) -> tuple[str, str, str]:
    return (norm_text(subject), norm_text(relation), norm_text(target))


def parse_triple_list(value: Any) -> list[dict[str, Any]]:
    data = parse_jsonish(value)
    if not isinstance(data, list):
        return []
    triples: list[dict[str, Any]] = []
    for item in data:
        triple = canonicalize_triple(item)
        if triple is not None:
            triples.append(triple)
    return triples


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
        if len(line) >= 2 and line[0].upper() in {"A", "B", "C", "D"} and line[1] in {".", ")", ":", "-"}:
            key = line[0].upper()
            value = line[2:].strip(" .):-\t")
            if value:
                parsed[key] = value
        else:
            fallback_values.append(line)

    if not parsed and len(fallback_values) == 4:
        parsed = dict(zip(["A", "B", "C", "D"], fallback_values))

    missing = [key for key in ["A", "B", "C", "D"] if not parsed.get(key)]
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
        response, err = execute_query(query_builder.range(start, start + page_size - 1))
        if err is not None:
            raise err
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


@st.cache_data(show_spinner=False)
def fetch_question_types() -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    if QUESTION_TYPES_CSV.exists():
        with QUESTION_TYPES_CSV.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                value = norm_text(row.get("canonical_qtype") or row.get("question_type"))
                if value and value not in seen:
                    values.append(value)
                    seen.add(value)
    if values:
        return values

    response, err = execute_query(supabase.table("vqa").select("qtype").limit(5000))
    if err is not None:
        return []
    for row in response.data or []:
        value = norm_text(row.get("qtype"))
        if value and value not in seen:
            values.append(value)
            seen.add(value)
    return sorted(values)


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


def build_verify_payload(
    row: dict[str, Any],
    scores: dict[str, int],
    decision: str,
    notes: str,
    rule_text: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    mapping = {
        "q0": scores["q0"],
        "q1": scores["q1"],
        "q2": scores["q2"],
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
    fired_rules: list[str] = []
    decision = "KEEP"

    if scores["q0"] <= 2:
        decision = "DROP"
        fired_rules.append("Q0<=2")
        reasons.append("Q0 ≤ 2: triple_used sai, yếu hoặc chưa đủ tin cậy cho câu hỏi.")

    if scores["q1"] <= 2:
        decision = "DROP"
        fired_rules.append("Q1<=2")
        reasons.append("Q1 ≤ 2: question sai bản chất hoặc diễn đạt không đạt.")

    if scores["q2"] <= 2:
        reasons.append("Q2 ≤ 2: đáp án hoặc distractor có vấn đề, cần kiểm tra lại choices.")

    if decision == "KEEP":
        reasons.append("Không kích hoạt hard-drop rule nào từ rubric hiện tại.")

    rule_text = " + ".join(fired_rules) if fired_rules else "PASS"
    return decision, rule_text, reasons


def render_verify_summary(scores: dict[str, int]) -> tuple[str, str]:
    auto_decision, auto_rule, reasons = evaluate_verify(scores)
    avg_score = sum(scores.values()) / len(scores)

    metric_cols = st.columns(4)
    metric_cols[0].metric("Q0", scores["q0"])
    metric_cols[1].metric("Q1", scores["q1"])
    metric_cols[2].metric("Q2", scores["q2"])
    metric_cols[3].metric("Avg", f"{avg_score:.2f}")

    if auto_decision == "DROP":
        st.error(f"Khuyến nghị theo rubric: DROP ({auto_rule})")
    else:
        st.success("Khuyến nghị theo rubric: KEEP")

    for reason in reasons:
        st.write(f"- {reason}")

    return auto_decision, auto_rule

def fetch_vqa_rows(
    start_vqa_id: int,
    end_vqa_id: int,
    vqa_is_drop: str,
    vqa_is_checked: str,
    qtype_filter: str,
    split_filter: str,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    select_cols = "vqa_id,image_id,qtype,question,is_checked,is_drop"

    if column_exists("vqa", "split"):
        select_cols += ",split"
    if column_exists("vqa", "triples_used"):
        select_cols += ",triples_used"
    if column_exists("vqa", "triples_retrieved"):
        select_cols += ",triples_retrieved"

    query = (
        supabase.table("vqa")
        .select(select_cols)
        .gte("vqa_id", start_vqa_id)
        .lte("vqa_id", end_vqa_id)
        .order("vqa_id")
    )

    query = apply_bool_filter(query, "is_drop", vqa_is_drop)
    query = apply_bool_filter(query, "is_checked", vqa_is_checked)

    if qtype_filter != "Tất cả":
        query = query.eq("qtype", qtype_filter)

    if split_filter != "Tất cả" and column_exists("vqa", "split"):
        query = query.eq("split", split_filter)

    resp, err = execute_query(query)
    if err is not None:
        raise err

    vqa_rows = resp.data or []
    if not vqa_rows:
        return [], {}

    image_ids = sorted(
        {
            row["image_id"]
            for row in vqa_rows
            if row.get("image_id")
        }
    )

    image_query = (
        supabase.table("image")
        .select("image_id,image_url,food_items,image_desc,is_checked,is_drop")
        .in_("image_id", image_ids)
        .eq("is_checked", True)
        .eq("is_drop", False)
    )
    image_resp, image_err = execute_query(image_query)
    if image_err is not None:
        raise image_err

    image_rows = image_resp.data or []
    image_map = {
        row["image_id"]: row
        for row in image_rows
        if row.get("image_id")
    }

    vqa_rows = [
        row
        for row in vqa_rows
        if row.get("image_id") in image_map
    ]

    return vqa_rows, image_map

def load_vqa_detail(vqa_id: int) -> dict[str, Any] | None:
    resp, err = execute_query(
        supabase.table("vqa")
        .select("*")
        .eq("vqa_id", vqa_id)
        .limit(1)
    )
    if err is not None:
        return None
    rows = resp.data or []
    return rows[0] if rows else None


def find_catalog_triple(subject: str, relation: str, target: str) -> dict[str, Any] | None:
    if not table_exists("kg_triple_catalog"):
        return None
    resp, err = execute_query(
        supabase.table("kg_triple_catalog")
        .select("*")
        .eq("subject", subject)
        .eq("relation", relation)
        .eq("target", target)
        .limit(1)
    )
    if err is not None:
        return None
    rows = resp.data or []
    return rows[0] if rows else None


def ensure_catalog_triple(triple: dict[str, Any], parent_triple_id: int | None = None) -> int | None:
    existing = find_catalog_triple(triple["subject"], triple["relation"], triple["target"])
    if existing:
        return safe_int(existing.get("triple_id"), 0) or None

    payload = {
        "subject": triple["subject"],
        "relation": triple["relation"],
        "target": triple["target"],
        "evidence": triple.get("evidence"),
        "source_url": triple.get("source_url"),
    }

    if column_exists("kg_triple_catalog", "is_checked"):
        payload["is_checked"] = False
    if column_exists("kg_triple_catalog", "is_drop"):
        payload["is_drop"] = False
    if column_exists("kg_triple_catalog", "created_from"):
        payload["created_from"] = "vqa_edit" if parent_triple_id else "vqa_inline_review"
    if column_exists("kg_triple_catalog", "parent_triple_id") and parent_triple_id is not None:
        payload["parent_triple_id"] = parent_triple_id
    if column_exists("kg_triple_catalog", "needs_review"):
        payload["needs_review"] = True
    if column_exists("kg_triple_catalog", "updated_at"):
        payload["updated_at"] = now_iso()

    _, err = execute_query(
        supabase.table("kg_triple_catalog").upsert(
            payload,
            on_conflict="subject,relation,target",
        )
    )
    if err is not None:
        return None

    existing = find_catalog_triple(triple["subject"], triple["relation"], triple["target"])
    if existing:
        return safe_int(existing.get("triple_id"), 0) or None

    return None


def maybe_update_catalog_review(triple_id: int | None, verdict: str) -> None:
    if not triple_id or not column_exists("kg_triple_catalog", "is_checked") or not column_exists("kg_triple_catalog", "is_drop"):
        return
    if verdict not in {"valid", "invalid"}:
        return
    payload: dict[str, Any] = {
        "is_checked": True,
        "is_drop": verdict == "invalid",
    }
    if column_exists("kg_triple_catalog", "updated_at"):
        payload["updated_at"] = now_iso()
    execute_query(supabase.table("kg_triple_catalog").update(payload).eq("triple_id", triple_id))


def fetch_vqa_triple_map_rows(vqa_id: int) -> dict[int, dict[str, Any]]:
    if not table_exists("vqa_kg_triple_map"):
        return {}
    resp, err = execute_query(
        supabase.table("vqa_kg_triple_map")
        .select("*")
        .eq("vqa_id", vqa_id)
    )
    if err is not None:
        return {}
    rows = resp.data or []
    return {safe_int(row.get("triple_id"), 0): row for row in rows if safe_int(row.get("triple_id"), 0)}


def upsert_vqa_triple_map(payload: dict[str, Any]) -> None:
    if not table_exists("vqa_kg_triple_map"):
        return
    allowed_payload = dict(payload)
    if column_exists("vqa_kg_triple_map", "updated_at"):
        allowed_payload["updated_at"] = now_iso()
    execute_query(
        supabase.table("vqa_kg_triple_map").upsert(allowed_payload, on_conflict="vqa_id,triple_id")
    )



def insert_triple_edit_log(
    vqa_id: int,
    old_triple_id: int,
    new_triple_id: int,
    edit_reason: str | None,
    editor_note: str | None,
) -> None:
    if not table_exists("kg_triple_edit_log"):
        return
    payload: dict[str, Any] = {
        "vqa_id": vqa_id,
        "old_triple_id": old_triple_id,
        "new_triple_id": new_triple_id,
    }
    if column_exists("kg_triple_edit_log", "edit_reason"):
        payload["edit_reason"] = edit_reason or None
    if column_exists("kg_triple_edit_log", "editor_note"):
        payload["editor_note"] = editor_note or None
    execute_query(supabase.table("kg_triple_edit_log").insert(payload))



def sync_vqa_triples_used(vqa_id: int, active_triples: list[dict[str, Any]]) -> None:
    if not column_exists("vqa", "triples_used"):
        return
    payload: dict[str, Any] = {"triples_used": active_triples}
    if column_exists("vqa", "updated_at"):
        payload["updated_at"] = now_iso()
    execute_query(supabase.table("vqa").update(payload).eq("vqa_id", vqa_id))



def fetch_triple_review_entries(vqa_row: dict[str, Any]) -> list[dict[str, Any]]:
    triples_used = parse_triple_list(vqa_row.get("triples_used"))
    mapping_rows = fetch_vqa_triple_map_rows(int(vqa_row["vqa_id"]))

    entries: list[dict[str, Any]] = []
    for idx, triple in enumerate(triples_used):
        catalog_row = find_catalog_triple(triple["subject"], triple["relation"], triple["target"]) or {}
        triple_id = safe_int(catalog_row.get("triple_id"), 0)
        mapping_row = mapping_rows.get(triple_id, {}) if triple_id else {}
        entries.append(
            {
                "index": idx,
                "original_subject": triple["subject"],
                "original_relation": triple["relation"],
                "original_target": triple["target"],
                "subject": triple["subject"],
                "relation": triple["relation"],
                "target": triple["target"],
                "evidence": catalog_row.get("evidence") or triple.get("evidence"),
                "source_url": catalog_row.get("source_url") or triple.get("source_url"),
                "catalog_triple_id": triple_id or None,
                "catalog_is_checked": catalog_row.get("is_checked"),
                "catalog_is_drop": catalog_row.get("is_drop"),
                "mapping_status": norm_text(mapping_row.get("triple_review_status") or mapping_row.get("review_status") or "").lower(),
                "mapping_note": norm_text(mapping_row.get("triple_review_note") or mapping_row.get("review_note") or ""),
                "replaced_by_triple_id": mapping_row.get("replaced_by_triple_id"),
            }
        )
    return entries



def render_triple_readonly_block(title: str, triples: list[dict[str, Any]], expanded: bool = False) -> None:
    st.subheader(title)
    if not triples:
        st.info("Không có triple để hiển thị.")
        return
    for idx, item in enumerate(triples, start=1):
        triple_text = f"{item['subject']} — {item['relation']} — {item['target']}"
        with st.expander(f"Triple {idx}: {triple_text}", expanded=expanded and idx == 1):
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



def render_triple_review_editor(vqa_row: dict[str, Any]) -> list[dict[str, Any]]:
    entries = fetch_triple_review_entries(vqa_row)
    if not entries:
        st.info("VQA này hiện không có `triples_used`.")
        return []

    st.caption(
        "Nếu triple đúng nhưng sai chi tiết, hãy chọn `Needs edit`. App sẽ tạo triple revised mới và remap VQA hiện tại,"
        " không ghi đè âm thầm triple gốc."
    )

    drafts: list[dict[str, Any]] = []
    vqa_id = int(vqa_row["vqa_id"])
    for entry in entries:
        idx = entry["index"]
        prefix = f"vqa_{vqa_id}_triple_{idx}"
        triple_text = f"{entry['subject']} — {entry['relation']} — {entry['target']}"

        with st.container(border=True):
            st.markdown(f"**Triple {idx + 1}**")
            st.write(triple_text)
            badge_parts: list[str] = []
            if entry["catalog_triple_id"]:
                badge_parts.append(f"catalog_id={entry['catalog_triple_id']}")
            if entry["catalog_is_checked"] is True:
                badge_parts.append("global=checked")
            elif entry["catalog_is_checked"] is False:
                badge_parts.append("global=unchecked")
            if entry["catalog_is_drop"] is True:
                badge_parts.append("status=drop")
            elif entry["catalog_is_drop"] is False:
                badge_parts.append("status=keep")
            if entry["mapping_status"]:
                badge_parts.append(f"mapping={entry['mapping_status']}")
            if badge_parts:
                st.caption(" | ".join(badge_parts))

            if entry.get("evidence"):
                st.write(f"**Evidence:** {entry['evidence']}")
            else:
                st.caption("Không có evidence.")

            if entry.get("source_url"):
                if entry["source_url"] == "LLM_Knowledge":
                    st.caption("source_url: LLM_Knowledge")
                else:
                    st.markdown(f"[Mở nguồn]({entry['source_url']})")
            else:
                st.caption("Không có source_url.")

            default_action = entry["mapping_status"] if entry["mapping_status"] in TRIPLE_REVIEW_OPTIONS else "valid"
            action = st.radio(
                "Verdict for this triple",
                options=list(TRIPLE_REVIEW_OPTIONS.keys()),
                index=list(TRIPLE_REVIEW_OPTIONS.keys()).index(default_action),
                format_func=lambda x: TRIPLE_REVIEW_OPTIONS[x],
                horizontal=True,
                key=f"{prefix}_action",
            )
            st.caption(TRIPLE_REVIEW_CAPTIONS[action])

            editor_note = st.text_input(
                "Ghi chú cho triple này",
                value=entry["mapping_note"],
                key=f"{prefix}_note",
                placeholder="Ví dụ: target sai; cần đổi từ Miền Nam thành Huế...",
            )

            edit_reason = ""
            edited_subject = entry["subject"]
            edited_relation = entry["relation"]
            edited_target = entry["target"]
            edited_evidence = norm_text(entry.get("evidence"))
            edited_source_url = norm_text(entry.get("source_url"))

            if action == "needs_edit":
                edit_reason = st.text_input(
                    "Lý do sửa triple",
                    key=f"{prefix}_edit_reason",
                    placeholder="Ví dụ: relation sai hoặc target sai.",
                )
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    edited_subject = st.text_input("Edited subject", value=entry["subject"], key=f"{prefix}_subject")
                with col_b:
                    edited_relation = st.text_input("Edited relation", value=entry["relation"], key=f"{prefix}_relation")
                with col_c:
                    edited_target = st.text_input("Edited target", value=entry["target"], key=f"{prefix}_target")

                edited_evidence = st.text_area(
                    "Edited evidence",
                    value=norm_text(entry.get("evidence")),
                    height=80,
                    key=f"{prefix}_evidence",
                )
                edited_source_url = st.text_input(
                    "Edited source_url",
                    value=norm_text(entry.get("source_url")),
                    key=f"{prefix}_source_url",
                )

                st.info(
                    f"Original: {entry['subject']} — {entry['relation']} — {entry['target']}\n\n"
                    f"Revised: {edited_subject.strip()} — {edited_relation.strip()} — {edited_target.strip()}"
                )

            drafts.append(
                {
                    "old_triple_id": entry["catalog_triple_id"],
                    "old_triple": {
                        "subject": entry["subject"],
                        "relation": entry["relation"],
                        "target": entry["target"],
                        "evidence": entry.get("evidence"),
                        "source_url": entry.get("source_url"),
                    },
                    "action": action,
                    "editor_note": editor_note.strip(),
                    "edit_reason": edit_reason.strip(),
                    "edited_triple": {
                        "subject": edited_subject.strip(),
                        "relation": edited_relation.strip(),
                        "target": edited_target.strip(),
                        "evidence": edited_evidence.strip() or None,
                        "source_url": edited_source_url.strip() or None,
                    },
                }
            )

    return drafts



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
    list_response, err = execute_query(query)
    if err is not None:
        st.error(f"Không tải được danh sách ảnh: {err}")
        return

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
        key="selected_img",
    )

    detail_response, err = execute_query(
        supabase.table("image").select("*").eq("image_id", selected_id).limit(1)
    )
    if err is not None or not detail_response.data:
        st.error(f"Không tải được chi tiết ảnh: {err}")
        return

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
        is_drop_val = keep_image == "Không"

        payload = {
            "food_items": db_food_items,
            "image_desc": db_image_desc,
            "is_drop": is_drop_val,
            "is_checked": True,
        }
        if column_exists("image", "updated_at"):
            payload["updated_at"] = now_iso()

        _, err = execute_query(supabase.table("image").update(payload).eq("image_id", img_id))
        if err is not None:
            st.error(f"Không lưu được ảnh: {err}")
            return

        current_idx_zero = all_ids.index(selected_id)
        if current_idx_zero + 1 < len(all_ids):
            st.session_state.next_img_id = all_ids[current_idx_zero + 1]

        st.rerun()



def render_schema_warnings() -> None:
    missing_messages: list[str] = []
    if not column_exists("vqa", "triples_retrieved"):
        missing_messages.append(OPTIONAL_SCHEMA_HELP["triples_retrieved"])
    if not table_exists("vqa_kg_triple_map"):
        missing_messages.append(OPTIONAL_SCHEMA_HELP["vqa_kg_triple_map"])
    if not table_exists("kg_triple_edit_log"):
        missing_messages.append(OPTIONAL_SCHEMA_HELP["kg_triple_edit_log"])
    if not (column_exists("kg_triple_catalog", "is_checked") and column_exists("kg_triple_catalog", "is_drop")):
        missing_messages.append(OPTIONAL_SCHEMA_HELP["kg_review_columns"])

    if missing_messages:
        with st.expander("Ghi chú schema bổ sung", expanded=False):
            for msg in missing_messages:
                st.write(f"- {msg}")



def load_vqa_verify_page() -> None:
    st.sidebar.header("Verify VQA")
    start_vqa_id = st.sidebar.number_input(
        "Từ VQA ID:",
        min_value=1,
        value=1,
        step=1,
        key="vqa_start_id",
    )

    end_vqa_id = st.sidebar.number_input(
        "Đến VQA ID:",
        min_value=1,
        value=1000,
        step=1,
        key="vqa_end_id",
    )
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

    split_filter = st.sidebar.selectbox(
        "Lọc theo split:",
        ["Tất cả", "train", "test", "validate"],
        index=2,
        key="vqa_split_filter",
    )

    with st.expander("Rubric verify", expanded=False):
        st.markdown(
            """
**Q0 - Triple Used Validity**  
1: triple sai / không liên quan; 2: triple yếu / thiếu; 3: triple đúng; 4: triple tốt và hỗ trợ suy luận.  
**Luật:** Q0 ≤ 2 → DROP

**Q1 - Question Validity**  
1: câu hỏi sai; 2: câu hỏi mơ hồ; 3: đúng nhưng chưa gọn; 4: rõ và tốt.  
**Luật:** Q1 ≤ 2 → DROP

**Q2 - Choice Quality**  
1: đáp án sai; 2: nhiều đáp án đúng hoặc distractor lệch loại; 3: đúng nhưng distractor yếu; 4: tốt.
            """
        )

    render_schema_warnings()

    try:
        if start_vqa_id > end_vqa_id:
            st.warning("`Từ VQA ID` phải nhỏ hơn hoặc bằng `Đến VQA ID`.")
            return

        vqa_rows, image_map = fetch_vqa_rows(
            start_vqa_id,
            end_vqa_id,
            filter_is_drop,
            filter_is_checked,
            qtype_filter,
            split_filter,
        )
    except Exception as exc:  # noqa: BLE001
        st.error(f"Không tải được danh sách VQA: {exc}")
        return

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
        split_text = row.get("split") or "-"
        return f"#{vqa_id} | {split_text} | {row.get('qtype') or '-'} | {row['image_id']} | {preview}"

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
    split_text = vqa_row.get("split") or "-"
    st.write(
        f"**Đang xử lý VQA ID:** `{selected_vqa_id}` | "
        f"**Ảnh:** `{vqa_row['image_id']}` | "
        f"**Split:** `{split_text}` | "
        f"**Vị trí:** {current_idx}/{total_filtered} | "
        f"{checked_text} | {drop_text}"
    )

    main_left, main_right = st.columns([1.0, 1.2])

    default_scores = {
        "q0": safe_int(get_existing_verify_value(vqa_row, "q0", 3), 3),
        "q1": safe_int(get_existing_verify_value(vqa_row, "q1", 4), 4),
        "q2": safe_int(get_existing_verify_value(vqa_row, "q2", 3), 3),
    }

    with main_left:
        st.image(image_row["image_url"], use_container_width=True)
        render_image_metadata_block(image_row)

    with main_right:
        st.subheader("Verify VQA")
        tabs = ["Nội dung câu hỏi", "Triple used"]
        has_retrieved = column_exists("vqa", "triples_retrieved")
        if has_retrieved:
            tabs.append("Triples retrieved")
        tabs.append("Phiếu verify")
        tab_objects = st.tabs(tabs)

        tab_content = tab_objects[0]
        tab_triple_used = tab_objects[1]
        tab_retrieved = tab_objects[2] if has_retrieved else None
        tab_verify = tab_objects[3] if has_retrieved else tab_objects[2]

        with tab_content:
            valid_qtypes = fetch_question_types()
            current_qtype = norm_text(vqa_row.get("qtype"))
            if not valid_qtypes:
                st.error("Không đọc được danh sách question type hợp lệ từ data/question_types.csv hoặc bảng vqa.")
                return
            if current_qtype not in valid_qtypes and current_qtype:
                st.warning(f"qtype hiện tại không nằm trong danh sách question type: {current_qtype}")
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

            with st.expander("Rationale (không bắt buộc verify)", expanded=False):
                rationale_input = st.text_area(
                    "Rationale",
                    value=vqa_row.get("rationale") or "",
                    height=140,
                    key=f"rationale_{selected_vqa_id}",
                )

        with tab_triple_used:
            triple_review_drafts = render_triple_review_editor(vqa_row)

        if has_retrieved and tab_retrieved is not None:
            with tab_retrieved:
                triples_retrieved = parse_triple_list(vqa_row.get("triples_retrieved"))
                render_triple_readonly_block("Triples retrieved", triples_retrieved, expanded=False)

        with tab_verify:
            st.subheader("Phiếu verify theo guideline")
            score_cols = st.columns(3)
            score_keys = ["q0", "q1", "q2"]
            score_inputs: dict[str, int] = {}
            for idx, score_key in enumerate(score_keys):
                with score_cols[idx]:
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
                placeholder="Ví dụ: distractor C quá yếu; triple 2 cần sửa target...",
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
        if column_exists("vqa", "updated_at"):
            payload["updated_at"] = now_iso()

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

        for idx, draft in enumerate(triple_review_drafts, start=1):
            if draft["action"] != "needs_edit":
                continue
            edited = draft["edited_triple"]
            if not edited["subject"] or not edited["relation"] or not edited["target"]:
                required_errors.append(f"Triple {idx}: subject/relation/target sau khi sửa không được trống")
            original_key = triple_key(
                draft["old_triple"]["subject"],
                draft["old_triple"]["relation"],
                draft["old_triple"]["target"],
            )
            edited_key = triple_key(edited["subject"], edited["relation"], edited["target"])
            if original_key == edited_key and norm_text(edited.get("evidence")) == norm_text(draft["old_triple"].get("evidence")) and norm_text(edited.get("source_url")) == norm_text(draft["old_triple"].get("source_url")):
                required_errors.append(f"Triple {idx}: bạn chọn Needs edit nhưng chưa thay đổi nội dung triple")

        if required_errors:
            st.error("Các trường bắt buộc hoặc điều kiện lưu chưa đạt: " + "; ".join(required_errors))
            return

        payload.update(
            build_verify_payload(
                row=vqa_row,
                scores=score_inputs,
                decision=final_decision,
                notes=verify_notes_input,
                rule_text=auto_rule,
            )
        )

        _, err = execute_query(supabase.table("vqa").update(payload).eq("vqa_id", selected_vqa_id))
        if err is not None:
            st.error(f"Không lưu được VQA: {err}")
            return

        active_triples: list[dict[str, Any]] = []
        mapping_enabled = table_exists("vqa_kg_triple_map")
        edit_log_enabled = table_exists("kg_triple_edit_log")

        for draft in triple_review_drafts:
            old_triple = draft["old_triple"]
            old_triple_id = draft["old_triple_id"] or ensure_catalog_triple(old_triple)
            action = draft["action"]
            note = draft["editor_note"] or None

            if action == "valid":
                active_triples.append(old_triple)
                maybe_update_catalog_review(old_triple_id, "valid")
                if mapping_enabled and old_triple_id:
                    map_payload = {
                        "vqa_id": selected_vqa_id,
                        "triple_id": old_triple_id,
                        "is_used": True,
                        "is_active_for_vqa": True,
                        "triple_review_status": "valid",
                        "triple_review_note": note,
                        "reviewed_from_page": "vqa_page",
                        "reviewed_at": now_iso(),
                    }
                    if not column_exists("vqa_kg_triple_map", "is_active_for_vqa"):
                        map_payload.pop("is_active_for_vqa", None)
                    if not column_exists("vqa_kg_triple_map", "triple_review_status"):
                        map_payload.pop("triple_review_status", None)
                    if not column_exists("vqa_kg_triple_map", "triple_review_note"):
                        map_payload.pop("triple_review_note", None)
                    if not column_exists("vqa_kg_triple_map", "reviewed_from_page"):
                        map_payload.pop("reviewed_from_page", None)
                    if not column_exists("vqa_kg_triple_map", "reviewed_at"):
                        map_payload.pop("reviewed_at", None)
                    if not column_exists("vqa_kg_triple_map", "is_used"):
                        map_payload.pop("is_used", None)
                    upsert_vqa_triple_map(map_payload)

            elif action == "invalid":
                maybe_update_catalog_review(old_triple_id, "invalid")
                if mapping_enabled and old_triple_id:
                    map_payload = {
                        "vqa_id": selected_vqa_id,
                        "triple_id": old_triple_id,
                        "is_used": True,
                        "is_active_for_vqa": False,
                        "triple_review_status": "invalid",
                        "triple_review_note": note,
                        "reviewed_from_page": "vqa_page",
                        "reviewed_at": now_iso(),
                    }
                    if not column_exists("vqa_kg_triple_map", "is_active_for_vqa"):
                        map_payload.pop("is_active_for_vqa", None)
                    if not column_exists("vqa_kg_triple_map", "triple_review_status"):
                        map_payload.pop("triple_review_status", None)
                    if not column_exists("vqa_kg_triple_map", "triple_review_note"):
                        map_payload.pop("triple_review_note", None)
                    if not column_exists("vqa_kg_triple_map", "reviewed_from_page"):
                        map_payload.pop("reviewed_from_page", None)
                    if not column_exists("vqa_kg_triple_map", "reviewed_at"):
                        map_payload.pop("reviewed_at", None)
                    if not column_exists("vqa_kg_triple_map", "is_used"):
                        map_payload.pop("is_used", None)
                    upsert_vqa_triple_map(map_payload)

            elif action == "needs_edit":
                edited = canonicalize_triple(draft["edited_triple"]) or {
                    "subject": draft["edited_triple"]["subject"],
                    "relation": draft["edited_triple"]["relation"],
                    "target": draft["edited_triple"]["target"],
                    "evidence": draft["edited_triple"].get("evidence"),
                    "source_url": draft["edited_triple"].get("source_url"),
                }
                new_triple_id = ensure_catalog_triple(edited, parent_triple_id=old_triple_id)
                active_triples.append(edited)
                if mapping_enabled and old_triple_id:
                    old_map_payload = {
                        "vqa_id": selected_vqa_id,
                        "triple_id": old_triple_id,
                        "is_used": True,
                        "is_active_for_vqa": False,
                        "triple_review_status": "needs_edit",
                        "triple_review_note": note,
                        "replaced_by_triple_id": new_triple_id,
                        "reviewed_from_page": "vqa_page",
                        "reviewed_at": now_iso(),
                    }
                    for optional_col in [
                        "is_active_for_vqa",
                        "triple_review_status",
                        "triple_review_note",
                        "replaced_by_triple_id",
                        "reviewed_from_page",
                        "reviewed_at",
                        "is_used",
                    ]:
                        if not column_exists("vqa_kg_triple_map", optional_col):
                            old_map_payload.pop(optional_col, None)
                    upsert_vqa_triple_map(old_map_payload)
                if mapping_enabled and new_triple_id:
                    new_map_payload = {
                        "vqa_id": selected_vqa_id,
                        "triple_id": new_triple_id,
                        "is_used": True,
                        "is_active_for_vqa": True,
                        "triple_review_status": "valid",
                        "triple_review_note": draft["edit_reason"] or note,
                        "reviewed_from_page": "vqa_page",
                        "reviewed_at": now_iso(),
                    }
                    for optional_col in [
                        "is_active_for_vqa",
                        "triple_review_status",
                        "triple_review_note",
                        "reviewed_from_page",
                        "reviewed_at",
                        "is_used",
                    ]:
                        if not column_exists("vqa_kg_triple_map", optional_col):
                            new_map_payload.pop(optional_col, None)
                    upsert_vqa_triple_map(new_map_payload)
                if edit_log_enabled and old_triple_id and new_triple_id:
                    insert_triple_edit_log(
                        vqa_id=selected_vqa_id,
                        old_triple_id=old_triple_id,
                        new_triple_id=new_triple_id,
                        edit_reason=draft["edit_reason"],
                        editor_note=note,
                    )

            else:  # unsure
                active_triples.append(old_triple)
                if mapping_enabled and old_triple_id:
                    map_payload = {
                        "vqa_id": selected_vqa_id,
                        "triple_id": old_triple_id,
                        "is_used": True,
                        "is_active_for_vqa": True,
                        "triple_review_status": "unsure",
                        "triple_review_note": note,
                        "reviewed_from_page": "vqa_page",
                        "reviewed_at": now_iso(),
                    }
                    if not column_exists("vqa_kg_triple_map", "is_active_for_vqa"):
                        map_payload.pop("is_active_for_vqa", None)
                    if not column_exists("vqa_kg_triple_map", "triple_review_status"):
                        map_payload.pop("triple_review_status", None)
                    if not column_exists("vqa_kg_triple_map", "triple_review_note"):
                        map_payload.pop("triple_review_note", None)
                    if not column_exists("vqa_kg_triple_map", "reviewed_from_page"):
                        map_payload.pop("reviewed_from_page", None)
                    if not column_exists("vqa_kg_triple_map", "reviewed_at"):
                        map_payload.pop("reviewed_at", None)
                    if not column_exists("vqa_kg_triple_map", "is_used"):
                        map_payload.pop("is_used", None)
                    upsert_vqa_triple_map(map_payload)

        sync_vqa_triples_used(selected_vqa_id, active_triples)

        current_idx_zero = vqa_ids.index(selected_vqa_id)
        if current_idx_zero + 1 < len(vqa_ids):
            st.session_state.next_vqa_id = vqa_ids[current_idx_zero + 1]

        st.success("Đã lưu VQA và xử lý triple review.")
        st.rerun()



def fetch_kg_rows(
    filter_is_checked: str,
    filter_is_drop: str,
    relation_filter: str,
    search_text: str,
) -> list[dict[str, Any]]:
    if not table_exists("kg_triple_catalog"):
        return []

    select_cols = "triple_id,subject,relation,target,evidence,source_url"
    if column_exists("kg_triple_catalog", "is_checked"):
        select_cols += ",is_checked"
    if column_exists("kg_triple_catalog", "is_drop"):
        select_cols += ",is_drop"
    if column_exists("kg_triple_catalog", "updated_at"):
        select_cols += ",updated_at"

    query = supabase.table("kg_triple_catalog").select(select_cols).order("triple_id")
    if column_exists("kg_triple_catalog", "is_checked"):
        query = apply_bool_filter(query, "is_checked", filter_is_checked)
    if column_exists("kg_triple_catalog", "is_drop"):
        query = apply_bool_filter(query, "is_drop", filter_is_drop)
    if relation_filter != "Tất cả":
        query = query.eq("relation", relation_filter)

    rows = fetch_all_rows(query)
    search_text = norm_text(search_text).lower()
    if search_text:
        rows = [
            row
            for row in rows
            if search_text in norm_text(row.get("subject")).lower()
            or search_text in norm_text(row.get("target")).lower()
            or search_text in norm_text(row.get("relation")).lower()
        ]
    return rows



def fetch_linked_vqas_for_triple(triple_id: int) -> list[dict[str, Any]]:
    if not table_exists("vqa_kg_triple_map"):
        return []
    resp, err = execute_query(
        supabase.table("vqa_kg_triple_map")
        .select("vqa_id,triple_id,triple_review_status,is_used,is_retrieved,is_active_for_vqa,replaced_by_triple_id")
        .eq("triple_id", triple_id)
    )
    if err is not None:
        return []
    mapping_rows = resp.data or []
    if not mapping_rows:
        return []

    vqa_ids = [row["vqa_id"] for row in mapping_rows if row.get("vqa_id") is not None]
    vqa_lookup: dict[int, dict[str, Any]] = {}
    if vqa_ids:
        vqa_resp, vqa_err = execute_query(
            supabase.table("vqa")
            .select("vqa_id,image_id,qtype,question,is_checked,is_drop,split")
            .in_("vqa_id", vqa_ids)
        )
        if vqa_err is None:
            vqa_lookup = {safe_int(row.get("vqa_id"), 0): row for row in (vqa_resp.data or [])}

    linked: list[dict[str, Any]] = []
    for row in mapping_rows:
        vqa_id = safe_int(row.get("vqa_id"), 0)
        merged = dict(row)
        merged.update(vqa_lookup.get(vqa_id, {}))
        linked.append(merged)
    return linked



def load_verify_triples_page() -> None:
    st.sidebar.header("Verify KG Triples")
    has_review_cols = column_exists("kg_triple_catalog", "is_checked") and column_exists("kg_triple_catalog", "is_drop")
    if not table_exists("kg_triple_catalog"):
        st.error("Chưa có bảng `kg_triple_catalog`.")
        return

    filter_is_checked = st.sidebar.selectbox(
        "Lọc theo triple.is_checked:",
        ["Tất cả", "True", "False"],
        index=2 if has_review_cols else 0,
        key="triple_filter_checked",
    )
    filter_is_drop = st.sidebar.selectbox(
        "Lọc theo triple.is_drop:",
        ["Tất cả", "True", "False"],
        index=0,
        key="triple_filter_drop",
    )

    rows_for_relation = fetch_kg_rows("Tất cả", "Tất cả", "Tất cả", "")
    relations = sorted({norm_text(row.get("relation")) for row in rows_for_relation if norm_text(row.get("relation"))})
    relation_filter = st.sidebar.selectbox(
        "Lọc theo relation:",
        ["Tất cả", *relations],
        index=0,
        key="triple_relation_filter",
    )
    search_text = st.sidebar.text_input("Tìm theo subject/target/relation:", key="triple_search")

    try:
        triple_rows = fetch_kg_rows(filter_is_checked, filter_is_drop, relation_filter, search_text)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Không tải được triple catalog: {exc}")
        return

    if not triple_rows:
        st.warning("Không có triple nào khớp với điều kiện lọc hiện tại.")
        return

    triple_ids = [safe_int(row["triple_id"], 0) for row in triple_rows]
    triple_map = {safe_int(row["triple_id"], 0): row for row in triple_rows}

    if "next_triple_id" in st.session_state:
        if st.session_state.next_triple_id in triple_ids:
            st.session_state.selected_triple_id = st.session_state.next_triple_id
        del st.session_state.next_triple_id

    def format_triple_option(tid: int) -> str:
        row = triple_map[tid]
        preview = f"{row['subject']} — {row['relation']} — {row['target']}"
        if len(preview) > 90:
            preview = preview[:87] + "..."
        return f"#{tid} | {preview}"

    selected_triple_id = st.sidebar.selectbox(
        "Chọn triple để verify:",
        triple_ids,
        format_func=format_triple_option,
        key="selected_triple_id",
    )

    current_row = triple_map[selected_triple_id]
    current_idx = triple_ids.index(selected_triple_id) + 1
    total_filtered = len(triple_ids)
    checked_text = "🟢 Đã duyệt" if current_row.get("is_checked") else "🔴 Chưa duyệt"
    drop_text = "🗑️ Drop" if current_row.get("is_drop") else "✅ Giữ lại"
    st.write(f"**Triple ID:** `{selected_triple_id}` | **Vị trí:** {current_idx}/{total_filtered} | {checked_text} | {drop_text}")

    left, right = st.columns([1.0, 1.1])
    with left:
        st.subheader("Thông tin triple")
        st.write(f"**Subject:** {current_row['subject']}")
        st.write(f"**Relation:** {current_row['relation']}")
        st.write(f"**Target:** {current_row['target']}")
        evidence = norm_text(current_row.get("evidence"))
        source_url = norm_text(current_row.get("source_url"))
        if evidence:
            st.write(f"**Evidence:** {evidence}")
        else:
            st.caption("Không có evidence.")
        if source_url:
            if source_url == "LLM_Knowledge":
                st.caption("source_url: LLM_Knowledge")
            else:
                st.markdown(f"[Mở nguồn]({source_url})")
        else:
            st.caption("Không có source_url.")

        if not has_review_cols:
            st.warning(OPTIONAL_SCHEMA_HELP["kg_review_columns"])

        default_verdict = "valid" if current_row.get("is_drop") is False else "invalid"
        if current_row.get("is_checked") is not True:
            default_verdict = "unsure"
        verdict = st.radio(
            "Global verdict",
            options=["valid", "invalid", "unsure"],
            format_func=lambda x: TRIPLE_REVIEW_OPTIONS[x],
            horizontal=True,
            key=f"kg_verdict_{selected_triple_id}",
        )
        st.caption(TRIPLE_REVIEW_CAPTIONS[verdict])

    with right:
        st.subheader("Linked VQAs")
        linked_rows = fetch_linked_vqas_for_triple(selected_triple_id)
        if not linked_rows:
            st.info("Chưa có linked VQA hoặc chưa có bảng `vqa_kg_triple_map`.")
        else:
            for row in linked_rows[:20]:
                with st.container(border=True):
                    st.write(f"**VQA #{row.get('vqa_id')}** | image `{row.get('image_id')}` | qtype `{row.get('qtype')}`")
                    st.write(norm_text(row.get("question")) or "(trống)")
                    badges = []
                    if row.get("triple_review_status"):
                        badges.append(f"status={row['triple_review_status']}")
                    if row.get("is_active_for_vqa") is True:
                        badges.append("active")
                    elif row.get("is_active_for_vqa") is False:
                        badges.append("inactive")
                    if row.get("is_retrieved") is True:
                        badges.append("retrieved")
                    if row.get("is_used") is True:
                        badges.append("used")
                    if badges:
                        st.caption(" | ".join(badges))

    st.markdown("---")
    if st.button("Lưu triple", type="primary", use_container_width=True, key="save_triple_page"):
        if not has_review_cols:
            st.error("Chưa có `is_checked` / `is_drop` trên `kg_triple_catalog`, nên chưa thể lưu verdict toàn cục cho triple.")
            return

        update_payload: dict[str, Any] = {
            "is_checked": verdict in {"valid", "invalid"},
            "is_drop": verdict == "invalid",
        }
        if column_exists("kg_triple_catalog", "updated_at"):
            update_payload["updated_at"] = now_iso()

        _, err = execute_query(
            supabase.table("kg_triple_catalog").update(update_payload).eq("triple_id", selected_triple_id)
        )
        if err is not None:
            st.error(f"Không lưu được triple: {err}")
            return

        current_idx_zero = triple_ids.index(selected_triple_id)
        if current_idx_zero + 1 < len(triple_ids):
            st.session_state.next_triple_id = triple_ids[current_idx_zero + 1]

        st.success("Đã lưu triple.")
        st.rerun()


page = st.sidebar.radio(
    "Chế độ",
    ["Verify Images", "Verify VQA", "Verify KG Triples"],
    index=1,
)

st.sidebar.markdown("---")

if page == "Verify Images":
    load_image_annotation_page()
elif page == "Verify VQA":
    load_vqa_verify_page()
else:
    load_verify_triples_page()
