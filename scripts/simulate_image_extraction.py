# Tujuan
# Simulasi ekstraksi dua gambar signal (trade plan + profit image) menggunakan Gemini dari .env.
# Caller
# Operator lokal via terminal.
# Dependensi
# python-dotenv, google-generativeai, Pillow.
# Main Functions
# Membaca dua file gambar dan menghasilkan JSON extraction terstruktur.
# Side Effects
# Memanggil API Gemini.

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
from typing import Any, Dict

from dotenv import load_dotenv
from PIL import Image
import google.generativeai as genai


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _extract_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    text = re.sub(r"^```json\s*|\s*```$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return {"raw_text": text}
        return json.loads(match.group(0))


def _analyze_image(model, image_path: pathlib.Path, mode: str) -> Dict[str, Any]:
    if mode == "trade_plan":
        prompt = (
            "Kamu parser signal trading dari chart image.\n"
            "Return JSON saja dengan schema:\n"
            "{"
            '"mode":"trade_plan",'
            '"pair":"...",'
            '"direction":"long|short|unknown",'
            '"entry_price":[number],'
            '"take_profit_levels":[number],'
            '"stop_loss":number|null,'
            '"order_type":"market|limit|unknown",'
            '"risk_level":"normal|high|unknown",'
            '"notes":"..."'
            "}\n"
            "Jika tidak terlihat, isi unknown/null/array kosong."
        )
    else:
        prompt = (
            "Kamu parser profit/management image trading.\n"
            "Return JSON saja dengan schema:\n"
            "{"
            '"mode":"profit_management",'
            '"action":"tp_partial|set_sl_breakeven|update_sl|skip",'
            '"pair":"...|unknown",'
            '"close_percentage":number|null,'
            '"suggested_new_sl":number|null,'
            '"evidence":"..."'
            "}\n"
            "Jika tidak cukup bukti, gunakan action=skip."
        )

    with Image.open(image_path) as img:
        resp = model.generate_content([prompt, img])
    return _extract_json(resp.text)


def main() -> int:
    parser = argparse.ArgumentParser(description="Simulasi ekstraksi 2 gambar signal dengan Gemini.")
    parser.add_argument(
        "--profit-image",
        default=str(PROJECT_ROOT / "CaraCrypto" / "image_caracrypto" / "photo_2026-04-15_01-47-52 (1).jpg"),
    )
    parser.add_argument(
        "--trade-plan-image",
        default=str(PROJECT_ROOT / "CaraCrypto" / "image_caracrypto" / "photo_2026-04-13_09-00-09.jpg"),
    )
    args = parser.parse_args()

    load_dotenv(PROJECT_ROOT / ".env")
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    model_name = (os.getenv("GEMINI_MODEL") or "gemini-2.0-flash").strip()
    if not api_key:
        print("ERR: GEMINI_API_KEY missing in .env")
        return 1

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)

    trade_path = pathlib.Path(args.trade_plan_image)
    profit_path = pathlib.Path(args.profit_image)
    if not trade_path.exists():
        print(f"ERR: trade plan image not found: {trade_path}")
        return 1
    if not profit_path.exists():
        print(f"ERR: profit image not found: {profit_path}")
        return 1

    trade_result = _analyze_image(model, trade_path, "trade_plan")
    profit_result = _analyze_image(model, profit_path, "profit_management")

    output = {
        "trade_plan_image": str(trade_path),
        "profit_image": str(profit_path),
        "trade_plan_extraction": trade_result,
        "profit_management_extraction": profit_result,
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
