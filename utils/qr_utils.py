import json
from pathlib import Path
from uuid import uuid4


def build_product_qr(payload, output_directory):
    try:
        import qrcode

        output_path = Path(output_directory)
        output_path.mkdir(parents=True, exist_ok=True)
        filename = f"product_{payload['product_id']}_{uuid4().hex[:8]}.png"
        image_path = output_path / filename
        qr_image = qrcode.make(json.dumps(payload))
        qr_image.save(image_path)
        return filename
    except Exception:
        return None


def decode_qr_payload(raw_text):
    try:
        return json.loads(raw_text)
    except (TypeError, json.JSONDecodeError):
        return None
