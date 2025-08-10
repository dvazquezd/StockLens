from pathlib import Path
import os
import json
import pandas as pd
from config.config import PROMPT_PATH


def load_prompt() -> str:
    return PROMPT_PATH.read_text(encoding="utf-8")


def run_agent_llm(processed_dir: Path, model: str = "gpt-4o-mini", provider: str = "openai"):
    """
    Agente LLM:
    - Carga las últimas N filas de cada *_signals.parquet
    - Si no hay OPENAI_API_KEY, guarda un draft JSON offline
    - Si hay API key, llama al proveedor y guarda la salida

    Cambios clave:
    - Normaliza 'time' a ISO (string) para evitar Timestamps no serializables
    - json.dumps(..., default=str) para cubrir tipos numpy/pandas
    """
    items = []
    for f in sorted(processed_dir.glob("*_signals.parquet")):
        symbol = f.name.split("_")[0]
        df = pd.read_parquet(f)

        # Normaliza las últimas 5 filas y convierte 'time' en ISO string
        last = df.tail(5).copy()
        if "time" in last.columns:
            # Asegura conversión a datetime y luego a string ISO (UTC offset si procede)
            last["time"] = pd.to_datetime(last["time"], utc=False, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S%z")

        # Convierte a registros básicos (ya serializables)
        items.append({
            "symbol": symbol,
            "last": last.to_dict(orient="records")
        })

    api_key = os.getenv("OAIKEY")
    if not api_key:
        # Draft offline si no hay credenciales
        draft = {
            "provider": provider,
            "model": model,
            "note": "OPENAI_API_KEY no encontrado; se genera resumen offline.",
            "assets": items,
        }
        out = processed_dir / "agent_summary_llm_draft.json"
        # default=str para cubrir cualquier tipo numpy/pandas residual
        out.write_text(json.dumps(draft, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"Agente LLM (draft) -> {out}")
        return draft

    # Cargar prompt base
    base_prompt = load_prompt()
    # Inserta los datos como JSON seguro
    assets_json = json.dumps(items, ensure_ascii=False, default=str)
    payload = f"{base_prompt}\n\nHere is the data:\n{assets_json}"

    # Llamada a OpenAI (requiere openai>=1.x instalado)
    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a financial market analysis assistant."},
            {"role": "user", "content": payload}
        ]
    )

    output_text = resp.choices[0].message.content.strip()
    out = processed_dir / "agent_summary_llm.json"
    out.write_text(output_text, encoding="utf-8")
    print(f"Agente LLM -> {out}")
    return output_text