from pathlib import Path
import os
import json
import pandas as pd
from openai import OpenAI
from config.config import PROMPT_PATH


def load_prompt() -> str:
    """
    Loads the base LLM prompt text from the configured prompt file path.

    The path to the prompt file is defined in the global `PROMPT_PATH` constant
    from the configuration module. The file is read using UTF-8 encoding.

    Returns:
        str: The full prompt text as a string.

    Raises:
        FileNotFoundError: If the prompt file does not exist at `PROMPT_PATH`.
        UnicodeDecodeError: If the file cannot be decoded with UTF-8 encoding.
    """
    return PROMPT_PATH.read_text(encoding="utf-8")


def run_agent_llm(processed_dir: Path, model: str = "gpt-4o-mini", provider: str = "openai"):
    """
    Runs an LLM-based analysis agent on the latest generated trading signals.

    This function:
        1. Loads the last 5 rows from each `*_signals.parquet` file in the processed
           data directory.
        2. Normalizes the `time` column to ISO 8601 string format for JSON compatibility.
        3. If no API key (`OAIKEY`) is found, generates and saves an offline draft JSON
           file containing the most recent signals.
        4. If an API key is available, calls the specified LLM provider/model with a
           base prompt and the loaded signal data, then saves the model's output to disk.

    Parameters:
        processed_dir (Path): Path to the directory containing processed `*_signals.parquet` files.
        model (str, optional): The LLM model name to use for analysis.
            Defaults to `"gpt-4o-mini"`.
        provider (str, optional): The LLM provider identifier (e.g., `"openai"`).
            Defaults to `"openai"`.

    Returns:
        dict | str: 
            - If no API key is found, returns the offline draft summary as a dictionary.
            - If an API call is made, returns the model's output text.

    Raises:
        FileNotFoundError: If the processed directory does not contain any `*_signals.parquet` files.
        ValueError: If loaded parquet files are missing the `time` column or are unreadable.
    """
    print(f"\nEjecutando agente LLM con modelo {model} y proveedor {provider}")
    
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
    print(f"Resultado del análisis realizado por {model} - {provider} en {out}")
    return output_text