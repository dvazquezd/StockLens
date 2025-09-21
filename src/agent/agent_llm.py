from pathlib import Path
import os
import json
import pandas as pd
from openai import OpenAI
from anthropic import Anthropic
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
        3. If no API key is found, generates and saves an offline draft JSON
           file containing the most recent signals.
        4. If an API key is available, calls the specified LLM provider/model with a
           base prompt and the loaded signal data, then saves the model's output to disk.

    Parameters:
        processed_dir (Path): Path to the directory containing processed `*_signals.parquet` files.
        model (str, optional): The LLM model name to use for analysis.
            Defaults to `"gpt-4o-mini"`.
        provider (str, optional): The LLM provider identifier (e.g., `"openai"`, `"anthropic"`).
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

    # Determinar qué clave API buscar según el proveedor
    if provider.lower() == "anthropic":
        api_key = os.getenv("ANTHROPIC_STOCK_LENS")
        api_key_name = "ANTHROPIC_STOCK_LENS"
    else:  # openai por defecto
        api_key = os.getenv("OAIKEY")
        api_key_name = "OAIKEY"

    if not api_key:
        # Draft offline si no hay credenciales
        draft = {
            "provider": provider,
            "model": model,
            "note": f"{api_key_name} no encontrado; se genera resumen offline.",
            "assets": items,
        }
        out = processed_dir / "agent_summary_llm.json"
        # default=str para cubrir cualquier tipo numpy/pandas residual
        out.write_text(json.dumps(draft, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"Agente LLM (draft) -> {out}")
        return draft

    # Cargar prompt base
    base_prompt = load_prompt()
    # Inserta los datos como JSON seguro
    assets_json = json.dumps(items, ensure_ascii=False, default=str)
    payload = f"{base_prompt}\n\nHere is the data:\n{assets_json}"

    # Añadir instrucción específica para el formato JSON
    json_instruction = "\n\nRespond ONLY with a valid JSON array containing objects with 'symbol', 'recommendation', and 'rationale' fields. Do not include any other text or explanations outside the JSON."
    payload_with_format = f"{base_prompt}{json_instruction}\n\nHere is the data:\n{assets_json}"

    # Llamada según el proveedor
    if provider.lower() == "anthropic":        
        client = Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=model,
            max_tokens=4000,
            messages=[
                {"role": "user", "content": payload}
            ]
        )
        # Extraer texto de la respuesta de Anthropic
        response_text = resp.content[0].text
        
    else:  # OpenAI por defecto
        from openai import OpenAI
        
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a financial market analysis assistant."},
                {"role": "user", "content": payload}
            ]
        )
        # Extraer texto de la respuesta de OpenAI
        response_text = resp.choices[0].message.content

    # Intentar parsear la respuesta como JSON
    try:
        # Limpiar posibles markdown o texto extra
        cleaned_response = response_text.strip()
        if cleaned_response.startswith('```json'):
            cleaned_response = cleaned_response.replace('```json', '').replace('```', '').strip()
        elif cleaned_response.startswith('```'):
            cleaned_response = cleaned_response.replace('```', '').strip()
            
        response_json = json.loads(cleaned_response)
        
        # Guardar directamente el JSON parseado
        out = processed_dir / "agent_summary_llm.json"
        out.write_text(json.dumps(response_json, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"Agente LLM ({provider}) -> {out}")
        
        return response_json
        
    except json.JSONDecodeError as e:
        print(f"Error al parsear JSON del {provider}: {e}")
        print(f"Respuesta original: {response_text}")
        
        # Fallback: guardar respuesta como texto
        fallback = {
            "provider": provider,
            "model": model,
            "timestamp": datetime.now().isoformat(),
            "error": "Failed to parse JSON response",
            "raw_response": response_text,
            "assets_analyzed": len(items)
        }
        
        out = processed_dir / "agent_summary_llm_error.json"
        out.write_text(json.dumps(fallback, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"Agente LLM (error fallback) -> {out}")
        
        return response_text