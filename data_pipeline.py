from __future__ import annotations
import pandas as pd
import argparse
import json
import re
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

print("Imports completed")
print("Starting functions definitions")

# Extractor

BR_PRICE_RE = re.compile(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", re.IGNORECASE)

def _fmt_brl(val: float) -> str:
    # 12.34 -> 'R$ 12,34'
    return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _parse_brl_price_text_to_float(text: str) -> Optional[float]:
    """
    Converte 'R$ 12,34' -> 12.34. Procura o primeiro match no texto.
    """
    if not text:
        return None
    m = BR_PRICE_RE.search(text)
    if not m:
        return None
    raw = m.group(0)
    num = raw.replace("R$", "").strip()
    num = num.replace(".", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None

def _json_loads_flex(s: str):
    # Parse flexível de JSON-LD que pode ser objeto ou array
    try:
        return json.loads(s)
    except Exception:
        return None

def _json_ld_blocks(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        txt = (tag.string or tag.text or "").strip()
        if not txt:
            continue
        data = _json_loads_flex(txt)
        if isinstance(data, dict):
            blocks.append(data)
        elif isinstance(data, list):
            blocks.extend([x for x in data if isinstance(x, dict)])
    return blocks

def _extract_name_from_jsonld(blocks: List[Dict[str, Any]]) -> Optional[str]:
    # Procura um @type Product com "name"
    for obj in blocks:
        typ = obj.get("@type")
        if typ == "Product" and obj.get("name"):
            return obj["name"].strip() or None
    return None

def _extract_name(soup: BeautifulSoup) -> Optional[str]:
    # 1) JSON-LD Product.name
    name = _extract_name_from_jsonld(_json_ld_blocks(soup))
    if name:
        return name

    # 2) og:title
    og = soup.find("meta", {"property": "og:title"})
    if og and og.get("content"):
        val = og["content"].strip()
        if val:
            return val

    # 3) h1 visível
    h1 = soup.find("h1")
    if h1:
        txt = h1.get_text(" ", strip=True)
        if txt:
            return txt

    # 4) <title> (fallback)
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
        for sep in (" | ", " – ", " - "):
            if sep in title:
                return title.split(sep)[0].strip()
        return title

    return None

def _extract_price_and_currency(soup: BeautifulSoup):
    """
    Retorna (price_text, price_value, currency, source_hint)
    Estratégia:
    A) Meta property="product:price:amount" + "product:price:currency" (padrão em muitas lojas/VTEX)
    B) itemprop="price" (content ou texto)
    C) metas alternativas comuns (og:price:amount, product:sale_price:amount)
    D) regex por 'R$ 9,99' no DOM
    """
    # A) Meta product:price:amount / product:price:currency
    meta_amount = soup.find("meta", {"property": "product:price:amount"})
    if meta_amount and meta_amount.get("content"):
        try:
            val = float(str(meta_amount["content"]).replace(",", "."))
            curr = "BRL"
            meta_curr = soup.find("meta", {"property": "product:price:currency"})
            if meta_curr and meta_curr.get("content"):
                curr = (meta_curr["content"] or "BRL").strip() or "BRL"
            return _fmt_brl(val), val, curr, 'meta[property="product:price:amount"]'
        except Exception:
            pass

    # B) itemprop="price"
    el = soup.select_one('[itemprop="price"]')
    if el:
        raw = el.get("content") or el.get_text(" ", strip=True)
        if raw:
            try:
                val = float(str(raw).replace(",", "."))
                return _fmt_brl(val), val, "BRL", 'itemprop="price"'
            except Exception:
                p = _parse_brl_price_text_to_float(raw)
                if p is not None:
                    return _fmt_brl(p), p, "BRL", 'itemprop="price" (regex)'

    # C) metas alternativas
    for prop in ("og:price:amount", "product:sale_price:amount"):
        m = soup.find("meta", {"property": prop})
        if m and m.get("content"):
            try:
                val = float(str(m["content"]).replace(",", "."))
                # moeda
                curr = "BRL"
                curr_meta = soup.find("meta", {"property": prop.replace(":amount", ":currency")})
                if curr_meta and curr_meta.get("content"):
                    curr = (curr_meta["content"] or "BRL").strip() or "BRL"
                return _fmt_brl(val), val, curr, f'meta[property="{prop}"]'
            except Exception:
                pass

    # D) regex em todo o DOM
    page_text = soup.get_text(" ", strip=True)
    p = _parse_brl_price_text_to_float(page_text)
    if p is not None:
        return _fmt_brl(p), p, "BRL", "regex-dom"

    return None, None, None, None

def fetch_html(url: str, timeout: int = 20) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "close",
    }
    with requests.Session() as s:
        s.headers.update(headers)
        resp = s.get(url, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text

def parse_product(html: str, url: Optional[str] = None) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    name = _extract_name(soup)
    price_text, price_value, currency, hint = _extract_price_and_currency(soup)
    return url, name, price_value


print("Function definitions completed")
print("Starting dataset preparation")

# other functions

def prepare_dataset():

    print("Importing website links")
    df_product = pd.read_csv('/Users/gustavobrunelli/Documents/AI Engineering/personal_automation/website_links.csv')
    print("website links imported")

    print("Starting data extraction")

    for lab, row in df_product.iterrows():

        try:

            url, name, price_value = parse_product(fetch_html(row['URL']), url=row['URL'])

            df_product.loc[lab, "product_name"] = name
            df_product.loc[lab, "price_value"] = price_value
            df_product.loc[lab, "comment"] = ""

        except Exception as e:

            print("Ocorreu um erro!")
            print(f"Detalhes do erro: {e}")

            df_product.loc[lab, "product_name"] = ""
            df_product.loc[lab, "price_value"] = ""
            df_product.loc[lab, "comment"] = f"Issue occurred during fetch/parse - {e}"

    df_product['time'] = datetime.now(timezone.utc)

    print("Data extraction completed")

    return df_product

df_product = prepare_dataset()

# Save to database

def save_to_database(df_product: pd.DataFrame):

    print("Saving data to database_price.csv")

    #open the database_price csv file and append the data from df_product to it
    df_database = pd.read_csv('/Users/gustavobrunelli/Documents/AI Engineering/personal_automation/database_price.csv')
    df_database = pd.concat([df_database, df_product], ignore_index=True)
    df_database.to_csv('/Users/gustavobrunelli/Documents/AI Engineering/personal_automation/database_price.csv', index=False)

    print("Data saved to database_price.csv")

save_to_database(df_product)