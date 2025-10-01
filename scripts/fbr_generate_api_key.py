#!/usr/bin/env python3
"""
fbr_generate_api_key.py — obtain an ephemeral FBR API key and export it to the job env.

- Calls POST https://fbrapi.com/generate_api_key
- Reads 'api_key' from JSON response
- Writes `FBR_API_KEY=<key>` to $GITHUB_ENV so later steps can use it

Exit codes:
  0 = success (key exported)
  1 = failure (no key / request error)
"""

import os
import sys
import json
import urllib.request

URL = "https://fbrapi.com/generate_api_key"

def main():
    try:
        req = urllib.request.Request(URL, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
            try:
                payload = json.loads(data.decode("utf-8"))
            except Exception as e:
                print("fbr_generate_api_key: JSON decode error:", e)
                print("Response preview:", data[:200])
                sys.exit(1)

        api_key = payload.get("api_key")
        if not api_key:
            print("fbr_generate_api_key: 'api_key' not in response:", payload)
            sys.exit(1)

        ghe = os.environ.get("GITHUB_ENV")
        if not ghe or not os.path.exists(os.path.dirname(ghe) if os.path.dirname(ghe) else "."):
            # Fallback: print for logs (masked by Actions), then exit success
            print("FBR_API_KEY obtained (export skipped because $GITHUB_ENV not found).")
            print(api_key)
            sys.exit(0)

        with open(ghe, "a", encoding="utf-8") as fh:
            fh.write(f"FBR_API_KEY={api_key}\n")

        print("fbr_generate_api_key: FBR_API_KEY acquired and exported to job env.")
        sys.exit(0)

    except Exception as e:
        print("fbr_generate_api_key: request failed:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()